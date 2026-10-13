"""
Backfill the data the tracker's filter needs but ingestion doesn't store yet,
for ONE congress (default: the current one).

Per bill already in `bills`:
  detail     /bill/{c}/{type}/{n}            -> bills.policy_area, introduced_date, legislation_url
  subjects   /bill/{c}/{type}/{n}/subjects   -> bill_subject
  summaries  /bill/{c}/{type}/{n}/summaries  -> bill_summary
  titles     /bill/{c}/{type}/{n}/titles     -> bill_title

Resumable: every (bill, endpoint) fetched is recorded in bill_fetch_log. Stop it
any time (Ctrl+C) and run again; it continues with what's left. Later runs only
re-fetch bills whose bills.update_date_including_text changed since.

Rate-limited to stay under Congress.gov's 5,000 requests/hour per key.
When the detail response says a bill has 0 subjects or 0 summaries, that call
is skipped and recorded as an empty, successful fetch.

Usage:
  export DATABASE_URL=postgresql://user:pass@localhost:5432/dbname
  export CONGRESS_API_KEY=...
  python backfill_gaps.py --preflight          # create tables + check DB, API key, permissions
  python backfill_gaps.py --dry-run            # worklist + time estimate, no calls
  python backfill_gaps.py --limit 20           # small test run
  python backfill_gaps.py                      # full run for the current congress
  python backfill_gaps.py --endpoints detail,subjects   # only some endpoints

Options can also come from environment variables (handy on Railway):
  BACKFILL_CONGRESS, BACKFILL_ENDPOINTS, BACKFILL_MAX_PER_HOUR, BACKFILL_CONCURRENCY

Stops cleanly on Ctrl+C or SIGTERM (Railway redeploy/stop); progress is kept.
Only one copy runs at a time per database (Postgres advisory lock).

Requires: httpx, sqlalchemy, and psycopg2-binary or psycopg.
Writes run history to the existing ingestion_runs / run_steps tables
(source = 'congress_gov_gap_backfill').
"""

import argparse
import asyncio
import html
import os
import re
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

import httpx
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from gap_models import (BILL_NEW_COLUMNS, NEW_TABLES, BillFetchLog, BillSubject,
                        BillSummary, BillTitle)

API_BASE = "https://api.congress.gov/v3"
PAGE_SIZE = 250
MAX_RETRIES = 5
ALL_ENDPOINTS = ("detail", "subjects", "summaries", "titles")
RUN_SOURCE = "congress_gov_gap_backfill"
LOCK_KEY = 7_311_960_004  # arbitrary, fixed: pg advisory lock id for this job


# ── helpers ──────────────────────────────────────────────────────────────────

def utcnow() -> datetime:
    """Naive UTC, matching the existing schema's datetime.utcnow defaults."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def current_congress(today: date | None = None) -> int:
    """The 1st Congress began in 1789; each lasts two years."""
    today = today or date.today()
    return (today.year - 1789) // 2 + 1


def _parse_date(value):
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()


def _parse_datetime(value):
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        dt = datetime.combine(date.fromisoformat(value), datetime.min.time())
    return dt.replace(tzinfo=None)  # stored naive UTC, like the rest of the schema


_TAG_RE = re.compile(r"<[^>]+>")
_BLOCK_RE = re.compile(r"</(p|li|h\d|div)>|<br\s*/?>", re.IGNORECASE)


def html_to_text(value):
    if not value:
        return None
    text_ = _BLOCK_RE.sub("\n", value)
    text_ = html.unescape(_TAG_RE.sub("", text_))
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text_.splitlines()]
    return "\n".join(line for line in lines if line) or None


def _db_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("Set DATABASE_URL.")
    url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        try:
            import psycopg  # noqa: F401
            url = url.replace("postgresql://", "postgresql+psycopg://", 1)
        except ImportError:
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url


def _api_key() -> str:
    key = os.environ.get("CONGRESS_API_KEY")
    if not key:
        try:  # same place the ingestion repo reads it from
            from src.config import CONGRESS_API_KEY as key  # type: ignore
        except Exception:
            key = None
    if not key:
        sys.exit("Set CONGRESS_API_KEY.")
    return key


# ── HTTP: rate limit + retry ─────────────────────────────────────────────────

class RateLimiter:
    """Spaces request starts evenly so a long run stays under max_per_hour."""

    def __init__(self, max_per_hour: int):
        self.interval = 3600.0 / max_per_hour
        self._next = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait(self):
        async with self._lock:
            now = time.monotonic()
            if now < self._next:
                await asyncio.sleep(self._next - now)
            self._next = max(now, self._next) + self.interval


@dataclass
class Counters:
    requests: int = 0
    skipped_calls: int = 0


class ApiError(Exception):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


class NotFound(ApiError):
    """Congress.gov has no such bill (or sub-resource). Not retried on later runs."""


async def get_json(client, limiter, counters, api_key, path, params=None) -> dict:
    """GET with retries on 429, 5xx and network errors. Never logs the key."""
    url = f"{API_BASE}/{path}"
    query = {"api_key": api_key, "format": "json", **(params or {})}
    for attempt in range(1, MAX_RETRIES + 1):
        await limiter.wait()
        counters.requests += 1
        try:
            response = await client.get(url, params=query, timeout=30.0)
        except httpx.TransportError as exc:
            if attempt == MAX_RETRIES:
                raise ApiError(f"network error on /{path}: {type(exc).__name__}") from None
            await asyncio.sleep(2 ** attempt)
            continue
        if response.status_code == 429 or response.status_code >= 500:
            if attempt == MAX_RETRIES:
                raise ApiError(f"HTTP {response.status_code} on /{path} after {MAX_RETRIES} attempts")
            retry_after = response.headers.get("Retry-After")
            wait = float(retry_after) if retry_after and retry_after.isdigit() else 2 ** attempt
            print(f"    HTTP {response.status_code} on /{path}; retrying in {wait:.0f}s")
            await asyncio.sleep(wait)
            continue
        if response.status_code == 404:
            raise NotFound(f"HTTP 404 on /{path}", 404)
        if response.status_code >= 400:
            raise ApiError(f"HTTP {response.status_code} on /{path}", response.status_code)
        return response.json()
    raise ApiError(f"gave up on /{path}")


async def get_all_pages(client, limiter, counters, api_key, path, extract) -> list:
    """Follows pagination until Congress.gov reports no next page."""
    items, offset = [], 0
    while True:
        data = await get_json(client, limiter, counters, api_key, path,
                              {"limit": PAGE_SIZE, "offset": offset})
        page = extract(data)
        items.extend(page)
        if not (data.get("pagination") or {}).get("next") or not page:
            return items
        offset += PAGE_SIZE


# ── per-bill work ────────────────────────────────────────────────────────────

@dataclass
class BillWork:
    congress: int
    bill_type: str
    bill_number: int
    source_update_date: datetime | None
    endpoints: list
    detail: dict | None = None
    results: dict = field(default_factory=dict)   # endpoint -> list of rows
    errors: dict = field(default_factory=dict)    # endpoint -> message
    not_found: set = field(default_factory=set)   # endpoints that returned 404

    @property
    def path(self) -> str:
        return f"bill/{self.congress}/{self.bill_type.lower()}/{self.bill_number}"

    @property
    def label(self) -> str:
        return f"{self.congress}-{self.bill_type}-{self.bill_number}"


async def fetch_bill(work: BillWork, client, limiter, counters, api_key, sem):
    async with sem:
        counts = {}
        if "detail" in work.endpoints:
            try:
                data = await get_json(client, limiter, counters, api_key, work.path)
                bill = data.get("bill") or {}
                work.detail = {
                    "policy_area": (bill.get("policyArea") or {}).get("name"),
                    "introduced_date": _parse_date(bill.get("introducedDate")),
                    "legislation_url": bill.get("legislationUrl"),
                }
                for key in ("subjects", "summaries", "titles"):
                    if isinstance(bill.get(key), dict) and "count" in bill[key]:
                        counts[key] = bill[key]["count"]
                    elif key in ("subjects", "summaries"):
                        counts[key] = 0  # absent from detail = none exist
            except NotFound:
                # The bill itself doesn't exist at Congress.gov: its sub-resources
                # can't either, so don't spend calls on them.
                work.not_found.update(work.endpoints)
                return work
            except ApiError as exc:
                work.errors["detail"] = str(exc)

        specs = {
            "subjects": ("subjects", lambda d: (d.get("subjects") or {}).get("legislativeSubjects") or []),
            "summaries": ("summaries", lambda d: d.get("summaries") or []),
            "titles": ("titles", lambda d: d.get("titles") or []),
        }
        for endpoint in ("subjects", "summaries", "titles"):
            if endpoint not in work.endpoints:
                continue
            if counts.get(endpoint) == 0:
                work.results[endpoint] = []
                counters.skipped_calls += 1
                continue
            suffix, extract = specs[endpoint]
            try:
                work.results[endpoint] = await get_all_pages(
                    client, limiter, counters, api_key, f"{work.path}/{suffix}", extract)
            except NotFound:
                work.not_found.add(endpoint)
            except ApiError as exc:
                work.errors[endpoint] = str(exc)
    return work


# ── database ─────────────────────────────────────────────────────────────────

def ensure_schema(engine):
    """Adds the bills columns and creates the new tables if missing. Idempotent."""
    with engine.begin() as conn:
        exists = conn.execute(text("SELECT to_regclass('public.bills')")).scalar()
        if not exists:
            sys.exit("Table `bills` not found. Point DATABASE_URL at the ingestion database.")
        for column, sql_type in BILL_NEW_COLUMNS.items():
            conn.execute(text(f"ALTER TABLE bills ADD COLUMN IF NOT EXISTS {column} {sql_type}"))
    for table in NEW_TABLES:
        table.create(engine, checkfirst=True)


def build_worklist(engine, congress, endpoints, refresh, limit):
    """Bills of `congress` whose endpoints were never fetched, errored, or are stale."""
    with engine.connect() as conn:
        bills = conn.execute(text("""
            SELECT congress, bill_type, bill_number,
                   COALESCE(update_date_including_text, update_date) AS src
            FROM bills WHERE congress = :c
            ORDER BY latest_action_date DESC NULLS LAST, bill_type, bill_number
        """), {"c": congress}).all()
        logs = {}
        if not refresh:
            for row in conn.execute(text("""
                SELECT bill_type, bill_number, endpoint, status, source_update_date
                FROM bill_fetch_log WHERE congress = :c
            """), {"c": congress}):
                logs[(row.bill_type, row.bill_number, row.endpoint)] = row

    work = []
    for b in bills:
        needed = []
        for endpoint in endpoints:
            log = logs.get((b.bill_type, b.bill_number, endpoint))
            stale = (log is None or log.status == "error"
                     or (b.src is not None and (log.source_update_date is None or log.source_update_date < b.src)))
            if stale:
                needed.append(endpoint)
        if needed:
            work.append(BillWork(b.congress, b.bill_type, b.bill_number, b.src, needed))
        if limit and len(work) >= limit:
            break
    return work, len(bills)


def write_bill(conn, w: BillWork):
    ident = {"congress": w.congress, "bill_type": w.bill_type, "bill_number": w.bill_number}
    written = 0
    now = utcnow()

    if w.detail is not None:
        conn.execute(text("""
            UPDATE bills SET policy_area = :policy_area, introduced_date = :introduced_date,
                   legislation_url = :legislation_url
            WHERE congress = :congress AND bill_type = :bill_type AND bill_number = :bill_number
        """), {**ident, **w.detail})
        written += 1

    def replace(model, rows):
        conn.execute(delete(model).where(model.congress == w.congress,
                                         model.bill_type == w.bill_type,
                                         model.bill_number == w.bill_number))
        if rows:
            conn.execute(insert(model), [{**ident, **r} for r in rows])
        return len(rows)

    if "subjects" in w.results:
        seen, rows = set(), []
        for s in w.results["subjects"]:
            name = s.get("name")
            if name and name not in seen:
                seen.add(name)
                rows.append({"subject_name": name, "update_date": _parse_datetime(s.get("updateDate"))})
        written += replace(BillSubject, rows)

    if "summaries" in w.results:
        rows = [{
            "version_code": s.get("versionCode"),
            "action_date": _parse_date(s.get("actionDate")),
            "action_desc": s.get("actionDesc"),
            "text_html": s.get("text"),
            "text_plain": html_to_text(s.get("text")),
            "update_date": _parse_datetime(s.get("updateDate")),
            "raw_json": s,
        } for s in w.results["summaries"]]
        written += replace(BillSummary, rows)

    if "titles" in w.results:
        rows = [{
            "title": t["title"],
            "title_type": t.get("titleType"),
            "title_type_code": str(t["titleTypeCode"]) if t.get("titleTypeCode") is not None else None,
            "bill_text_version_code": t.get("billTextVersionCode"),
            "bill_text_version_name": t.get("billTextVersionName"),
            "chamber_code": t.get("chamberCode"),
            "update_date": _parse_datetime(t.get("updateDate")),
        } for t in w.results["titles"] if t.get("title")]
        written += replace(BillTitle, rows)

    for endpoint in w.endpoints:
        if endpoint in w.not_found:
            status, ok = "not_found", False
        else:
            ok = endpoint not in w.errors and (endpoint != "detail" or w.detail is not None)
            status = "ok" if ok else "error"
        values = {
            **ident, "endpoint": endpoint,
            "status": status,
            "item_count": (1 if endpoint == "detail" else len(w.results.get(endpoint, []))) if ok else None,
            "source_update_date": w.source_update_date,
            "fetched_at": now,
            "error_message": None if ok else (
                "not found at Congress.gov" if status == "not_found"
                else w.errors.get(endpoint, "not fetched")[:500]),
        }
        stmt = pg_insert(BillFetchLog).values(**values)
        conn.execute(stmt.on_conflict_do_update(
            index_elements=["congress", "bill_type", "bill_number", "endpoint"],
            set_={k: stmt.excluded[k] for k in
                  ("status", "item_count", "source_update_date", "fetched_at", "error_message")}))
    return written


def start_run(engine, congress, endpoints) -> int | None:
    try:
        with engine.begin() as conn:
            return conn.execute(text("""
                INSERT INTO ingestion_runs (source, status, started_at, records_fetched, records_written)
                VALUES (:s, 'running', :t, 0, 0) RETURNING id
            """), {"s": RUN_SOURCE, "t": utcnow()}).scalar()
    except Exception as exc:  # run logging is best-effort
        print(f"  (ingestion_runs not written: {type(exc).__name__})")
        return None


def log_step(engine, run_id, status, started, detail, error=None):
    if run_id is None:
        return
    import json
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO run_steps (run_id, step_name, status, started_at, completed_at, detail, error_message)
            VALUES (:r, 'fetch_write', :s, :st, :ct, CAST(:d AS json), :e)
        """), {"r": run_id, "s": status, "st": started, "ct": utcnow(),
               "d": json.dumps(detail, default=str), "e": error})


def finish_run(engine, run_id, status, fetched, written, error=None):
    if run_id is None:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE ingestion_runs SET status = :s, completed_at = :t, records_fetched = :f,
                   records_written = :w, error_message = :e WHERE id = :id
        """), {"s": status, "t": utcnow(), "f": fetched, "w": written,
               "e": error, "id": run_id})


def acquire_run_lock(engine):
    """Holds a session-level advisory lock for the whole run. Returns the
    connection (keep it open) or None if another copy already holds it."""
    conn = engine.connect()
    got = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_KEY}).scalar()
    if not got:
        conn.close()
        return None
    conn.commit()
    return conn


# ── preflight ────────────────────────────────────────────────────────────────

def preflight(args) -> int:
    """Creates the tables/columns and checks everything a run needs. Exit code 1 on any FAIL."""
    results = []

    def report(level, msg):
        results.append(level)
        print(f"  [{level}] {msg}")

    congress = args.congress or current_congress()
    print(f"Preflight for congress {congress}")

    # 1. Database
    try:
        engine = create_engine(_db_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            version = conn.execute(text("SHOW server_version")).scalar()
            db = conn.execute(text("SELECT current_database()")).scalar()
        report("PASS", f"connected to database '{db}' (PostgreSQL {version})")
    except SystemExit:
        raise
    except Exception as exc:
        report("FAIL", f"cannot connect: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}")
        return 1

    # 2. Tables
    try:
        with engine.connect() as conn:
            before = {t.name: conn.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{t.name}"}).scalar()
                      for t in NEW_TABLES}
        ensure_schema(engine)
        created = [n for n, existed in before.items() if not existed]
        report("PASS", "gap tables ready" + (f" (created: {', '.join(created)})" if created else " (already existed)"))
        with engine.connect() as conn:
            cols = {r[0] for r in conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='bills'"))}
        missing = [c for c in BILL_NEW_COLUMNS if c not in cols]
        if missing:
            report("FAIL", f"bills is missing columns: {missing}")
        else:
            report("PASS", "bills has policy_area, introduced_date, legislation_url")
    except SystemExit as exc:
        report("FAIL", str(exc))
        return 1
    except Exception as exc:
        report("FAIL", f"could not create tables/columns (permissions?): {type(exc).__name__}: {str(exc).splitlines()[0][:200]}")
        return 1

    # 3. Data to work on
    with engine.connect() as conn:
        n_bills = conn.execute(text("SELECT count(*) FROM bills WHERE congress = :c"), {"c": congress}).scalar()
        done = dict(conn.execute(text(
            "SELECT status, count(*) FROM bill_fetch_log WHERE congress = :c GROUP BY 1"), {"c": congress}).all())
    if n_bills == 0:
        report("FAIL", f"no bills for congress {congress} in `bills`; run the ingestion list backfill first")
    else:
        report("PASS", f"{n_bills} bills for congress {congress} in `bills`; fetch log so far: {done or 'empty'}")

    # 4. Write permission (rolled back)
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            conn.execute(text("""INSERT INTO bill_fetch_log (congress, bill_type, bill_number, endpoint, status, fetched_at)
                                 VALUES (0, 'PREFLIGHT', 0, 'detail', 'ok', now())"""))
            trans.rollback()
        report("PASS", "can write to bill_fetch_log (test row rolled back)")
    except Exception as exc:
        report("FAIL", f"cannot write: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}")

    # 5. Run logging tables (optional)
    with engine.connect() as conn:
        have = [t for t in ("ingestion_runs", "run_steps")
                if conn.execute(text("SELECT to_regclass(:t)"), {"t": f"public.{t}"}).scalar()]
    if len(have) == 2:
        report("PASS", "ingestion_runs / run_steps found; runs will be logged there")
    else:
        report("WARN", "ingestion_runs / run_steps not found; the backfill runs but isn't logged in the DB")

    # 6. Optional index on bill_action
    if args.with_action_index:
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_bill_action_identity "
                                  "ON bill_action (congress, bill_type, bill_number)"))
            report("PASS", "index ix_bill_action_identity on bill_action is in place")
        except Exception as exc:
            report("WARN", f"could not create bill_action index: {type(exc).__name__}")

    # 7. Another copy running?
    lock_conn = acquire_run_lock(engine)
    if lock_conn is None:
        report("WARN", "another backfill is running right now (advisory lock held)")
    else:
        lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": LOCK_KEY})
        lock_conn.close()
        report("PASS", "no other backfill running")

    # 8. Congress.gov API key
    try:
        key = _api_key()
        r = httpx.get(f"{API_BASE}/bill/{congress}",
                      params={"api_key": key, "format": "json", "limit": 1}, timeout=30.0)
        if r.status_code == 200:
            api_count = (r.json().get("pagination") or {}).get("count")
            remaining = r.headers.get("X-RateLimit-Remaining")
            report("PASS", f"Congress.gov key works; API lists {api_count} bills for congress {congress}"
                   + (f"; {remaining} requests left this hour" if remaining else ""))
            if api_count and n_bills and n_bills < api_count:
                report("WARN", f"`bills` has {n_bills} of {api_count}; the list backfill is behind "
                               "(this script only fills bills already in `bills`)")
        elif r.status_code in (401, 403):
            report("FAIL", f"Congress.gov rejected the API key (HTTP {r.status_code})")
        else:
            report("FAIL", f"Congress.gov returned HTTP {r.status_code}")
    except SystemExit as exc:
        report("FAIL", str(exc))
    except httpx.HTTPError as exc:
        report("FAIL", f"cannot reach api.congress.gov: {type(exc).__name__}")

    failed = results.count("FAIL")
    print(f"Preflight: {results.count('PASS')} passed, {results.count('WARN')} warnings, {failed} failed")
    return 1 if failed else 0


# ── main ─────────────────────────────────────────────────────────────────────

async def run(args):
    endpoints = [e.strip() for e in args.endpoints.split(",") if e.strip()]
    bad = set(endpoints) - set(ALL_ENDPOINTS)
    if bad:
        sys.exit(f"Unknown endpoint(s): {', '.join(sorted(bad))}")
    congress = args.congress or current_congress()

    engine = create_engine(_db_url(), pool_pre_ping=True)
    ensure_schema(engine)
    work, total_bills = build_worklist(engine, congress, endpoints, args.refresh, args.limit)

    calls_per_bill = sum(len(w.endpoints) for w in work)
    hours = calls_per_bill / args.max_per_hour
    print(f"Congress {congress}: {total_bills} bills in `bills`, {len(work)} need work "
          f"({calls_per_bill} endpoint fetches, at most).")
    print(f"Upper-bound time at {args.max_per_hour}/hour: {hours:.1f} h "
          f"(less in practice: empty subjects/summaries are skipped).")
    if total_bills == 0:
        print("No bills for this congress yet. Run the ingestion list backfill first.")
        return
    if args.dry_run or not work:
        return

    lock_conn = acquire_run_lock(engine)
    if lock_conn is None:
        print("Another backfill is already running against this database; exiting.")
        return

    # Railway (and docker stop) send SIGTERM: treat it like Ctrl+C.
    main_task = asyncio.current_task()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, main_task.cancel)
        except (NotImplementedError, RuntimeError):  # e.g. Windows
            pass

    api_key = _api_key()
    limiter = RateLimiter(args.max_per_hour)
    counters = Counters()
    sem = asyncio.Semaphore(args.concurrency)
    run_id = start_run(engine, congress, endpoints)
    done = written = failed_endpoints = not_found = 0
    started_all = time.monotonic()

    try:
        async with httpx.AsyncClient() as client:
            for i in range(0, len(work), args.batch_size):
                batch = work[i:i + args.batch_size]
                step_started = utcnow()
                await asyncio.gather(*(fetch_bill(w, client, limiter, counters, api_key, sem)
                                       for w in batch))
                batch_written, batch_errors = 0, []
                for w in batch:
                    with engine.begin() as conn:
                        batch_written += write_bill(conn, w)
                    for endpoint, msg in w.errors.items():
                        batch_errors.append({"bill": w.label, "endpoint": endpoint, "error": msg})
                    not_found += len(w.not_found)
                done += len(batch)
                written += batch_written
                failed_endpoints += len(batch_errors)
                log_step(engine, run_id, "success" if not batch_errors else "failed", step_started,
                         {"bills": len(batch), "rows_written": batch_written,
                          "requests_so_far": counters.requests,
                          "errors": len(batch_errors), "error_samples": batch_errors[:5]})
                elapsed = time.monotonic() - started_all
                rate = done / elapsed if elapsed else 0
                eta_h = (len(work) - done) / rate / 3600 if rate else 0
                print(f"  {done}/{len(work)} bills | {counters.requests} requests "
                      f"({counters.skipped_calls} skipped as empty) | {failed_endpoints} errors | "
                      f"{not_found} not found | "
                      f"ETA {eta_h:.1f} h")
    except (KeyboardInterrupt, asyncio.CancelledError):
        finish_run(engine, run_id, "failed", counters.requests, written, "interrupted; re-run to resume")
        print("\nStopped. Progress is saved; run the same command again to continue.", flush=True)
        lock_conn.close()
        return

    finish_run(engine, run_id, "success" if failed_endpoints == 0 else "failed",
               counters.requests, written,
               None if failed_endpoints == 0 else f"{failed_endpoints} endpoint fetches failed; re-run to retry")
    lock_conn.close()
    print(f"Done: {done} bills, {counters.requests} requests, {failed_endpoints} failed fetches "
          f"(re-running retries only those).")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    env = os.environ.get
    p.add_argument("--congress", type=int, default=int(env("BACKFILL_CONGRESS")) if env("BACKFILL_CONGRESS") else None,
                   help="default: the current congress (env BACKFILL_CONGRESS)")
    p.add_argument("--endpoints", default=env("BACKFILL_ENDPOINTS") or ",".join(ALL_ENDPOINTS),
                   help="comma-separated subset of: detail,subjects,summaries,titles")
    p.add_argument("--limit", type=int, help="process at most N bills (testing)")
    p.add_argument("--batch-size", type=int, default=50, help="bills per write batch (default 50)")
    p.add_argument("--concurrency", type=int, default=int(env("BACKFILL_CONCURRENCY") or 4),
                   help="bills fetched in parallel (default 4)")
    p.add_argument("--max-per-hour", type=int, default=int(env("BACKFILL_MAX_PER_HOUR") or 4800),
                   help="request cap per hour (default 4800; Congress.gov allows 5000/key)")
    p.add_argument("--refresh", action="store_true", help="ignore bill_fetch_log; re-fetch everything")
    p.add_argument("--dry-run", action="store_true", help="show the worklist and estimate, make no calls")
    p.add_argument("--preflight", action="store_true",
                   help="create the tables/columns, check DB, permissions and API key, then exit")
    p.add_argument("--with-action-index", action="store_true",
                   help="with --preflight: also create the recommended index on bill_action")
    args = p.parse_args()
    if args.preflight:
        sys.exit(preflight(args))
    try:
        asyncio.run(run(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass


if __name__ == "__main__":
    main()
