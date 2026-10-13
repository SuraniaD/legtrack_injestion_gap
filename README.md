# legtracker-ingestion-gap

A Railway service that fills in what the ingestion database is missing for the tracker's filter, for the **current Congress only**:

| Endpoint | Fills |
| --- | --- |
| Detail | `bills.policy_area`, `bills.introduced_date`, `bills.legislation_url` |
| `/subjects` | `bill_subject` |
| `/summaries` | `bill_summary` |
| `/titles` | `bill_title` |

`bill_fetch_log` tracks progress, so the job can stop and resume and later runs only fetch what changed. The service **creates these tables itself** on its first start.

```
backfill_gaps.py     the job (with --preflight, --dry-run, resume, rate limit)
gap_models.py        definitions of the new columns and tables
requirements.txt
Dockerfile           python:3.12-slim
railway.toml         build + start command + restart policy
```

---

## 1. Push to GitHub

```bash
git init && git add . && git commit -m "Ingestion gap backfill service"
git remote add origin git@github.com:<you>/legtracker-ingestion-gap.git
git push -u origin main
```

## 2. Create the service on Railway

1. Open the Railway **project that contains the ingestion Postgres**.
2. **+ New → GitHub Repo →** pick this repo. Railway reads `railway.toml` and builds from the `Dockerfile`.
3. Before the first deploy finishes, open the service's **Variables** tab and add:

| Variable | Value | Notes |
| --- | --- | --- |
| `DATABASE_URL` | `${{Postgres.DATABASE_URL}}` | A reference to the DB service. Replace `Postgres` with your database service's name as shown in Railway. Uses Railway's private network |
| `CONGRESS_API_KEY` | your key | Mark it as sealed if you like |
| `BACKFILL_ENDPOINTS` | *(optional)* `detail,subjects` | Run only some endpoints; leave unset for all four |
| `BACKFILL_MAX_PER_HOUR` | *(optional)* `4800` | Lower it if the same key is used by another job at the same time |
| `BACKFILL_CONGRESS` | *(optional)* | Defaults to the current Congress (119) |

**If the database isn't on Railway, or is in a different Railway project,** set `DATABASE_URL` to its public connection string (for a Railway Postgres, `DATABASE_PUBLIC_URL`). A database running on your own machine can't be reached from Railway.

4. Deploy, or redeploy after adding the variables.

**Keep it to 1 replica.** A second copy would exit immediately anyway, because the job holds a Postgres advisory lock.

## 3. What the logs show

Every start runs a preflight first. The backfill only begins if nothing fails. Example output (numbers will differ):

```
Preflight for congress 119
  [PASS] connected to database 'railway' (PostgreSQL 16.x)
  [PASS] gap tables ready (created: bill_subject, bill_summary, bill_title, bill_fetch_log)
  [PASS] bills has policy_area, introduced_date, legislation_url
  [PASS] 19633 bills for congress 119 in `bills`; fetch log so far: empty
  [PASS] can write to bill_fetch_log (test row rolled back)
  [PASS] ingestion_runs / run_steps found; runs will be logged there
  [PASS] no other backfill running
  [PASS] Congress.gov key works; API lists 19633 bills for congress 119; 4999 requests left this hour
Preflight: 8 passed, 0 warnings, 0 failed
Congress 119: 19633 bills in `bills`, 19633 need work (78532 endpoint fetches, at most).
Upper-bound time at 4800/hour: 16.4 h (less in practice: empty subjects/summaries are skipped).
  50/19633 bills | 171 requests (29 skipped as empty) | 0 errors | 0 not found | ETA 15.2 h
  ...
```

| Preflight line | If it fails |
| --- | --- |
| cannot connect | `DATABASE_URL` is wrong, or the DB is unreachable from Railway (see step 2) |
| could not create tables/columns | The DB user lacks `CREATE`/`ALTER` on `public`; run once with an owner role |
| no bills for congress 119 | The ingestion list backfill hasn't loaded this Congress yet |
| Congress.gov rejected the API key | Wrong or inactive `CONGRESS_API_KEY` |
| WARN: list backfill is behind | `bills` has fewer bills than the API; this job only fills bills already in `bills` |

## 4. How the service behaves

| Event | What happens |
| --- | --- |
| Backfill finishes | Exits 0; the service stops (no restart) |
| Crash (OOM, DB drop) | Railway restarts it (up to 3 times); it resumes from `bill_fetch_log` |
| Redeploy or **Stop** in Railway | SIGTERM reaches the job; it stops cleanly, logs the run as `interrupted`, and the next start resumes |
| Preflight fails | Exits 1; restarted up to 3 times, then stays stopped with the reason in the logs |
| Two copies at once | The second logs "Another backfill is already running" and exits |

## 5. After the first full run

Uncomment `cronSchedule` in `railway.toml` to re-run daily. Each run only re-fetches bills whose `update_date_including_text` changed, so it's quick once the backfill is done. If a run is still going when the next one is due, Railway skips that one.

Check progress or results any time:

```sql
SELECT endpoint, status, count(*) FROM bill_fetch_log WHERE congress = 119 GROUP BY 1, 2 ORDER BY 1, 2;

SELECT id, status, started_at, completed_at, records_fetched, records_written, error_message
FROM ingestion_runs WHERE source = 'congress_gov_gap_backfill' ORDER BY id DESC LIMIT 5;
```

## 6. Keep the ingestion repo in sync

This service creates `bill_subject`, `bill_summary`, `bill_title` and `bill_fetch_log`, plus three columns on `bills`. **Add the same definitions to the ingestion repo's `src/models.py`** (see the top of `gap_models.py`). Otherwise that repo's next `alembic revision --autogenerate` will propose dropping them. Once added, autogenerate reports no differences (tested).

Optional, for the tracker's filter later: `python backfill_gaps.py --preflight --with-action-index` creates an index on `bill_action (congress, bill_type, bill_number)`. It briefly locks `bill_action` against writes while building, so run it when ingestion isn't writing, or add the index through the ingestion repo's Alembic instead.

## Tested

Run in a clean Python 3.12 environment with only `requirements.txt` installed (psycopg 3), against databases built from the ingestion repo's `src/models.py`, with live Congress.gov calls:

- **Preflight passes** on a Railway-style `postgres://` URL. It fails with a clear message on:
  - wrong password
  - empty `bills`
  - rejected API key
- **Tables:** preflight creates the 4 tables and 3 columns on a fresh database.
- **Data:** the backfill fills real data, resumes, doesn't retry 404s, and re-fetches only changed bills.
- **Start command:** run exactly as in `railway.toml`:
  - SIGTERM stops the job cleanly; the run is logged as `interrupted` and the lock is released.
  - A second copy exits immediately.

Not tested here: the Docker image build itself (no Docker in this environment). It's a standard `python:3.12-slim` + `pip install` image.
