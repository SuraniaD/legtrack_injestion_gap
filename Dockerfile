FROM python:3.12-slim

# Unbuffered output so every progress line shows up in Railway's logs immediately.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY backfill_gaps.py gap_models.py ./

# Preflight creates the tables and checks DB + API key; the backfill only
# starts if every check passes. railway.toml uses the same command.
CMD ["sh", "-c", "python backfill_gaps.py --preflight && exec python backfill_gaps.py"]
