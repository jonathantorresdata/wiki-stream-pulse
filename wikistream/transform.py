"""Bronze → silver → gold in DuckDB, plus quality checks that stop the run if they fail."""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

SQL = """
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Silver: one row per event. At-least-once delivery can land the same event twice
-- (after a crash or a reconnection), so duplicates are removed by event id here.
CREATE OR REPLACE TABLE silver.events AS
WITH typed AS (
    SELECT
        event_id,
        -- Event time = when the edit was made; stream time = when it entered the stream.
        coalesce(to_timestamp(edit_time), CAST(event_time AS TIMESTAMPTZ)) AS event_time,
        CAST(event_time AS TIMESTAMPTZ)  AS stream_time,
        CAST(received_at AS TIMESTAMPTZ) AS received_at,
        wiki, domain, type, namespace, title, "user",
        coalesce(is_bot, false)          AS is_bot,
        coalesce(is_minor, false)        AS is_minor,
        coalesce(length_new, 0) - coalesce(length_old, 0) AS bytes_changed,
        kafka_partition, kafka_offset,
        filename AS _source_file
    FROM read_json('{bronze}/**/*.ndjson', format = 'newline_delimited', filename = true, union_by_name = true)
    WHERE event_id IS NOT NULL
)
SELECT *, epoch(received_at) - epoch(event_time) AS lag_seconds  -- processing time minus event time
FROM typed
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY received_at) = 1;

-- Gold: per-minute windows on EVENT time (when the edit happened), not arrival time.
CREATE OR REPLACE TABLE gold.edits_per_minute AS
SELECT
    date_trunc('minute', event_time)                 AS minute,
    count(*)                                         AS events,
    count(*) FILTER (WHERE type = 'edit')            AS edits,
    count(*) FILTER (WHERE type = 'new')             AS new_pages,
    count(*) FILTER (WHERE is_bot)                   AS by_bots,
    sum(bytes_changed) FILTER (WHERE type IN ('edit', 'new')) AS bytes_changed
FROM silver.events
GROUP BY ALL;

CREATE OR REPLACE TABLE gold.top_wikis AS
SELECT wiki, count(*) AS events, round(100.0 * avg(CASE WHEN is_bot THEN 1 ELSE 0 END), 1) AS bot_pct
FROM silver.events
GROUP BY wiki;

-- Spanish Wikipedia, articles only (namespace 0), edits by people.
CREATE OR REPLACE TABLE gold.es_top_articles AS
SELECT title, count(*) AS edits, count(DISTINCT "user") AS editors
FROM silver.events
WHERE wiki = 'eswiki' AND namespace = 0 AND type IN ('edit', 'new') AND NOT is_bot
GROUP BY title;

CREATE OR REPLACE TABLE gold.lag AS
SELECT
    count(*)                                        AS events,
    round(quantile_cont(lag_seconds, 0.5), 2)       AS p50_seconds,
    round(quantile_cont(lag_seconds, 0.95), 2)      AS p95_seconds,
    round(max(lag_seconds), 1)                      AS max_seconds,
    count(*) FILTER (WHERE lag_seconds > 60)        AS late_events
FROM silver.events;
"""

CHECKS = {
    "silver.events: event_id is unique":
        "SELECT count(*) - count(DISTINCT event_id) FROM silver.events",
    "silver.events: no event from the future (2 min tolerance)":
        "SELECT count(*) FROM silver.events WHERE event_time > received_at + INTERVAL 2 MINUTE",
    "silver.events: every event has a wiki and a type":
        "SELECT count(*) FROM silver.events WHERE wiki IS NULL OR type IS NULL",
    "gold.edits_per_minute: windows add up to silver":
        "SELECT abs((SELECT sum(events) FROM gold.edits_per_minute) - (SELECT count(*) FROM silver.events))",
}


def build(bronze_dir: Path, db_path: Path) -> dict:
    """Rebuild silver and gold from bronze and run the checks. Raises if any check fails."""
    with duckdb.connect(str(db_path)) as con:
        # Timestamps without an offset are UTC; never let the machine's time zone decide.
        con.execute("SET TimeZone = 'UTC'")
        con.execute(SQL.format(bronze=bronze_dir.as_posix()))
        raw = con.execute(
            f"SELECT count(*) FROM read_json('{bronze_dir.as_posix()}/**/*.ndjson', format = 'newline_delimited')"
        ).fetchone()[0]
        clean = con.execute("SELECT count(*) FROM silver.events").fetchone()[0]
        failed = []
        for name, query in CHECKS.items():
            bad = con.execute(query).fetchone()[0]
            logger.info("  %s  %s", "PASS" if bad == 0 else "FAIL", name)
            if bad:
                failed.append(name)
    if failed:
        raise RuntimeError(f"{len(failed)} quality check(s) failed: {failed}")
    return {"bronze_rows": raw, "silver_rows": clean, "duplicates_removed": raw - clean}
