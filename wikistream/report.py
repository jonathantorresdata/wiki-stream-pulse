"""Build a static HTML report (site/index.html) from the gold tables."""

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def _rows(con, sql):
    cur = con.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def build_report(db_path: Path, out_dir: Path, run: dict) -> Path:
    with duckdb.connect(str(db_path), read_only=True) as con:
        con.execute("SET TimeZone = 'UTC'")
        minutes = _rows(con, """
            SELECT strftime(minute, '%H:%M') AS minute, events, by_bots, events - by_bots AS by_people, new_pages
            FROM gold.edits_per_minute
            WHERE minute >= (SELECT min(date_trunc('minute', received_at)) + INTERVAL 1 MINUTE FROM silver.events)
              AND minute <  (SELECT max(date_trunc('minute', received_at)) FROM silver.events)
            ORDER BY minute""")
        wikis = _rows(con, "SELECT wiki, events, bot_pct FROM gold.top_wikis ORDER BY events DESC LIMIT 10")
        es = _rows(con, "SELECT title, edits, editors FROM gold.es_top_articles ORDER BY edits DESC, editors DESC, title LIMIT 10")
        lag = _rows(con, "SELECT * FROM gold.lag")[0]
        span = con.execute("SELECT strftime(min(event_time), '%Y-%m-%d %H:%M:%S'), strftime(max(event_time), '%Y-%m-%d %H:%M:%S'), count(DISTINCT wiki), round(100 * avg(CASE WHEN is_bot THEN 1 ELSE 0 END), 1) FROM silver.events").fetchone()
    data = {"minutes": minutes, "wikis": wikis, "es": es, "lag": lag, "run": run,
            "span": {"from": str(span[0])[:19], "to": str(span[1])[:19], "wikis": span[2], "bot_pct": span[3]},
            "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")}
    template = (Path(__file__).parent / "report_template.html").read_text()
    out_dir.mkdir(parents=True, exist_ok=True)
    page = out_dir / "index.html"
    page.write_text(template.replace("__DATA__", json.dumps(data, default=str)))
    return page
