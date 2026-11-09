# 0003 — Micro-batches

**Context.** About 2,400 events arrive per minute. One file per event would create millions of tiny files; one file per run would lose everything on a crash.

**Decision.** Buffer events and flush every 2,000 events or 10 seconds, whichever comes first, to `data/bronze/date=YYYY-MM-DD/hour=HH/batch-*.ndjson` (written atomically). The folder layout is the usual date/hour partitioning, so a query on one day reads one folder.

**Consequences.**
- At most 10 seconds of events can be replayed after a crash (and are deduplicated).
- Newline-delimited JSON keeps bronze append-only and readable by any engine; DuckDB reads it with one glob.
