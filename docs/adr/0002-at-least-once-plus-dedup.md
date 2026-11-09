# 0002 — At-least-once delivery plus deduplication

**Context.** Exactly-once delivery is expensive and rarely guaranteed end to end. The checkpoint can only be saved after a batch is written; a crash between the two steps replays that batch.

**Decision.** Accept at-least-once delivery: write the batch first, commit the checkpoint second. Make the result idempotent downstream: silver keeps one row per `event_id` (`QUALIFY row_number() OVER (PARTITION BY event_id ...) = 1`).

**Consequences.**
- No event is lost; any event delivered twice is counted once (unit test: 40 delivered rows → 30 events, 10 duplicates removed).
- The report shows how many duplicate deliveries were removed.
