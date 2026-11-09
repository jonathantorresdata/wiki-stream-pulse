# Architecture decision records

| # | Decision |
|---|---|
| [0001](0001-resumable-offsets.md) | Resume the stream from a checkpoint (Last-Event-ID), like a Kafka consumer |
| [0002](0002-at-least-once-plus-dedup.md) | At-least-once delivery + deduplication by event id = effectively exactly-once |
| [0003](0003-micro-batches.md) | Micro-batches to NDJSON instead of one file per event |
| [0004](0004-event-time-and-utc.md) | Windows on event time, and every timestamp in UTC |
