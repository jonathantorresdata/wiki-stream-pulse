# 0001 — Resume from a checkpoint

**Context.** A stream consumer will stop: deploys, network drops, crashes. Restarting "from now" loses every event produced while it was down.

**Decision.** Wikimedia EventStreams is backed by Kafka and every event carries its position (topic, partition, offset or timestamp) in the SSE `id` line. After each batch is on disk, that position is saved to `data/checkpoint.txt`; on start, and on every reconnection, it is sent back as `Last-Event-ID`, so the server resumes right after it.

**Consequences.**
- Tested on the live stream: the consumer was stopped for about 25 seconds and resumed; every 10-second window of event time still has events (no gap), and the first minute after resuming read about 3,400 events instead of the usual 2,400 while it caught up.
- Reconnections inside a run use the same mechanism, with exponential backoff.
