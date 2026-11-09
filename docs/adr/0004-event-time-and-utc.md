# 0004 — Event time, and UTC everywhere

**Context.** An event has three clocks: when the edit was made (event time), when it entered the stream (stream time) and when it reached the consumer (processing time). The first version computed the delay against the stream time and against the local clock, and showed a negative median delay. A later run failed the "no event from the future" check by exactly 3,600 seconds: DuckDB was reading timestamps without an offset in the laptop's time zone (UTC+1) instead of UTC.

**Decision.** Windows are built on event time (the edit's own timestamp). Delay = processing time − event time. Every DuckDB session sets `TimeZone = 'UTC'`, so results do not depend on the machine.

**Consequences.**
- Median delay from edit to arrival: about 1.2 s; 95% within about 11 s.
- The same code gives the same numbers on a laptop in Spain and on a CI runner in UTC. The quality check that caught the bug stays in place.
