"""Consume the stream for a while and land it in bronze in micro-batches."""

import logging
import time
from pathlib import Path

from wikistream.sink import Checkpoint, slim, utcnow, write_batch
from wikistream.sse import stream

logger = logging.getLogger(__name__)


def collect(bronze_dir: Path, checkpoint: Checkpoint, seconds: float, batch_size: int = 2000,
            batch_seconds: float = 10.0, events=None) -> dict:
    """Read events for `seconds`, flushing a batch every `batch_size` events or `batch_seconds`.

    Returns counts for logging. `events` can be any iterator of Event (tests pass a fixture).
    """
    source = events if events is not None else stream(checkpoint.load())
    deadline = time.monotonic() + seconds
    rows, last_position, batch_started = [], None, utcnow()
    totals = {"events": 0, "batches": 0}

    def flush():
        nonlocal rows, batch_started
        if rows:
            write_batch(bronze_dir, rows, batch_started)
            if last_position:
                checkpoint.save(last_position)  # commit only after the batch is on disk
            totals["batches"] += 1
            totals["events"] += len(rows)
        rows, batch_started = [], utcnow()

    for event in source:
        rows.append(slim(event, utcnow()))
        last_position = event.position or last_position
        if len(rows) >= batch_size or (utcnow() - batch_started).total_seconds() >= batch_seconds:
            flush()
        if time.monotonic() >= deadline:
            break
    flush()
    logger.info("Collected %d events in %d batches", totals["events"], totals["batches"])
    return totals
