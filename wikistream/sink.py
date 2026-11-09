"""Write events to the bronze layer in micro-batches, then commit the stream position.

Order matters: a batch file is written (atomically) BEFORE its last position is saved
as the checkpoint. If the process dies between the two steps, the next run resumes
from the previous checkpoint and writes some events again: at-least-once delivery.
The silver layer removes those duplicates by event id, so the end result is the same
as exactly-once.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from wikistream.sse import Event


class Checkpoint:
    """Where to resume the stream: the position of the last event safely on disk."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> str | None:
        if not self.path.exists():
            return None
        return self.path.read_text().strip() or None

    def save(self, position: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(position)
        tmp.replace(self.path)


def slim(event: Event, received_at: datetime) -> dict:
    """Keep the fields the models use, plus when the event reached us (processing time)."""
    d = event.data
    meta = d.get("meta", {})
    length = d.get("length") or {}
    return {
        "event_id": meta.get("id"),
        "edit_time": d.get("timestamp"),   # when the change was made (epoch seconds)
        "event_time": meta.get("dt"),      # when the event entered the stream
        "received_at": received_at.isoformat(timespec="milliseconds"),
        "wiki": d.get("wiki"),
        "domain": meta.get("domain"),
        "type": d.get("type"),
        "namespace": d.get("namespace"),
        "title": d.get("title"),
        "user": d.get("user"),
        "is_bot": d.get("bot"),
        "is_minor": d.get("minor"),
        "length_old": length.get("old"),
        "length_new": length.get("new"),
        "kafka_partition": meta.get("partition"),
        "kafka_offset": meta.get("offset"),
    }


def write_batch(bronze_dir: Path, rows: list[dict], batch_started: datetime) -> Path:
    """One newline-delimited JSON file per micro-batch, partitioned by date and hour."""
    folder = bronze_dir / f"date={batch_started:%Y-%m-%d}" / f"hour={batch_started:%H}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"batch-{batch_started:%Y%m%dT%H%M%S%f}.ndjson"
    tmp = path.with_suffix(".tmp")
    tmp.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))
    tmp.replace(path)
    return path


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
