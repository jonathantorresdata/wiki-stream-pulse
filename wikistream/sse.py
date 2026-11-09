"""Minimal Server-Sent Events client with resumable offsets.

Wikimedia EventStreams is backed by Kafka. Every event carries an `id` line with the
Kafka topic/partition/position it came from. Sending that value back in the
`Last-Event-ID` header resumes the stream exactly after that event, which is how a
Kafka consumer resumes from a committed offset.
"""

import json
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

logger = logging.getLogger(__name__)

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "wiki-stream-pulse/1.0 (+https://github.com/jonathantorresdata/wiki-stream-pulse)"


@dataclass(frozen=True)
class Event:
    position: str  # value of the SSE "id" line: where to resume from
    data: dict


def parse(lines: Iterable[str]) -> Iterator[Event]:
    """Turn raw SSE lines into events. Blank lines end an event; ':' lines are comments."""
    position, data = None, []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line == "":
            if data:
                try:
                    yield Event(position or "", json.loads("\n".join(data)))
                except json.JSONDecodeError:
                    logger.warning("Skipping a malformed event")
            position, data = None, []
        elif line.startswith(":"):
            continue
        elif line.startswith("id:"):
            position = line[3:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].lstrip())


def stream(last_position: str | None = None, max_retries: int = 5) -> Iterator[Event]:
    """Yield events forever, reconnecting with backoff and resuming from the last position."""
    attempt = 0
    while True:
        headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
        if last_position:
            headers["Last-Event-ID"] = last_position
        try:
            with urllib.request.urlopen(urllib.request.Request(STREAM_URL, headers=headers), timeout=60) as response:
                attempt = 0
                lines = (b.decode("utf-8", errors="replace") for b in response)
                for event in parse(lines):
                    last_position = event.position or last_position
                    yield event
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            attempt += 1
            if attempt > max_retries:
                raise
            wait = min(2 ** attempt, 60)
            logger.warning("Stream dropped (%s); resuming in %ss from the last position", error, wait)
            time.sleep(wait)
