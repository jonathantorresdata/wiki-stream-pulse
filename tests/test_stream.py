from pathlib import Path

import pytest

from wikistream.collect import collect
from wikistream.sink import Checkpoint
from wikistream.sse import parse
from wikistream.transform import build

SAMPLE = Path(__file__).parent / "fixtures" / "sample_stream.txt"


def sample_events():
    return list(parse(SAMPLE.read_text(encoding="utf-8").splitlines(keepends=True)))


def test_parser_reads_every_event_and_its_position():
    events = sample_events()
    assert len(events) == 30
    assert all(e.position.startswith("[") for e in events)
    assert all("meta" in e.data for e in events)


def test_parser_ignores_comments_and_bad_json():
    lines = [":ok\n", "\n", "id: 1\n", "data: {not json\n", "\n", "id: 2\n", 'data: {"a": 1}\n', "\n"]
    events = list(parse(lines))
    assert [e.position for e in events] == ["2"]


def test_collect_writes_batches_and_commits_the_last_position(tmp_path):
    events = sample_events()
    checkpoint = Checkpoint(tmp_path / "checkpoint.txt")
    totals = collect(tmp_path / "bronze", checkpoint, seconds=60, batch_size=10, events=iter(events))
    assert totals == {"events": 30, "batches": 3}
    assert len(list((tmp_path / "bronze").rglob("*.ndjson"))) == 3
    assert checkpoint.load() == events[-1].position


def test_redelivered_events_are_deduplicated(tmp_path):
    """At-least-once: after a crash the last batch arrives again; silver must not double count."""
    events = sample_events()
    bronze = tmp_path / "bronze"
    collect(bronze, Checkpoint(tmp_path / "c1"), seconds=60, batch_size=30, events=iter(events))
    collect(bronze, Checkpoint(tmp_path / "c2"), seconds=60, batch_size=30, events=iter(events[-10:]))
    result = build(bronze, tmp_path / "test.duckdb")
    assert result == {"bronze_rows": 40, "silver_rows": 30, "duplicates_removed": 10}


def test_checkpoint_is_empty_before_the_first_run(tmp_path):
    assert Checkpoint(tmp_path / "missing.txt").load() is None


@pytest.mark.parametrize("position", ['[{"topic":"x","partition":0,"offset":5}]'])
def test_checkpoint_round_trip(tmp_path, position):
    cp = Checkpoint(tmp_path / "checkpoint.txt")
    cp.save(position)
    assert cp.load() == position
