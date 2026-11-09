"""Consume the live stream for a while, rebuild the models and publish the report.

Usage:
    python scripts/run.py --minutes 5      # collect 5 minutes, then transform and report
    python scripts/run.py --minutes 0      # only rebuild models and report from bronze

Re-running resumes the stream from the saved checkpoint. Exit code 1 if a quality check fails.
"""

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from wikistream.collect import collect  # noqa: E402
from wikistream.report import build_report  # noqa: E402
from wikistream.sink import Checkpoint  # noqa: E402
from wikistream.transform import build  # noqa: E402

BRONZE = ROOT / "data" / "bronze"
CHECKPOINT = Checkpoint(ROOT / "data" / "checkpoint.txt")
DB = ROOT / "data" / "wikistream.duckdb"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("run")


def main() -> int:
    parser = argparse.ArgumentParser(description="Wiki Stream Pulse")
    parser.add_argument("--minutes", type=float, default=5)
    parser.add_argument("--batch-size", type=int, default=2000)
    args = parser.parse_args()

    totals = {"events": 0, "batches": 0}
    if args.minutes > 0:
        resumed = CHECKPOINT.load() is not None
        logger.info("1/3 Consume %s min (%s)", args.minutes, "resuming from checkpoint" if resumed else "from now")
        totals = collect(BRONZE, CHECKPOINT, seconds=args.minutes * 60, batch_size=args.batch_size)

    logger.info("2/3 Transform and check")
    try:
        result = build(BRONZE, DB)
    except RuntimeError as error:
        logger.error(error)
        return 1
    logger.info("   bronze %(bronze_rows)s rows, silver %(silver_rows)s, duplicates removed %(duplicates_removed)s", result)

    logger.info("3/3 Report")
    page = build_report(DB, ROOT / "site", {**totals, **result})
    logger.info("Done: %s", page)
    return 0


if __name__ == "__main__":
    sys.exit(main())
