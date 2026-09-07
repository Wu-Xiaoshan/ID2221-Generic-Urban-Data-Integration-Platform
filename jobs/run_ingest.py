"""Run the ingestion framework.

    python jobs/run_ingest.py                      # every dataset
    python jobs/run_ingest.py taxi_zones weather   # only these
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.config import PlatformConfig, load_datasets  # noqa: E402
from urbanplatform.ingest import ingest_all  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402


def main(argv: list[str]) -> int:
    platform = PlatformConfig.load()
    configs = load_datasets(argv or None)

    spark = build_session(platform)
    try:
        runs = ingest_all(spark, platform, configs)
    finally:
        spark.stop()

    print("\n" + "=" * 68)
    print(f"{'dataset':<20} {'status':<9} {'read':>11} {'written':>11} {'rejected':>9} {'sec':>7}")
    print("-" * 68)
    for run in runs:
        print(
            f"{run.dataset_name:<20} {run.status:<9} "
            f"{run.records_read or 0:>11,} {run.records_written or 0:>11,} "
            f"{run.records_rejected or 0:>9,} {run.duration_sec or '':>7}"
        )
    print("=" * 68)

    return 1 if any(run.status != "success" for run in runs) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
