# Run the integration pipeline: reads the four silver tables, joins them into gold/integrated_taxi_trips, and appends
# one row to _meta/integration_metadata. Run jobs/run_ingest.py first.

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.config import PlatformConfig
from urbanplatform.integrate import IntegrationConfig, run_integration
from urbanplatform.session import build_session


def main() -> int:
    platform = PlatformConfig.load()
    cfg = IntegrationConfig.load()

    spark = build_session(platform)
    try:
        run = run_integration(spark, platform, cfg)
    finally:
        spark.stop()

    print("\n" + "=" * 68)
    print(f"integration run {run.run_id}: {run.status}")
    if run.status == "success":
        print(f"  trips in / out       {run.trips_in:,} / {run.trips_out:,}")
        print(f"  unlocated trips      {run.unlocated_trips:,}")
        print(f"  output files         {run.output_files}")
        for name, cov in run.context_coverage.items():
            total = sum(cov.values()) or 1
            print(
                f"  {name:<18} exact {cov['exact']:,} "
                f"({100 * cov['exact'] / total:.1f}%)  filled {cov['filled']:,}  "
                f"missing {cov['missing']:,}"
            )
        print(f"  duration             {run.duration_sec}s")
    else:
        print(f"  FAILED: {run.error}")
    print("=" * 68)

    return 0 if run.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
