"""Bring the gold table and the data products up to date after silver changed.

    python jobs/run_refresh.py          # only what changed
    python jobs/run_refresh.py --full   # rebuild gold and every product

Run it after jobs/run_update.py. Gold is updated from the silver change feeds: new and
changed trips are enriched and merged in, and trips whose pickup-hour weather or air quality
changed are recomputed. Each product is then left alone, refreshed for the changed months,
or rebuilt, as conf/products.yaml declares. Whenever the change feed cannot say what
changed, the step falls back to a full rebuild and records why.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.analytics import AnalyticsConfig  # noqa: E402
from urbanplatform.analytics.materialize import refresh_products  # noqa: E402
from urbanplatform.analytics.products import ProductsConfig  # noqa: E402
from urbanplatform.config import PlatformConfig  # noqa: E402
from urbanplatform.integrate import IntegrationConfig, refresh_integration, run_integration  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--full", action="store_true", help="rebuild gold and every product")
    args = parser.parse_args()

    platform = PlatformConfig.load()
    integration = IntegrationConfig.load()
    products = ProductsConfig.load()

    spark = build_session(platform)
    started = time.perf_counter()
    try:
        if args.full:
            gold = run_integration(spark, platform, integration, note="--full requested")
        else:
            gold = refresh_integration(spark, platform, integration)
        runs = []
        if gold.status == "success":
            runs = refresh_products(
                spark, platform, AnalyticsConfig.load(), products, incremental=not args.full
            )
        elapsed = time.perf_counter() - started
    finally:
        spark.stop()

    print("\n" + "=" * 92)
    print(f"gold {gold.target_table}: {gold.status}, {gold.mode} in {gold.duration_sec}s")
    if gold.note:
        print(f"  full rebuild because: {gold.note}")
    if gold.mode == "incremental":
        print(f"  rows inserted {gold.rows_inserted:,}, updated {gold.rows_updated:,}, "
              f"deleted {gold.rows_deleted:,}; gold now has {gold.trips_out:,} rows")
    if gold.error:
        print(f"  FAILED: {gold.error}")
    if runs:
        print(f"\n{'product':<28}{'status':<9}{'mode':<13}{'months':<18}{'rows':>8}{'time':>9}")
        for r in runs:
            rows = f"{r.row_count:,}" if r.row_count is not None else "-"
            print(f"{r.product_name:<28}{r.status:<9}{r.refresh_mode or '-':<13}"
                  f"{r.refreshed_partitions or '-':<18}{rows:>8}{r.duration_sec:>8}s")
            if r.note:
                print(f"    {r.note}")
            if r.error:
                print(f"    FAILED: {r.error}")
    print(f"\ntotal {elapsed:.1f}s, excluding Spark start-up")
    print("=" * 92)

    failed = gold.status != "success" or any(r.status == "failed" for r in runs)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
