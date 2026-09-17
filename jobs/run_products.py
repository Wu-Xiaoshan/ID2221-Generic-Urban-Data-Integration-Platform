"""Task 4: build or refresh the analytical data products from the integrated dataset.

Writes each product to data/lakehouse/products/<name> and appends one row per product to
_meta/data_product_metadata. Run jobs/run_integrate.py first.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.analytics import AnalyticsConfig  # noqa: E402
from urbanplatform.analytics.materialize import refresh_products  # noqa: E402
from urbanplatform.analytics.products import ProductsConfig  # noqa: E402
from urbanplatform.config import PlatformConfig  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*", help="products to build (default: all)")
    parser.add_argument("--list", action="store_true", help="list the products and exit")
    parser.add_argument("--if-stale", action="store_true",
                        help="skip products already built from the current gold table version")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    products = ProductsConfig.load()

    if args.list:
        width = max(len(p.name) for p in products.products)
        for p in products.products:
            print(f"{p.name:<{width}}  v{p.schema_version}  {p.grain}")
            print(f"{'':<{width}}        {p.description}")
        return 0

    known = {p.name for p in products.products}
    unknown = [n for n in args.names if n not in known]
    if unknown:
        raise SystemExit(f"unknown product(s): {', '.join(unknown)}\navailable: {', '.join(sorted(known))}")

    platform = PlatformConfig.load()
    source = platform.table_path(products.source_layer, products.source_table)
    if not os.path.isdir(source):
        raise SystemExit(f"{source} does not exist -- run jobs/run_ingest.py and jobs/run_integrate.py first")

    spark = build_session(platform)
    try:
        runs = refresh_products(
            spark, platform, AnalyticsConfig.load(), products, args.names or None, args.if_stale
        )
    finally:
        spark.stop()

    print("\n" + "=" * 96)
    print(f"data product run {runs[0].run_id}  (source {runs[0].source_table} v{runs[0].source_version})")
    print(f"{'product':<28}{'status':<9}{'rows':>8}{'files':>7}{'size':>11}{'time':>9}  schema")
    for r in runs:
        size = f"{r.size_bytes / 1024:.1f} KB" if r.size_bytes is not None else "-"
        rows = f"{r.row_count:,}" if r.row_count is not None else "-"
        files = r.num_files if r.num_files is not None else "-"
        print(f"{r.product_name:<28}{r.status:<9}{rows:>8}{files:>7}{size:>11}{r.duration_sec:>8}s  "
              f"v{r.schema_version} {r.schema_fingerprint or ''}")
        if r.error:
            print(f"    FAILED: {r.error}")
    print("=" * 96)
    return 0 if all(r.status != "failed" for r in runs) else 1


if __name__ == "__main__":
    raise SystemExit(main())
