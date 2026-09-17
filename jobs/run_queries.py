"""Task 2: run the analytical query library against the Week 1 output.
"""

from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.analytics import AnalyticsConfig, build_queries, register_views  # noqa: E402
from urbanplatform.config import PlatformConfig  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("names", nargs="*", help="queries to run (default: all)")
    parser.add_argument("--list", action="store_true", help="list the queries and exit")
    parser.add_argument("--rows", type=int, default=20, help="rows to display (default 20)")
    parser.add_argument("--explain", action="store_true",
                        help="print EXPLAIN FORMATTED instead of running")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    analytics = AnalyticsConfig.load()
    queries = build_queries(analytics)
    by_name = {q.name: q for q in queries}

    if args.list:
        width = max(len(q.name) for q in queries)
        print(f"{'query':<{width}}  analysis  title")
        for q in queries:
            print(f"{q.name:<{width}}  {q.analysis:^8}  {q.title}")
        return 0

    if args.names:
        unknown = [n for n in args.names if n not in by_name]
        if unknown:
            raise SystemExit(
                f"unknown quer{'y' if len(unknown) == 1 else 'ies'}: {', '.join(unknown)}\n"
                f"available: {', '.join(by_name)}"
            )
        selected = [by_name[n] for n in args.names]
    else:
        selected = queries

    platform = PlatformConfig.load()
    source = analytics.sources["integrated"]
    integrated = platform.table_path(source["layer"], source["table"])
    if not os.path.isdir(integrated):
        raise SystemExit(
            f"{integrated} does not exist -- run jobs/run_ingest.py and "
            "jobs/run_integrate.py first"
        )

    spark = build_session(platform)
    try:
        register_views(spark, platform, analytics)

        timings: list[tuple[str, float]] = []
        for query in selected:
            print(f"\n{'=' * 78}\n[{query.name}]  analysis {query.analysis}: {query.title}")
            if query.optimisation:
                print(f"Task 3 vehicle for: {query.optimisation}")
            print("=" * 78)

            if args.explain:
                spark.sql(query.sql).explain(mode="formatted")
                continue

            started = time.perf_counter()
            df = spark.sql(query.sql)
            df.show(args.rows, truncate=False)
            elapsed = time.perf_counter() - started
            timings.append((query.name, elapsed))
            print(f"-> {elapsed:.2f} s")

        if timings:
            print(f"\n{'=' * 78}\nwall-clock, single cold run (not a benchmark)\n{'=' * 78}")
            width = max(len(n) for n, _ in timings)
            for name, elapsed in timings:
                print(f"  {name:<{width}}  {elapsed:7.2f} s")
            print(f"  {'total':<{width}}  {sum(t for _, t in timings):7.2f} s")
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
