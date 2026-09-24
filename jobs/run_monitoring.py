from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.config import REPO_ROOT, PlatformConfig  # noqa: E402
from urbanplatform.monitoring import (  # noqa: E402
    MonitoringQuery,
    format_table,
    load_queries,
    register_views,
    render_markdown,
    run_queries,
)
from urbanplatform.session import build_session  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report the platform's operational status from the _meta tables."
    )
    parser.add_argument("names", nargs="*", help="questions to answer (default: all)")
    parser.add_argument("--list", action="store_true", help="list the questions and exit")
    parser.add_argument("--sql", help="run one ad-hoc query against the views instead")
    parser.add_argument("--limit", type=int, default=40, help="rows to show per query")
    parser.add_argument("--no-report", action="store_true", help="print only, write nothing")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    queries = load_queries()

    if args.list:
        width = max(len(q.name) for q in queries)
        for query in queries:
            print(f"{query.name:<{width}}  {query.question}")
            print(f"{'':<{width}}  reads: {', '.join(query.requires) or '-'}")
        return 0

    if args.names:
        known = {q.name: q for q in queries}
        unknown = [n for n in args.names if n not in known]
        if unknown:
            raise SystemExit(
                f"unknown question(s): {', '.join(unknown)}\n"
                f"available: {', '.join(known)}"
            )
        queries = [known[n] for n in args.names]

    if args.sql:
        queries = [MonitoringQuery(name="ad-hoc", question=args.sql, sql=args.sql)]

    platform = PlatformConfig.load()
    spark = build_session(platform)
    try:
        available = register_views(spark, platform)
        if not available:
            raise SystemExit(
                f"no metadata tables under {platform.meta_path('')} -- "
                "run jobs/run_ingest.py first"
            )
        print(f"views: {', '.join(available)}\n")
        results = run_queries(spark, queries, available, limit=args.limit)
    finally:
        spark.stop()

    for result in results:
        print("=" * 100)
        print(f"{result.query.name}")
        if result.query.question and result.query.question != result.query.sql:
            print(f"  {result.query.question}")
        print()
        if result.skipped:
            print(f"    skipped: {result.skipped}")
        elif result.error:
            print(f"    failed: {result.error}")
        else:
            print(format_table(result.columns, result.rows))
        print()

    if not args.no_report and not args.sql:
        out_dir = os.path.join(REPO_ROOT, "reports", "monitoring")
        os.makedirs(out_dir, exist_ok=True)
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(out_dir, f"{stamp}.md")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(render_markdown(results))
        print(f"report written to {os.path.relpath(path, REPO_ROOT)}")

    return 1 if any(r.error for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
