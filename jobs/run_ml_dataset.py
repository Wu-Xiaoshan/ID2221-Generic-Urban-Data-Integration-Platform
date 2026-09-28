"""Build a task's training dataset from the gold table.
Writes data/lakehouse/ml/<output>, partitioned by split, appends a row to _meta/ml_metadata
and writes reports/ml/<task>_dataset.md.
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.config import REPO_ROOT, PlatformConfig
from urbanplatform.ml import MLTaskConfig, available_tasks
from urbanplatform.ml.dataset import render_dataset_report, run_dataset
from urbanplatform.session import build_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task", nargs="?", default="trip_duration", help="a task in conf/ml/")
    parser.add_argument("--source-version", type=int, help="read the source table at this Delta version")
    parser.add_argument("--list", action="store_true", help="list the tasks and exit")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.list:
        for name in available_tasks():
            print(f"{name:<20} {MLTaskConfig.load(name).description}")
        return 0
    if args.task not in available_tasks():
        raise SystemExit(f"unknown task '{args.task}'; available: {', '.join(available_tasks())}")

    platform = PlatformConfig.load()
    task = MLTaskConfig.load(args.task)
    source = platform.table_path(task.source_layer, task.source_table)
    if not os.path.isdir(source):
        raise SystemExit(f"{source} does not exist -- run jobs/run_ingest.py and jobs/run_integrate.py first")

    spark = build_session(platform)
    try:
        run, profile = run_dataset(spark, platform, task, args.source_version)
    finally:
        spark.stop()

    print("\n" + "=" * 72)
    print(f"ml dataset run {run.run_id}: {run.status}")
    if run.status == "success":
        report = os.path.join(REPO_ROOT, "reports", "ml", f"{task.name}_dataset.md")
        os.makedirs(os.path.dirname(report), exist_ok=True)
        with open(report, "w", encoding="utf-8") as fh:
            fh.write(render_dataset_report(task, run, profile))
        split = run.details["split"]
        print(f"  source               {run.source_table} v{run.source_version}")
        print(f"  rows in / kept       {run.rows_in:,} / {run.details['rows_kept']:,}")
        print(f"  train / val / test   {run.rows_train:,} / {run.rows_validation:,} / {run.rows_test:,}")
        if "test_start" in split:
            print(f"  validation from      {split['validation_start']}, test from {split['test_start']}")
        print(f"  output               {run.output_table} v{run.output_version}")
        print(f"  report               {os.path.relpath(report, REPO_ROOT)}")
        print(f"  duration             {run.duration_sec}s")
    else:
        print(f"  FAILED: {run.error}")
    print("=" * 72)
    return 0 if run.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
