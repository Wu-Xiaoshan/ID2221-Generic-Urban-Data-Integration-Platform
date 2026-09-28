"""Fits the pipeline declared under `features` in conf/ml/<task>.yaml on the dataset's training
split, applies it to all three splits
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.config import REPO_ROOT, PlatformConfig
from urbanplatform.ml import MLTaskConfig, available_tasks
from urbanplatform.ml.featurize import render_features_report, run_features
from urbanplatform.session import build_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task", nargs="?", default="trip_duration", help="a task in conf/ml/")
    parser.add_argument("--dataset-version", type=int, help="read the training dataset at this Delta version")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.task not in available_tasks():
        raise SystemExit(f"unknown task '{args.task}'; available: {', '.join(available_tasks())}")

    platform = PlatformConfig.load()
    task = MLTaskConfig.load(args.task)
    dataset = platform.table_path("ml", task.dataset.output)
    if not os.path.isdir(dataset):
        raise SystemExit(f"{dataset} does not exist -- run jobs/run_ml_dataset.py {task.name} first")

    spark = build_session(platform)
    try:
        run, info = run_features(spark, platform, task, args.dataset_version)
    finally:
        spark.stop()

    print("\n" + "=" * 72)
    print(f"ml features run {run.run_id}: {run.status}")
    if run.status == "success":
        report = os.path.join(REPO_ROOT, "reports", "ml", f"{task.name}_features.md")
        os.makedirs(os.path.dirname(report), exist_ok=True)
        with open(report, "w", encoding="utf-8") as fh:
            fh.write(render_features_report(task, run, info))
        d = run.details
        print(f"  dataset              {run.source_table} v{run.source_version}")
        print(f"  vector               {d['vector_size']} slots: "
              + ", ".join(f"{k} {v}" for k, v in sorted(d["slots_by_source"].items(), key=lambda kv: -kv[1])))
        print(f"  missing flags        {', '.join(d['missing_indicators']) or 'none'}")
        print(f"  train / val / test   {run.rows_train:,} / {run.rows_validation:,} / {run.rows_test:,}")
        print(f"  pipeline             {d['pipeline_path']}")
        print(f"  output               {run.output_table} v{run.output_version}")
        print(f"  report               {os.path.relpath(report, REPO_ROOT)}")
        print(f"  fit / write / total  {d['fit_sec']}s / {d['transform_write_sec']}s / {run.duration_sec}s")
    else:
        print(f"  FAILED: {run.error}")
    print("=" * 72)
    return 0 if run.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
