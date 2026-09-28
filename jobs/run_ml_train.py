"""Train, evaluate and save a task's model (Week 4 Task 3).

Fits the feature pipeline of conf/ml/<task>.yaml and the estimator under `model` as one Spark ML
Pipeline on the dataset's training split, evaluates it on every split, and saves it as the next
version under data/lakehouse/ml/models/<output>/. Appends a row to _meta/ml_metadata and writes
reports/ml/<task>_model.md.

Retraining after new data reaches gold:

    python jobs/run_ml_train.py --rebuild     # rebuild the dataset from the newest gold, then train
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.config import REPO_ROOT, PlatformConfig
from urbanplatform.ml import MLTaskConfig, available_tasks
from urbanplatform.ml.dataset import render_dataset_report, run_dataset
from urbanplatform.ml.model import model_history, render_model_report, run_training
from urbanplatform.session import build_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task", nargs="?", default="trip_duration", help="a task in conf/ml/")
    parser.add_argument("--dataset-version", type=int, help="train on the training dataset at this Delta version")
    parser.add_argument("--rebuild", action="store_true",
                        help="rebuild the training dataset from gold first (what run_ml_dataset.py does)")
    parser.add_argument("--source-version", type=int, help="with --rebuild: read gold at this Delta version")
    return parser.parse_args()


def write_report(name: str, text: str) -> str:
    path = os.path.join(REPO_ROOT, "reports", "ml", name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)
    return os.path.relpath(path, REPO_ROOT)


def main() -> int:
    args = parse_args()
    if args.task not in available_tasks():
        raise SystemExit(f"unknown task '{args.task}'; available: {', '.join(available_tasks())}")
    if args.rebuild and args.dataset_version is not None:
        raise SystemExit("--rebuild trains on the dataset it builds; leave out --dataset-version")
    if args.source_version is not None and not args.rebuild:
        raise SystemExit("--source-version needs --rebuild")

    platform = PlatformConfig.load()
    task = MLTaskConfig.load(args.task)
    if task.model is None:
        raise SystemExit(f"conf/ml/{task.name}.yaml has no `model` section")
    dataset = platform.table_path("ml", task.dataset.output)
    if not args.rebuild and not os.path.isdir(dataset):
        raise SystemExit(f"{dataset} does not exist -- run jobs/run_ml_dataset.py {task.name} first, or pass --rebuild")

    spark = build_session(platform)
    try:
        if args.rebuild:
            ds_run, profile = run_dataset(spark, platform, task, args.source_version)
            if ds_run.status != "success":
                print(f"\ndataset rebuild failed: {ds_run.error}")
                return 1
            report = write_report(f"{task.name}_dataset.md", render_dataset_report(task, ds_run, profile))
            print(f"\ndataset rebuilt from {ds_run.source_table} v{ds_run.source_version}: "
                  f"{ds_run.output_table} v{ds_run.output_version}, {report}")
        run, card = run_training(spark, platform, task, args.dataset_version)
        history = model_history(platform, task) if run.status == "success" else []
    finally:
        spark.stop()

    print("\n" + "=" * 72)
    print(f"ml train run {run.run_id}: {run.status}")
    if run.status == "success":
        report = write_report(f"{task.name}_model.md", render_model_report(task, run, card, history))
        head = task.model.metrics[0]
        ds = card["dataset"]
        print(f"  dataset              {ds['table']} v{ds['version']} (gold v{ds.get('source_version')})")
        print(f"  train / val / test   {run.rows_train:,} / {run.rows_validation:,} / {run.rows_test:,}")
        for s in ("validation", "test"):
            if s in card["metrics"]:
                shown = "  ".join(f"{m} {v:.4f}" for m, v in card["metrics"][s].items())
                base = card["baseline"].get(s, {}).get(head)
                print(f"  {s:<20} {shown}" + (f"  (mean baseline {head} {base:.4f})" if base is not None else ""))
        prev = card.get("previous")
        if prev and "test" in prev:
            print(f"  previous v{prev['version']:<11} test {head} {prev['test'][head]:.4f} on this test split")
        print(f"  model                {run.output_table}/v{card['version']}")
        print(f"  report               {report}")
        print(f"  fit / eval / total   {card['fit_sec']}s / {card['evaluate_sec']}s / {run.duration_sec}s")
    else:
        print(f"  FAILED: {run.error}")
    print("=" * 72)
    return 0 if run.status == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
