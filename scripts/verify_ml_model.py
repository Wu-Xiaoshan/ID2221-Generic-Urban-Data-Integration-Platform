# Correctness checks for a task's saved models: lineage, reloading, determinism, no test data in the fit.

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexerModel
from pyspark.sql import functions as F

from urbanplatform.config import PlatformConfig
from urbanplatform.ml import MLTaskConfig
from urbanplatform.ml.dataset import SPLIT
from urbanplatform.ml.features import _CAT, fitted_stage, prepare
from urbanplatform.ml.model import (
    PIPELINE_DIR,
    build_training_pipeline,
    dataset_lineage,
    evaluate,
    latest_version,
    load_model,
    model_history,
    registry_path,
)
from urbanplatform.session import build_session
from verify_ml import Checks


def close(a: float, b: float, tol: float = 1e-9) -> bool:
    return abs(a - b) <= tol * max(1.0, abs(a), abs(b))


def main() -> int:
    task = MLTaskConfig.load(sys.argv[1] if len(sys.argv) > 1 else "trip_duration")
    platform = PlatformConfig.load()
    root = registry_path(platform, task)
    history = model_history(platform, task)
    if not history:
        raise SystemExit(f"no model in {root} -- run jobs/run_ml_train.py {task.name} first")
    spark = build_session(platform)
    check = Checks()
    try:
        print(f"\nmodel registry ml/models/{task.model.output}")
        versions = [c["version"] for c in history]
        check("latest is the newest version", latest_version(root) == versions[-1],
              f"versions {', '.join(f'v{v}' for v in versions)}, latest v{latest_version(root)}")
        stages_ok = all(len(PipelineModel.load(os.path.join(root, f"v{c['version']}", PIPELINE_DIR)).stages)
                        == len(c["stages"]) for c in history)
        check("every version loads", stages_ok, f"{len(history)} saved pipeline(s) reloaded with their stages")

        model, card = load_model(platform, task)
        ds = card["dataset"]
        print(f"\nlatest model v{card['version']}, trained on {ds['table']} v{ds['version']}")
        check("current configuration", card["model_fingerprint"] == task.model_fingerprint,
              f"trained under {card['model_fingerprint']}, conf/ml is {task.model_fingerprint}")
        lineage = dataset_lineage(spark, platform, task, ds["version"])
        check("lineage recorded", lineage.get("source_version") == ds.get("source_version")
              and lineage.get("config_fingerprint") == ds.get("config_fingerprint"),
              f"dataset v{ds['version']} was built from gold v{lineage.get('source_version')}, "
              f"the card says v{ds.get('source_version')}")

        dataset = spark.read.format("delta").option("versionAsOf", ds["version"]).load(
            platform.table_path("ml", task.dataset.output))
        train = dataset.filter(F.col(SPLIT) == "train")
        test = dataset.filter(F.col(SPLIT) == "test")
        n_train = train.count()
        check("trained on the recorded split", n_train == card["rows"]["train"],
              f"{n_train:,} training rows at dataset v{ds['version']}, card {card['rows']['train']:,}")

        again = evaluate(model.transform(test), card["task_type"], task.model.metrics)
        check("reloaded model reproduces its test metrics",
              all(close(again[m], card["metrics"]["test"][m]) for m in again),
              ", ".join(f"{m} {again[m]:.6f} vs {card['metrics']['test'][m]:.6f}" for m in again))

        spec = task.model_features
        pipeline, _, _ = build_training_pipeline(spec, task.model, train)
        refit = pipeline.fit(train)
        old, new = model.stages[-1], refit.stages[-1]
        if hasattr(old, "coefficients"):
            diff = max(abs(a - b) for a, b in zip(old.coefficients.toArray(), new.coefficients.toArray()))
            same = diff <= 1e-9 and close(old.intercept, new.intercept)
            detail = f"largest coefficient difference {diff:.2e}, intercept {old.intercept:.6f} vs {new.intercept:.6f}"
        else:
            same = all(close(a, b) for a, b in zip(old.featureImportances.toArray(), new.featureImportances.toArray()))
            detail = "same feature importances" if same else "feature importances differ"
        check("refit on the same data gives the same model", same, detail)

        indexer = fitted_stage(model, StringIndexerModel)
        categorical = [f.name for f in spec.of_kind("categorical")]
        seen = prepare(spec, train).agg(*[F.collect_set(_CAT + n).alias(n) for n in categorical]).first().asDict()
        mismatched = [n for n, labels in zip(categorical, indexer.labelsArray) if set(labels) != set(seen[n])]
        check("categories learnt from train only", not mismatched,
              f"{len(categorical)} category lists equal the training values"
              + (f"; differ: {', '.join(mismatched)}" if mismatched else ""))

        prev = card.get("previous")
        if prev:
            check("previous version scored on the new test split", "test" in prev,
                  f"v{prev['version']} test {task.model.metrics[0]} {prev['test'][task.model.metrics[0]]:.4f}"
                  if "test" in prev else prev.get("error", ""))
    finally:
        spark.stop()

    print(f"\n{check.passed} passed, {check.failed} failed")
    return 1 if check.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
