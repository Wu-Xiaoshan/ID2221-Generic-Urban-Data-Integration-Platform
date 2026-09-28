from __future__ import annotations

import argparse
import dataclasses
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from urbanplatform.config import REPO_ROOT, PlatformConfig
from urbanplatform.ml import MLTaskConfig
from urbanplatform.ml.config import FeaturesSpec
from urbanplatform.ml.dataset import SPLIT
from urbanplatform.ml.features import FEATURES, feature_manifest, fit_feature_pipeline
from urbanplatform.session import build_session

SEED = 7


def sample(df: DataFrame, key: str, fraction: float) -> DataFrame:
    return df.where(F.pmod(F.xxhash64(F.col(key), F.lit(SEED)), F.lit(100_000)) < int(fraction * 100_000))


def estimator(name: str):
    if name == "lr":
        return LinearRegression(featuresCol=FEATURES, labelCol="label", regParam=0.01, elasticNetParam=0.0)
    return RandomForestRegressor(featuresCol=FEATURES, labelCol="label", numTrees=30, maxDepth=10,
                                 maxBins=300, subsamplingRate=0.8, seed=SEED)


def spec_for(model: str, spec: FeaturesSpec) -> FeaturesSpec:
    return dataclasses.replace(spec, encoding="index", scaling="none") if model == "rf" else spec


def evaluate(pred: DataFrame) -> dict[str, float]:
    ev = RegressionEvaluator(labelCol="label", predictionCol="prediction")
    return {m: ev.setMetricName(m).evaluate(pred) for m in ("rmse", "mae", "r2")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task", nargs="?", default="trip_duration")
    parser.add_argument("--train-frac", type=float, default=0.05, help="share of the train split used")
    parser.add_argument("--val-frac", type=float, default=0.10, help="share of the validation split used")
    parser.add_argument("--models", default="lr,rf", help="comma-separated: lr, rf")
    args = parser.parse_args()

    platform = PlatformConfig.load()
    task = MLTaskConfig.load(args.task)
    spec = task.features
    models = [m.strip() for m in args.models.split(",") if m.strip()]
    context = [s for s in spec.sources if s != "taxi_trips"]
    variants = [("all datasets", set())]
    variants += [(f"without {s}", {s}) for s in context]
    variants += [("taxi_trips only", set(context))]

    spark = build_session(platform)
    results, importances = [], {}
    try:
        dataset = spark.read.format("delta").load(platform.table_path("ml", task.dataset.output))
        train = sample(dataset.filter(F.col(SPLIT) == "train"), task.key, args.train_frac).cache()
        val = sample(dataset.filter(F.col(SPLIT) == "validation"), task.key, args.val_frac).cache()
        n_train, n_val = train.count(), val.count()
        mean = train.agg(F.avg("label")).first()[0]
        baseline = evaluate(val.withColumn("prediction", F.lit(mean)))
        print(f"train sample {n_train:,}, validation sample {n_val:,}, mean baseline rmse {baseline['rmse']:.3f}")

        for model in models:
            for name, excluded in variants:
                variant = spec_for(model, spec.without_sources(excluded))
                t0 = time.perf_counter()
                fe, _ = fit_feature_pipeline(variant, train)
                tr = fe.transform(train).cache()
                fitted = estimator(model).fit(tr)
                metrics = evaluate(fitted.transform(fe.transform(val)))
                seconds = time.perf_counter() - t0
                size = tr.schema[FEATURES].metadata["ml_attr"]["num_attrs"]
                results.append((model, name, len(variant.features), size, metrics, seconds))
                print(f"{model:<3} {name:<24} {len(variant.features):>3} features {size:>4} slots  "
                      f"rmse {metrics['rmse']:.3f}  mae {metrics['mae']:.3f}  r2 {metrics['r2']:.4f}  {seconds:.0f}s")
                if model == "rf" and not excluded:
                    manifest = feature_manifest(variant, tr.schema[FEATURES])
                    for slot, weight in zip(manifest.slots, fitted.featureImportances.toArray()):
                        key = (slot.feature, slot.source)
                        importances[key] = importances.get(key, 0.0) + float(weight)
                tr.unpersist()
    finally:
        spark.stop()

    lines = [
        f"# Feature analysis: {task.name}",
        "",
        f"Probe models on a hash sample of {n_train:,} training rows ({args.train_frac:.0%} of train),",
        f"judged on {n_val:,} validation rows ({args.val_frac:.0%} of validation). Predicting the training",
        f"mean ({mean:.2f} min) gives RMSE {baseline['rmse']:.3f}, MAE {baseline['mae']:.3f}.",
        "",
        "- `lr`: linear regression (ridge 0.01) on the pipeline as configured: one-hot, standardised.",
        "- `rf`: random forest, 30 trees of depth 10, on the same pipeline with `encoding: index` and",
        "  `scaling: none`, switched in the configuration object only.",
        "",
        "## Ablation by source dataset",
        "",
        "| model | features from | features | slots | RMSE | MAE | R² | RMSE change vs all | time |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    full = {model: m["rmse"] for model, name, _, _, m, _ in results if name == "all datasets"}
    for model, name, n_features, size, m, seconds in results:
        delta = m["rmse"] - full[model]
        lines.append(f"| {model} | {name} | {n_features} | {size} | {m['rmse']:.3f} | {m['mae']:.3f} | "
                     f"{m['r2']:.4f} | {'-' if name == 'all datasets' else f'{delta:+.3f}'} | {seconds:.0f} s |")

    if importances:
        by_source: dict[str, float] = {}
        for (_, source), w in importances.items():
            by_source[source] = by_source.get(source, 0.0) + w
        lines += [
            "",
            "## Random-forest importance",
            "",
            "| source dataset | importance |",
            "|---|---:|",
            *[f"| {s} | {w:.3f} |" for s, w in sorted(by_source.items(), key=lambda kv: -kv[1])],
            "",
            "| feature | source | importance |",
            "|---|---|---:|",
            *[f"| `{f}` | {s} | {w:.4f} |" for (f, s), w in sorted(importances.items(), key=lambda kv: -kv[1])],
        ]
    lines.append("")

    report = os.path.join(REPO_ROOT, "reports", "ml", f"{task.name}_feature_analysis.md")
    os.makedirs(os.path.dirname(report), exist_ok=True)
    with open(report, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"wrote {os.path.relpath(report, REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
