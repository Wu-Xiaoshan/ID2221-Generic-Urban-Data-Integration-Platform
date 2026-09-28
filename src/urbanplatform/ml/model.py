"""Week 4 Task 3: the model.

One Spark ML Pipeline, the Task 2 feature stages followed by an estimator, is fitted on the
training split of a task's dataset, evaluated on every split, and saved as the next numbered
version under ml/models/<output>/. Retraining is the same run on a newer dataset: earlier
versions stay where they are, and the new one is compared with the last on the new test split.
"""

from __future__ import annotations

import dataclasses
import json
import os
import re
import time
import traceback
import uuid
from collections.abc import Callable
from typing import Any

from pyspark.ml import Estimator, Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import PlatformConfig
from ..writer import table_properties, table_version
from .config import SPLITS, FeaturesSpec, MLTaskConfig, ModelSpec
from .dataset import LABEL, SPLIT
from .features import (
    FEATURES,
    FeatureManifest,
    build_feature_pipeline,
    check_inputs,
    feature_manifest,
    numerics_with_nulls,
    prepare,
)
from .featurize import MANIFEST
from .metadata import PROP, MLRun, utc_now, write_ml_metadata

PREDICTION = "prediction"
PIPELINE_DIR = "pipeline"
MODEL_CARD = "model.json"
LATEST = "latest.json"

# name -> (task type, function(params) -> unfitted estimator)
_ESTIMATORS: dict[str, tuple[str, Callable[[dict[str, Any]], Estimator]]] = {}

# task type -> its evaluator and the metrics that evaluator knows
_EVALUATORS: dict[str, tuple[type, tuple[str, ...]]] = {
    "regression": (RegressionEvaluator, ("rmse", "mse", "mae", "r2", "var")),
}


def estimator(name: str, task_type: str) -> Callable:
    def register(fn: Callable[[dict[str, Any]], Estimator]) -> Callable[[dict[str, Any]], Estimator]:
        _ESTIMATORS[name] = (task_type, fn)
        return fn

    return register


@estimator("linear_regression", task_type="regression")
def linear_regression(params: dict[str, Any]) -> Estimator:
    return LinearRegression(featuresCol=FEATURES, labelCol=LABEL, predictionCol=PREDICTION, **params)


def make_estimator(spec: ModelSpec) -> tuple[str, Estimator]:
    try:
        task_type, fn = _ESTIMATORS[spec.type]
    except KeyError:
        raise ValueError(f"unknown model type '{spec.type}'; known: {', '.join(sorted(_ESTIMATORS))}") from None
    _, known = _EVALUATORS[task_type]
    unknown = [m for m in spec.metrics if m not in known]
    if unknown:
        raise ValueError(f"metric(s) {', '.join(unknown)} do not apply to {task_type}; known: {', '.join(known)}")
    return task_type, fn(spec.params)


def build_training_pipeline(
    spec: FeaturesSpec, model: ModelSpec, train: DataFrame
) -> tuple[Pipeline, list[str], str]:
    """The Task 2 feature stages with the estimator appended, unfitted.

    Which numerics get a missing flag is decided on ``train`` first, as in fit_feature_pipeline.
    Returns the pipeline, those numerics and the task type.
    """
    check_inputs(spec, train.columns)
    task_type, est = make_estimator(model)
    indicators = numerics_with_nulls(spec, prepare(spec, train)) if spec.missing_indicators else []
    stages = build_feature_pipeline(spec, indicators).getStages()
    return Pipeline(stages=[*stages, est]), indicators, task_type


def evaluate(pred: DataFrame, task_type: str, metrics: list[str]) -> dict[str, float]:
    cls, _ = _EVALUATORS[task_type]
    ev = cls(labelCol=LABEL, predictionCol=PREDICTION)
    return {m: float(ev.setMetricName(m).evaluate(pred)) for m in metrics}


# the model registry: ml/models/<output>/v<N>/{pipeline/, model.json, feature_manifest.json}
def registry_path(platform: PlatformConfig, task: MLTaskConfig) -> str:
    return os.path.join(platform.table_path("ml", "models"), task.model.output)


def model_versions(root: str) -> list[int]:
    """The versions whose model card was written, i.e. that finished."""
    if not os.path.isdir(root):
        return []
    return sorted(
        int(m.group(1)) for d in os.listdir(root)
        if (m := re.fullmatch(r"v(\d+)", d)) and os.path.isfile(os.path.join(root, d, MODEL_CARD))
    )


def latest_version(root: str) -> int | None:
    try:
        with open(os.path.join(root, LATEST), encoding="utf-8") as fh:
            return int(json.load(fh)["version"])
    except FileNotFoundError:
        versions = model_versions(root)
        return versions[-1] if versions else None


def _next_version(root: str) -> int:
    # Counts unfinished directories too, so a failed run's leftovers are never overwritten.
    taken = [int(m.group(1)) for d in os.listdir(root) if (m := re.fullmatch(r"v(\d+)", d))] if os.path.isdir(root) else []
    return max(taken, default=0) + 1


def read_card(root: str, version: int) -> dict[str, Any]:
    with open(os.path.join(root, f"v{version}", MODEL_CARD), encoding="utf-8") as fh:
        return json.load(fh)


def model_history(platform: PlatformConfig, task: MLTaskConfig) -> list[dict[str, Any]]:
    root = registry_path(platform, task)
    return [read_card(root, v) for v in model_versions(root)]


def load_model(
    platform: PlatformConfig, task: MLTaskConfig, version: int | None = None
) -> tuple[PipelineModel, dict[str, Any]]:
    """A saved model and its card; the latest version unless one is given.

    The pipeline scores rows of the task's dataset, labelled or not. Loading it needs `src/` on
    the path, for the SelectColumns and GroupImputer stages.
    """
    root = registry_path(platform, task)
    version = latest_version(root) if version is None else version
    if version is None:
        raise FileNotFoundError(f"no model in {root} -- run jobs/run_ml_train.py {task.name} first")
    return PipelineModel.load(os.path.join(root, f"v{version}", PIPELINE_DIR)), read_card(root, version)


def dataset_lineage(spark: SparkSession, platform: PlatformConfig, task: MLTaskConfig, version: int) -> dict[str, Any]:
    """The gold version, configuration and split a version of the training dataset was built from.

    The table properties describe the current version only, so an older version is looked up in
    _meta/ml_metadata instead.
    """
    path = platform.table_path("ml", task.dataset.output)
    if version == table_version(spark, path):
        props = table_properties(spark, path)
        source_version = props.get(PROP + "source_version")
        return {
            "source_table": props.get(PROP + "source_table"),
            "source_version": int(source_version) if source_version is not None else None,
            "config_fingerprint": props.get(PROP + "config_fingerprint"),
            "split": json.loads(props.get(PROP + "split", "{}")),
        }
    meta = platform.meta_path("ml_metadata")
    if os.path.isdir(meta):
        row = (
            spark.read.format("delta").load(meta)
            .where((F.col("stage") == "dataset") & (F.col("status") == "success")
                   & (F.col("output_table") == f"ml/{task.dataset.output}")
                   & (F.col("output_version") == version))
            .orderBy(F.col("started_at").desc())
            .first()
        )
        if row is not None:
            return {
                "source_table": row["source_table"],
                "source_version": row["source_version"],
                "config_fingerprint": row["config_fingerprint"],
                "split": json.loads(row["details"] or "{}").get("split", {}),
            }
    return {}


def model_weights(fitted: Any, manifest: FeatureManifest) -> dict[str, Any] | None:
    """The fitted estimator's weights mapped back to features: coefficients of a linear model,
    importances of a tree model.

    A feature with one slot (numeric, binary, missing flag) gets its weight. A one-hot feature
    gets its number of slots and its lowest and highest weight, since a list of single slots is
    dominated by rare categories.
    """
    if hasattr(fitted, "coefficients"):
        kind, weights = "coefficient", fitted.coefficients.toArray().tolist()
    elif hasattr(fitted, "featureImportances"):
        kind, weights = "importance", fitted.featureImportances.toArray().tolist()
    else:
        return None
    groups: dict[str, list[tuple[Any, float]]] = {}
    for slot, w in zip(manifest.slots, weights):
        name = slot.feature + (" (missing flag)" if slot.detail == "missing flag" else "")
        groups.setdefault(name, []).append((slot, w))
    single, onehot = [], []
    for name, slots in groups.items():
        source = slots[0][0].source
        if len(slots) == 1:
            single.append({"feature": name, "source": source, "detail": slots[0][0].detail, "weight": slots[0][1]})
        else:
            lo, hi = min(slots, key=lambda sw: sw[1]), max(slots, key=lambda sw: sw[1])
            onehot.append({"feature": name, "source": source, "slots": len(slots),
                           "low": [lo[0].detail, lo[1]], "high": [hi[0].detail, hi[1]],
                           "total": sum(abs(w) for _, w in slots)})
    return {
        "kind": kind,
        "intercept": float(fitted.intercept) if hasattr(fitted, "intercept") else None,
        "nonzero": sum(1 for w in weights if w != 0),
        "single": sorted(single, key=lambda f: -abs(f["weight"])),
        "grouped": sorted(onehot, key=lambda f: -(f["high"][1] - f["low"][1])),
    }


def _breakdown(scored: DataFrame, dataset: DataFrame, key: str, column: str) -> list[dict[str, Any]]:
    test = dataset.filter(F.col(SPLIT) == "test").select(key, column)
    err = F.col(PREDICTION) - F.col(LABEL)
    rows = (
        scored.filter(F.col(SPLIT) == "test").join(test, key)
        .groupBy(column)
        .agg(F.count(F.lit(1)).alias("rows"), F.avg(LABEL).alias("label_mean"),
             F.sqrt(F.avg(err * err)).alias("rmse"), F.avg(F.abs(err)).alias("mae"))
        .orderBy(F.desc("rows"))
        .collect()
    )
    return [r.asDict() for r in rows]


def run_training(
    spark: SparkSession, platform: PlatformConfig, task: MLTaskConfig, dataset_version: int | None = None
) -> tuple[MLRun, dict[str, Any]]:
    if task.model is None:
        raise ValueError(f"[{task.name}] has no `model` section in conf/ml/{task.name}.yaml")
    spec, mspec = task.model_features, task.model
    dataset_path = platform.table_path("ml", task.dataset.output)
    root = registry_path(platform, task)
    run = MLRun(run_id=str(uuid.uuid4()), task=task.name, stage="train", started_at=utc_now(),
                source_table=f"ml/{task.dataset.output}", output_table=f"ml/models/{mspec.output}",
                config_fingerprint=task.model_fingerprint)
    t0 = time.perf_counter()
    card: dict[str, Any] = {}
    scored = None
    try:
        version = table_version(spark, dataset_path) if dataset_version is None else dataset_version
        run.source_version = version
        lineage = dataset_lineage(spark, platform, task, version)
        dataset = spark.read.format("delta").option("versionAsOf", version).load(dataset_path)
        if mspec.breakdown and mspec.breakdown not in dataset.columns:
            raise ValueError(f"model.breakdown: '{mspec.breakdown}' is not a column of {run.source_table}")
        train = dataset.filter(F.col(SPLIT) == "train")

        pipeline, indicators, task_type = build_training_pipeline(spec, mspec, train)
        t_fit = time.perf_counter()
        model = pipeline.fit(train)
        fit_sec = time.perf_counter() - t_fit

        # One pass through the fitted pipeline for every split; the metrics then read the cache.
        t_eval = time.perf_counter()
        scored = model.transform(dataset).select(task.key, SPLIT, LABEL, PREDICTION).cache()
        rows = {r[SPLIT]: int(r["count"]) for r in scored.groupBy(SPLIT).count().collect()}
        run.split_rows(rows)
        run.rows_in = sum(rows.values())
        metrics = {s: evaluate(scored.filter(F.col(SPLIT) == s), task_type, mspec.metrics)
                   for s in SPLITS if rows.get(s)}

        baseline: dict[str, Any] = {}
        if task_type == "regression":
            mean = float(train.agg(F.avg(LABEL)).first()[0])
            baseline = {"prediction": mean, **{
                s: evaluate(scored.filter(F.col(SPLIT) == s).withColumn(PREDICTION, F.lit(mean)),
                            task_type, mspec.metrics)
                for s in ("validation", "test") if rows.get(s)}}
        breakdown = _breakdown(scored, dataset, task.key, mspec.breakdown) if mspec.breakdown else []

        # The version this one replaces, scored on the same test split.
        previous = None
        prev_version = latest_version(root)
        if prev_version is not None and rows.get("test"):
            prev_card = read_card(root, prev_version)
            previous = {"version": prev_version, "dataset": prev_card.get("dataset", {})}
            try:
                prev_model = PipelineModel.load(os.path.join(root, f"v{prev_version}", PIPELINE_DIR))
                previous["test"] = evaluate(prev_model.transform(dataset.filter(F.col(SPLIT) == "test")),
                                            task_type, mspec.metrics)
            except Exception as exc:  # e.g. the old pipeline reads a column this dataset no longer has
                previous["error"] = f"{type(exc).__name__}: {str(exc).splitlines()[0]}"
        eval_sec = time.perf_counter() - t_eval

        manifest = feature_manifest(spec, model.transform(dataset.limit(0)).schema[FEATURES])
        new = _next_version(root)
        path = os.path.join(root, f"v{new}")
        model.write().overwrite().save(os.path.join(path, PIPELINE_DIR))
        with open(os.path.join(path, MANIFEST), "w", encoding="utf-8") as fh:
            fh.write(manifest.to_json())

        card = {
            "task": task.name,
            "version": new,
            "run_id": run.run_id,
            "trained_at": run.started_at.isoformat(),
            "task_type": task_type,
            "model": dataclasses.asdict(mspec),
            "model_fingerprint": run.config_fingerprint,
            "dataset": {"table": run.source_table, "version": version, **lineage},
            "rows": rows,
            "vector_size": manifest.size,
            "missing_indicators": indicators,
            "stages": [type(s).__name__ for s in model.stages],
            "metrics": metrics,
            "baseline": baseline,
            "breakdown": {"column": mspec.breakdown, "rows": breakdown} if mspec.breakdown else None,
            "previous": previous,
            "weights": model_weights(model.stages[-1], manifest),
            "fit_sec": round(fit_sec, 1),
            "evaluate_sec": round(eval_sec, 1),
            "pipeline_path": os.path.relpath(os.path.join(path, PIPELINE_DIR),
                                             platform.lakehouse_root).replace(os.sep, "/"),
        }
        # The card is written last: a version without one did not finish and is never loaded.
        with open(os.path.join(path, MODEL_CARD), "w", encoding="utf-8") as fh:
            json.dump(card, fh, indent=1, default=str)
        with open(os.path.join(root, LATEST), "w", encoding="utf-8") as fh:
            json.dump({"version": new}, fh)

        run.output_version = new
        run.details = {
            "model_version": new,
            "gold_version": lineage.get("source_version"),
            "metrics": {s: metrics[s] for s in ("validation", "test") if s in metrics},
            "baseline_test": baseline.get("test"),
            "previous_test": previous.get("test") if previous else None,
            "fit_sec": card["fit_sec"],
        }
        run.status = "success"
    except Exception as exc:
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}"
    finally:
        if scored is not None:
            scored.unpersist()
        run.finished_at = utc_now()
        run.duration_sec = f"{time.perf_counter() - t0:.1f}"
        if platform.monitoring_enabled:
            write_ml_metadata(spark, run, platform.meta_path("ml_metadata"))
    return run, card


# report
_METRIC_NAME = {"rmse": "RMSE", "mse": "MSE", "mae": "MAE", "r2": "R²", "var": "explained variance"}


def _num(value: Any, digits: int = 3) -> str:
    return "-" if value is None else f"{value:,.{digits}f}"


def render_model_report(task: MLTaskConfig, run: MLRun, card: dict[str, Any], history: list[dict[str, Any]]) -> str:
    mspec = task.model
    metrics, baseline = card["metrics"], card["baseline"]
    names = mspec.metrics
    ds = card["dataset"]
    split = ds.get("split", {})
    params = ", ".join(f"{k}={v}" for k, v in mspec.params.items()) or "defaults"
    lines = [
        f"# Model: {task.name}",
        "",
        f"{task.description}",
        "",
        f"- model: `{mspec.type}` ({params}), version {card['version']} of `{run.output_table}`",
        f"- lineage: `{ds.get('source_table')}` v{ds.get('source_version')} -> `{ds['table']}` "
        f"v{ds['version']} -> `{run.output_table}/v{card['version']}`",
        f"- pipeline: {len(card['stages'])} stages ({len(card['stages']) - 1} from the feature pipeline, "
        f"then `{card['stages'][-1]}`), {card['vector_size']} feature slots, saved at `{card['pipeline_path']}`",
        f"- configuration fingerprint: `{card['model_fingerprint']}`",
        f"- fit on train {card['fit_sec']} s, scoring and evaluation {card['evaluate_sec']} s, "
        f"run {run.duration_sec} s (run `{run.run_id}`)",
        "",
        "## Evaluation",
        "",
    ]
    if "test_start" in split:
        lines += [f"Train holds the trips picked up before {split['validation_start']}, validation those from "
                  f"{split['validation_start']} and test those from {split['test_start']} on.", ""]
    lines += [
        "| split | rows | " + " | ".join(_METRIC_NAME.get(m, m) for m in names) + " |",
        "|---|---:|" + "---:|" * len(names),
    ]
    for s in SPLITS:
        if s in metrics:
            lines.append(f"| {s} | {card['rows'][s]:,} | " + " | ".join(_num(metrics[s][m], 4 if m == 'r2' else 3)
                                                                     for m in names) + " |")
    for s in ("validation", "test"):
        if s in baseline:
            lines.append(f"| {s}, mean baseline | {card['rows'][s]:,} | "
                         + " | ".join(_num(baseline[s][m], 4 if m == 'r2' else 3) for m in names) + " |")
    if baseline:
        lines += ["", f"The mean baseline predicts the training mean, {baseline['prediction']:.2f} min, for every trip."]

    prev = card.get("previous")
    if prev:
        lines += ["", "## Against the previous version", ""]
        pds = prev.get("dataset", {})
        if "test" in prev:
            lines += [
                f"Version {prev['version']}, trained on `{pds.get('table')}` v{pds.get('version')} "
                f"(gold v{pds.get('source_version')}), scored on this version's test split:",
                "",
                "| version | " + " | ".join(_METRIC_NAME.get(m, m) for m in names) + " |",
                "|---|" + "---:|" * len(names),
                f"| v{prev['version']} (previous) | " + " | ".join(_num(prev['test'][m], 4 if m == 'r2' else 3) for m in names) + " |",
                f"| v{card['version']} (this run) | " + " | ".join(_num(metrics['test'][m], 4 if m == 'r2' else 3) for m in names) + " |",
            ]
        else:
            lines.append(f"Version {prev['version']} could not score this test split: {prev.get('error')}")

    bd = card.get("breakdown")
    if bd and bd["rows"]:
        lines += [
            "",
            f"## Test error by `{bd['column']}`",
            "",
            f"| {bd['column']} | trips | mean duration | RMSE | MAE |",
            "|---|---:|---:|---:|---:|",
        ]
        for r in bd["rows"]:
            lines.append(f"| {r[bd['column']]} | {r['rows']:,} | {_num(r['label_mean'], 2)} | "
                         f"{_num(r['rmse'])} | {_num(r['mae'])} |")

    w = card.get("weights")
    if w and "single" in w:
        lines += ["", f"## {w['kind'].capitalize()}s by feature", ""]
        if w["kind"] == "coefficient":
            lines += [
                f"Intercept {_num(w['intercept'], 2)} min; {w['nonzero']} of {card['vector_size']} coefficients are "
                "non-zero. A numeric feature is standardised, so its coefficient is minutes per standard",
                "deviation (of `log1p` of it, where configured). A binary feature's is minutes when it is 1.",
                "",
            ]
        lines += [f"| feature | source | {w['kind']} |", "|---|---|---:|"]
        for f in w["single"]:
            lines.append(f"| `{f['feature']}` | {f['source']} | {f['weight']:+.3f} |")
        if w["grouped"]:
            lines += [
                "",
                "A one-hot feature's coefficients are minutes added when that category is present. The categories",
                "of one feature share their level with the intercept, so only their spread within the feature means",
                "something; the table orders the features by it.",
                "",
                "| feature | source | slots | lowest | highest |",
                "|---|---|---:|---|---|",
            ]
            for f in w["grouped"]:
                lines.append(f"| `{f['feature']}` | {f['source']} | {f['slots']} | "
                             f"{f['low'][1]:+.2f} ({f['low'][0].lstrip('= ')}) | "
                             f"{f['high'][1]:+.2f} ({f['high'][0].lstrip('= ')}) |")

    head = names[0]
    lines += [
        "",
        "## Versions",
        "",
        f"Every run saves a new version; the latest is the one `load_model` returns. {_METRIC_NAME.get(head, head)} "
        "of each version is on its own test split, so versions trained on different data are",
        "compared in the section above instead.",
        "",
        f"| version | trained (UTC) | gold version | dataset version | train rows | test from | "
        f"validation {_METRIC_NAME.get(head, head)} | test {_METRIC_NAME.get(head, head)} | fingerprint |",
        "|---:|---|---:|---:|---:|---|---:|---:|---|",
    ]
    for c in history:
        cds = c.get("dataset", {})
        m = c.get("metrics", {})
        lines.append(
            f"| v{c['version']} | {c['trained_at'][:16].replace('T', ' ')} | {cds.get('source_version')} | "
            f"{cds.get('version')} | {c['rows'].get('train', 0):,} | {cds.get('split', {}).get('test_start', '-')} | "
            f"{_num(m.get('validation', {}).get(head))} | {_num(m.get('test', {}).get(head))} | "
            f"`{c['model_fingerprint']}` |"
        )
    lines.append("")
    return "\n".join(lines)
