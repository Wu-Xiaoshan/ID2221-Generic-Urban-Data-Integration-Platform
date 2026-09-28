# Fit the feature pipeline on a dataset's training split, apply it to every split, and record it.

from __future__ import annotations

import json
import os
import time
import traceback
import uuid
from typing import Any

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import PlatformConfig
from ..writer import set_table_properties, table_version
from .config import SPLITS, FeatureSpec, MLTaskConfig
from .dataset import SPLIT
from .features import (
    _CAT,
    _NUM,
    FEATURES,
    MISSING_CATEGORY,
    FeatureManifest,
    feature_manifest,
    fit_feature_pipeline,
    generated_columns,
    learnt_state,
    prepare,
)
from .metadata import PROP, MLRun, utc_now, write_ml_metadata

MANIFEST = "feature_manifest.json"


def split_profile(task: MLTaskConfig, dataset: DataFrame, labels: dict[str, list[str]]) -> dict[str, dict[str, int]]:
    spec = task.features
    aggs = [F.count(F.lit(1)).alias("rows")]
    for f in spec.of_kind("numeric"):
        c = F.col(_NUM + f.name)
        aggs.append(F.sum((c.isNull() | F.isnan(c)).cast("long")).alias(f"null__{f.name}"))
    for f in spec.of_kind("categorical"):
        c = F.col(_CAT + f.name)
        aggs.append(F.sum((c == MISSING_CATEGORY).cast("long")).alias(f"missing__{f.name}"))
        seen = [str(v) for v in labels.get(f.name, [])]
        aggs.append(F.sum((~c.isin(*seen)).cast("long")).alias(f"unseen__{f.name}"))
    rows = prepare(spec, dataset).groupBy(SPLIT).agg(*aggs).collect()
    return {r[SPLIT]: {k: int(v or 0) for k, v in r.asDict().items() if k != SPLIT} for r in rows}


def preprocessing_steps(f: FeatureSpec, derived: dict[str, tuple[str, str]], indicators: list[str],
                        imputed: set[str], state: dict[str, Any], spec) -> list[str]:
    steps = []
    if f.name in derived:
        steps.append(f"derived ({derived[f.name][0]})")
    if f.missing_values:
        steps.append(f"{', '.join(str(v) for v in f.missing_values)} -> missing")
    if f.kind == "numeric":
        steps.append("cast to double")
        if f.log1p:
            steps.append("log1p")
        if f.name in indicators:
            steps.append("missing flag")
        if f.name in imputed and f.impute_by:
            groups = state.get("group_impute", {}).get(f.name, {}).get("groups", {})
            steps.append(f"impute {spec.impute} by {f.impute_by} ("
                         + ", ".join(f"{g} {v:.3g}" for g, v in sorted(groups.items())) + ")")
        elif f.name in imputed:
            steps.append(f"impute {spec.impute} ({state.get('impute', {}).get(f.name, 0):.3g})")
        if spec.scaling != "none":
            steps.append(f"scale ({spec.scaling})")
    elif f.kind == "categorical":
        steps.append(f"null -> {MISSING_CATEGORY}")
        steps.append(f"index ({len(state.get('labels', {}).get(f.name, []))} values + unseen)")
        if spec.encoding == "onehot":
            steps.append("one-hot")
    else:
        steps.append("to 0/1, null -> 0")
    return steps


def run_features(
    spark: SparkSession, platform: PlatformConfig, task: MLTaskConfig, dataset_version: int | None = None
) -> tuple[MLRun, dict[str, Any]]:
    spec = task.features
    dataset_path = platform.table_path("ml", task.dataset.output)
    out_path = platform.table_path("ml", spec.output)
    model_path = platform.table_path("ml", spec.pipeline)
    run = MLRun(run_id=str(uuid.uuid4()), task=task.name, stage="features", started_at=utc_now(),
                source_table=f"ml/{task.dataset.output}", output_table=f"ml/{spec.output}",
                config_fingerprint=task.features_fingerprint)
    t0 = time.perf_counter()
    info: dict[str, Any] = {}
    try:
        version = table_version(spark, dataset_path) if dataset_version is None else dataset_version
        run.source_version = version
        dataset = spark.read.format("delta").option("versionAsOf", version).load(dataset_path)

        t_fit = time.perf_counter()
        model, indicators = fit_feature_pipeline(spec, dataset.filter(F.col(SPLIT) == "train"))
        fit_sec = time.perf_counter() - t_fit
        model.write().overwrite().save(model_path)

        t_write = time.perf_counter()
        features = model.transform(dataset)
        (
            features.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(SPLIT)
            .save(out_path)
        )
        write_sec = time.perf_counter() - t_write

        written = spark.read.format("delta").load(out_path)
        manifest = feature_manifest(spec, written.schema[FEATURES])
        with open(os.path.join(model_path, MANIFEST), "w", encoding="utf-8") as fh:
            fh.write(manifest.to_json())

        state = learnt_state(spec, model)
        profile = split_profile(task, dataset, state.get("labels", {}))
        run.rows_in = sum(p["rows"] for p in profile.values())
        run.split_rows({r[SPLIT]: r["count"] for r in written.groupBy(SPLIT).count().collect()})
        if run.rows_train + run.rows_validation + run.rows_test != run.rows_in:
            raise RuntimeError("the feature table does not have one row per dataset row")

        run.details = {
            "vector_size": manifest.size,
            "slots_by_source": manifest.slots_by("source"),
            "missing_indicators": indicators,
            "fit_sec": round(fit_sec, 1),
            "transform_write_sec": round(write_sec, 1),
            "pipeline_path": os.path.relpath(model_path, platform.lakehouse_root).replace(os.sep, "/"),
            "stages": [type(s).__name__ for s in model.stages],
        }
        set_table_properties(spark, out_path, {
            PROP + "task": task.name,
            PROP + "dataset_version": str(version),
            PROP + "config_fingerprint": run.config_fingerprint,
            PROP + "vector_size": str(manifest.size),
            PROP + "pipeline": run.details["pipeline_path"],
        })
        run.output_version = table_version(spark, out_path)
        run.status = "success"
        info = {"manifest": manifest, "state": state, "profile": profile, "indicators": indicators,
                "stages": model.stages}
    except Exception as exc:
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}"
    finally:
        run.finished_at = utc_now()
        run.duration_sec = f"{time.perf_counter() - t0:.1f}"
        if platform.monitoring_enabled:
            write_ml_metadata(spark, run, platform.meta_path("ml_metadata"))
    return run, info


def load_feature_pipeline(platform: PlatformConfig, task: MLTaskConfig) -> tuple[PipelineModel, FeatureManifest]:
    """The fitted pipeline and its manifest, as saved by the last features run."""
    from .features import Slot

    path = platform.table_path("ml", task.features.pipeline)
    with open(os.path.join(path, MANIFEST), encoding="utf-8") as fh:
        raw = json.load(fh)
    manifest = FeatureManifest(size=raw["size"], slots=[Slot(*s) for s in raw["slots"]])
    return PipelineModel.load(path), manifest


# report
_STAGE_ROLE = {
    "SQLTransformer": "SQL",
    "Imputer": "fill numeric nulls",
    "ImputerModel": "fill numeric nulls",
    "GroupImputerModel": "fill numeric nulls per group",
    "VectorAssembler": "assemble",
    "StandardScalerModel": "standardise numerics",
    "MinMaxScalerModel": "rescale numerics to [0, 1]",
    "StringIndexerModel": "index categories",
    "OneHotEncoderModel": "one-hot encode",
    "SelectColumns": "drop intermediate and raw columns",
}


def _describe_stage(stage) -> str:
    name = type(stage).__name__
    if name == "SQLTransformer":
        sql = stage.getStatement()
        return sql if len(sql) < 110 else sql[:107] + "..."
    if name == "VectorAssembler":
        return f"{len(stage.getInputCols())} inputs -> `{stage.getOutputCol()}`"
    if name == "GroupImputerModel":
        cols = ", ".join(f"`{c}`" for c in stage.getOrDefault(stage.inputCols))
        return f"fill {cols} with the training {stage.getOrDefault(stage.strategy)} of each `{stage.getOrDefault(stage.groupCol)}`"
    if name == "SelectColumns":
        return "keeps " + ", ".join(f"`{c}`" for c in stage.getOrDefault(stage.keep))
    return _STAGE_ROLE.get(name, "")


def render_features_report(task: MLTaskConfig, run: MLRun, info: dict[str, Any]) -> str:
    spec = task.features
    manifest: FeatureManifest = info["manifest"]
    state, profile, indicators = info["state"], info["profile"], info["indicators"]
    derived = generated_columns(spec)
    slots = manifest.slots_by("feature")
    by_source = manifest.slots_by("source")
    d = run.details

    lines = [
        f"# Feature pipeline: {task.name}",
        "",
        f"- input: `{run.source_table}` version {run.source_version}",
        f"- output: `{run.output_table}` version {run.output_version}, "
        f"{run.rows_in:,} rows, one `{FEATURES}` vector of {manifest.size} slots each",
        f"- fitted pipeline: `{d['pipeline_path']}` ({len(d['stages'])} stages)",
        f"- configuration fingerprint: `{run.config_fingerprint}`",
        f"- fit on train {d['fit_sec']} s, transform and write {d['transform_write_sec']} s, "
        f"run {run.duration_sec} s (run `{run.run_id}`)",
        "",
        "## Stages",
        "",
        "| # | stage | does |",
        "|---:|---|---|",
    ]
    for i, stage in enumerate(info["stages"], 1):
        lines.append(f"| {i} | {type(stage).__name__} | {_describe_stage(stage).replace('|', '/')} |")

    lines += [
        "",
        "## Where the vector comes from",
        "",
        "| source dataset | features | slots | share of slots |",
        "|---|---:|---:|---:|",
    ]
    features_per_source: dict[str, int] = {}
    for f in spec.features:
        features_per_source[f.source] = features_per_source.get(f.source, 0) + 1
    for source, n in sorted(by_source.items(), key=lambda kv: -kv[1]):
        lines.append(f"| {source} | {features_per_source.get(source, 0)} | {n} | {100 * n / manifest.size:.1f}% |")

    lines += [
        "",
        "## Features",
        "",
        "Nulls are counted after declared missing codes are turned into nulls: they are the values",
        "the imputer fills (numeric) or the `__missing__` category (categorical). Unseen values are",
        "categories that never occur in the training split.",
        "",
        "| feature | kind | source | preprocessing | steps | slots | missing train / val / test | unseen val / test |",
        "|---|---|---|---|---:|---:|---|---|",
    ]

    def share(split: str, key: str) -> str:
        p = profile.get(split)
        if not p or not p["rows"]:
            return "-"
        n = p.get(key, 0)
        return f"{100 * n / p['rows']:.2f}%" if n else "0"

    imputed = {f.name for f in spec.of_kind("numeric")
               if any(p.get(f"null__{f.name}", 0) for p in profile.values())}
    rows = []
    for f in spec.features:
        steps = preprocessing_steps(f, derived, indicators, imputed, state, spec)
        key = f"null__{f.name}" if f.kind == "numeric" else f"missing__{f.name}"
        missing = " / ".join(share(s, key) for s in SPLITS) if f.kind != "binary" else "-"
        unseen = (" / ".join(share(s, f"unseen__{f.name}") for s in ("validation", "test"))
                  if f.kind == "categorical" else "-")
        rows.append((len(steps), f"| `{f.name}` | {f.kind} | {f.source} | {'; '.join(steps)} | {len(steps)} | "
                     f"{slots.get(f.name, 0)} | {missing} | {unseen} |"))
    for _, row in sorted(rows, key=lambda r: -r[0]):
        lines.append(row)

    lines += [
        "",
        "Missing flags were added for: " + (", ".join(f"`{n}`" for n in indicators) or "none") + ".",
        "",
    ]
    return "\n".join(lines)
