"""Week 4 Task 1: the training dataset, built from the gold table.
The dataset records the gold version it was read from and the fingerprint of the task
configuration, as table properties and in _meta/ml_metadata.
"""

from __future__ import annotations

import datetime as dt
import json
import time
import traceback
import uuid
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import PlatformConfig
from ..writer import set_table_properties, table_version
from .config import SPLITS, MLTaskConfig, SplitSpec
from .metadata import PROP, MLRun, utc_now, write_ml_metadata

LABEL = "label"
SPLIT = "split"


def read_source(
    spark: SparkSession, platform: PlatformConfig, task: MLTaskConfig, version: int | None = None
) -> tuple[DataFrame, int]:
    path = platform.table_path(task.source_layer, task.source_table)
    version = table_version(spark, path) if version is None else version
    return spark.read.format("delta").option("versionAsOf", version).load(path), version


def _filters(task: MLTaskConfig) -> dict[str, str]:
    return {**task.dataset.filters, "label_present": f"({task.dataset.label}) IS NOT NULL"}


def _passes(expr: str) -> Column:
    return F.coalesce(F.expr(f"({expr})"), F.lit(False))


def filter_counts(df: DataFrame, filters: dict[str, str]) -> dict[str, int]:
    aggs = [F.count(F.lit(1)).alias("_rows_in")]
    aggs += [F.sum((~_passes(e)).cast("long")).alias(name) for name, e in filters.items()]
    everything = F.lit(True)
    for e in filters.values():
        everything = everything & _passes(e)
    aggs.append(F.sum(everything.cast("long")).alias("_rows_kept"))
    row = df.agg(*aggs).first().asDict()
    return {k: int(v or 0) for k, v in row.items()}


def split_boundaries(df: DataFrame, spec: SplitSpec) -> dict[str, str]:
    if spec.strategy != "chronological":
        return {}
    if spec.validation_start and spec.test_start:
        return {"validation_start": spec.validation_start, "test_start": spec.test_start, "pinned": "true"}
    newest = df.agg(F.max(spec.column)).first()[0]
    if newest is None:
        raise ValueError("no rows left after the filters; nothing to split")
    newest = newest if isinstance(newest, dt.date) else newest.date()
    test_start = newest - dt.timedelta(days=spec.test_days - 1)
    validation_start = test_start - dt.timedelta(days=spec.validation_days)
    return {
        "validation_start": validation_start.isoformat(),
        "test_start": test_start.isoformat(),
        "newest": newest.isoformat(),
    }


def split_column(spec: SplitSpec, key: str, boundaries: dict[str, str]) -> Column:
    if spec.strategy == "chronological":
        col = F.col(spec.column)
        return (
            F.when(col < F.lit(boundaries["validation_start"]).cast("date"), "train")
            .when(col < F.lit(boundaries["test_start"]).cast("date"), "validation")
            .otherwise("test")
        )
    bucket = F.pmod(F.xxhash64(F.col(key), F.lit(spec.seed)), F.lit(1_000_000)) / 1_000_000.0
    train_end = spec.fractions["train"]
    validation_end = train_end + spec.fractions["validation"]
    return F.when(bucket < train_end, "train").when(bucket < validation_end, "validation").otherwise("test")


def build_dataset(
    source: DataFrame, task: MLTaskConfig, boundaries: dict[str, str] | None = None
) -> tuple[DataFrame, dict[str, str]]:
    ds = task.dataset
    wanted = [task.key, *([ds.split.column] if ds.split.column else []), *ds.columns]
    missing = [c for c in dict.fromkeys(wanted) if c not in source.columns]
    if missing:
        raise ValueError(
            f"[{task.name}] column(s) not in {task.source_table}: {', '.join(missing)}. "
            "A new column reaches gold through conf/integration.yaml first."
        )

    kept = source
    for expr in _filters(task).values():
        kept = kept.where(_passes(expr))

    if boundaries is None:
        boundaries = split_boundaries(kept, ds.split)
    columns = list(dict.fromkeys(c for c in wanted if c != task.key))
    dataset = kept.select(
        task.key,
        F.expr(ds.label).cast("double").alias(LABEL),
        *columns,
        split_column(ds.split, task.key, boundaries).alias(SPLIT),
    )
    return dataset, boundaries


def profile_dataset(df: DataFrame, task: MLTaskConfig) -> dict[str, dict[str, Any]]:
    split_col = task.dataset.split.column
    aggs = [
        F.count(F.lit(1)).alias("rows"),
        F.avg(LABEL).alias("label_mean"),
        F.stddev(LABEL).alias("label_std"),
        F.percentile_approx(LABEL, [0.05, 0.5, 0.95], 100_000).alias("label_pct"),
    ]
    if split_col:
        aggs += [F.min(split_col).alias("first"), F.max(split_col).alias("last")]
    aggs += [F.sum(F.col(c).isNull().cast("long")).alias(f"null__{c}") for c in task.dataset.columns]
    out = {}
    for row in df.groupBy(SPLIT).agg(*aggs).collect():
        r = row.asDict()
        out[r.pop(SPLIT)] = {
            **{k: v for k, v in r.items() if not k.startswith("null__")},
            "nulls": {k[len("null__"):]: int(v or 0) for k, v in r.items() if k.startswith("null__")},
        }
    return out


def run_dataset(
    spark: SparkSession, platform: PlatformConfig, task: MLTaskConfig, source_version: int | None = None
) -> tuple[MLRun, dict[str, Any]]:
    run = MLRun(run_id=str(uuid.uuid4()), task=task.name, stage="dataset", started_at=utc_now(),
                source_table=f"{task.source_layer}/{task.source_table}",
                output_table=f"ml/{task.dataset.output}",
                config_fingerprint=task.dataset_fingerprint)
    out_path = platform.table_path("ml", task.dataset.output)
    t0 = time.perf_counter()
    profile: dict[str, Any] = {}
    try:
        source, version = read_source(spark, platform, task, source_version)
        run.source_version = version

        counts = filter_counts(source, _filters(task))
        run.rows_in = counts.pop("_rows_in")
        kept = counts.pop("_rows_kept")

        dataset, boundaries = build_dataset(source, task)
        (
            dataset.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy(SPLIT)
            .save(out_path)
        )

        written = spark.read.format("delta").load(out_path)
        profile = profile_dataset(written, task)
        run.split_rows({s: profile.get(s, {}).get("rows", 0) for s in SPLITS})
        if sum(p["rows"] for p in profile.values()) != kept:
            raise RuntimeError(f"wrote {sum(p['rows'] for p in profile.values())} rows, expected {kept}")

        run.details = {"split": {"strategy": task.dataset.split.strategy, **boundaries},
                       "filters_failed": counts, "rows_kept": kept}
        set_table_properties(spark, out_path, {
            PROP + "task": task.name,
            PROP + "label": task.dataset.label,
            PROP + "source_table": run.source_table,
            PROP + "source_version": str(version),
            PROP + "config_fingerprint": run.config_fingerprint,
            PROP + "split": json.dumps(run.details["split"], sort_keys=True),
        })
        run.output_version = table_version(spark, out_path)
        run.status = "success"
    except Exception as exc:
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=3)}"
    finally:
        run.finished_at = utc_now()
        run.duration_sec = f"{time.perf_counter() - t0:.1f}"
        if platform.monitoring_enabled:
            write_ml_metadata(spark, run, platform.meta_path("ml_metadata"))
    return run, profile


# report
def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def render_dataset_report(task: MLTaskConfig, run: MLRun, profile: dict[str, Any]) -> str:
    ds = task.dataset
    split = run.details.get("split", {})
    total = sum(p["rows"] for p in profile.values()) or 1
    lines = [
        f"# Training dataset: {task.name}",
        "",
        f"{task.description}",
        "",
        f"- source: `{run.source_table}` version {run.source_version}",
        f"- output: `{run.output_table}` version {run.output_version}",
        f"- configuration fingerprint: `{run.config_fingerprint}`",
        f"- label: `{ds.label}`",
        f"- built {run.started_at:%Y-%m-%d %H:%M} UTC in {run.duration_sec} s (run `{run.run_id}`)",
        "",
        "## Filters",
        "",
        "A row can fail several filters, so the failures do not add up to the rows removed.",
        "",
        "| filter | condition | rows failing | % of source |",
        "|---|---|---:|---:|",
    ]
    filters = _filters(task)
    for name, failed in run.details.get("filters_failed", {}).items():
        lines.append(f"| {name} | `{filters[name]}` | {failed:,} | {100 * failed / run.rows_in:.2f}% |")
    kept = run.details.get("rows_kept", 0)
    lines += [
        f"| **kept** | | **{kept:,}** | **{100 * kept / run.rows_in:.2f}%** of {run.rows_in:,} |",
        "",
        "## Splits",
        "",
        f"Strategy `{split.get('strategy')}`"
        + (f", validation from {split['validation_start']}, test from {split['test_start']}"
           + (" (pinned)" if split.get("pinned") else f" (counted back from {split.get('newest')})")
           if "test_start" in split else "") + ".",
        "",
        "| split | rows | share | first | last | label mean | std | p5 | median | p95 |",
        "|---|---:|---:|---|---|---:|---:|---:|---:|---:|",
    ]
    for s in SPLITS:
        p = profile.get(s)
        if not p:
            continue
        pct = p["label_pct"] or [None, None, None]
        lines.append(
            f"| {s} | {p['rows']:,} | {100 * p['rows'] / total:.1f}% | {p.get('first', '-')} | "
            f"{p.get('last', '-')} | {_fmt(p['label_mean'])} | {_fmt(p['label_std'])} | "
            f"{_fmt(pct[0])} | {_fmt(pct[1])} | {_fmt(pct[2])} |"
        )
    lines += [
        "",
        "## Columns",
        "",
        "| column | from dataset | nulls (train) | nulls (validation) | nulls (test) |",
        "|---|---|---:|---:|---:|",
    ]
    for column, origin in ds.columns.items():
        cells = []
        for s in SPLITS:
            p = profile.get(s)
            n = p["nulls"][column] if p else 0
            cells.append(f"{n:,} ({100 * n / p['rows']:.2f}%)" if p and n else "0")
        lines.append(f"| `{column}` | {origin} | " + " | ".join(cells) + " |")
    lines.append("")
    return "\n".join(lines)
