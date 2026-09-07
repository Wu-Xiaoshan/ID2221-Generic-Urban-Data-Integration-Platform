"""Generic data-quality checks.

Every rule is declared in YAML. This module knows rule *types*, never datasets.

A rule either rejects a row or flags it. Rejected rows are removed and written to
_meta/quarantine together with the name of the rule that removed them, so a rejection
can be explained later rather than only counted. Flagged rows stay in the table with a
boolean ``dq_<rule>`` column, which is the right treatment for anomalies that are
suspicious but not invalid.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .config import DatasetConfig, QualityRule

_FAILED = "_dq_failed_rules"


def _passes(rule: QualityRule, df: DataFrame) -> Column:
    """Return a boolean column that is true when the row satisfies the rule.

    Null handling is uniform: a null value passes every rule except ``not_null``.
    Otherwise a single missing value would trip several rules at once and the
    quarantine reason would not say which problem actually mattered.
    """
    params = rule.params

    if rule.type == "not_null":
        columns = params.get("columns") or [params["column"]]
        condition = F.lit(True)
        for column in columns:
            condition = condition & F.col(column).isNotNull()
        return condition

    if rule.type == "range":
        column = F.col(params["column"])
        condition = F.lit(True)
        if "min" in params:
            condition = condition & (
                column > params["min"] if params.get("exclusive_min") else column >= params["min"]
            )
        if "max" in params:
            condition = condition & (
                column < params["max"] if params.get("exclusive_max") else column <= params["max"]
            )
        return F.col(params["column"]).isNull() | condition

    if rule.type == "timestamp_window":
        column = F.col(params["column"])
        condition = (column >= F.lit(params["min"]).cast("timestamp")) & (
            column < F.lit(params["max"]).cast("timestamp")
        )
        return column.isNull() | condition

    if rule.type == "allowed_values":
        column = F.col(params["column"])
        return column.isNull() | column.isin(params["values"])

    if rule.type == "expression":
        return F.expr(params["expression"])

    if rule.type == "unique":
        raise ValueError("'unique' is handled by _apply_unique, not as a row predicate")

    raise ValueError(f"unknown rule type '{rule.type}'")


def _apply_unique(df: DataFrame, rule: QualityRule) -> DataFrame:
    """Mark every row after the first within a key group as failing.

    Kept separate from the row-wise rules because uniqueness is a property of a group,
    not of a row. The surviving row is the first in the frame's existing order, which is
    deterministic for this platform: the taxi surrogate key is a content hash, so
    duplicate rows are identical anyway.
    """
    columns = rule.params.get("columns") or [rule.params["column"]]
    window = Window.partitionBy(*[F.col(c) for c in columns]).orderBy(F.lit(1))
    occurrence = F.row_number().over(window)
    return df.withColumn(
        _FAILED,
        F.when(occurrence > 1, F.array_union(F.col(_FAILED), F.array(F.lit(rule.name)))).otherwise(
            F.col(_FAILED)
        ),
    )


def apply_rules(df: DataFrame, cfg: DatasetConfig) -> tuple[DataFrame, DataFrame]:
    """Split the frame into (clean rows, rejected rows).

    Rejected rows carry ``_dq_failed_rules``, an array naming every rule they failed.
    """
    df = df.withColumn(_FAILED, F.array().cast("array<string>"))

    for rule in cfg.rules:
        if rule.on_fail != "reject":
            continue
        if rule.type == "unique":
            df = _apply_unique(df, rule)
        else:
            df = df.withColumn(
                _FAILED,
                F.when(
                    ~_passes(rule, df), F.array_union(F.col(_FAILED), F.array(F.lit(rule.name)))
                ).otherwise(F.col(_FAILED)),
            )

    clean = df.filter(F.size(_FAILED) == 0).drop(_FAILED)
    rejected = df.filter(F.size(_FAILED) > 0)

    # Flags are added only to rows that survived, since a rejected row is not in the
    # table to be inspected.
    for rule in cfg.rules:
        if rule.on_fail != "flag":
            continue
        if rule.type == "unique":
            raise ValueError(f"rule '{rule.name}': 'unique' cannot be flagged, only rejected")
        clean = clean.withColumn(f"dq_{rule.name}", ~_passes(rule, clean))

    return clean, rejected


def to_quarantine(rejected: DataFrame, cfg: DatasetConfig, run_id: str) -> DataFrame:
    """Reshape rejected rows into the shared quarantine schema.

    The whole source row is serialised to JSON so that one quarantine table can hold
    rejects from every dataset. A per-dataset quarantine schema would multiply tables
    by datasets for no benefit, since the table is read by a human debugging a run.
    """
    payload = [c for c in rejected.columns if c != _FAILED]
    return rejected.select(
        F.lit(cfg.name).alias("dataset_name"),
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("rejected_at"),
        F.concat_ws(",", F.col(_FAILED)).alias("failed_rules"),
        F.to_json(F.struct(*payload)).alias("row_json"),
    )
