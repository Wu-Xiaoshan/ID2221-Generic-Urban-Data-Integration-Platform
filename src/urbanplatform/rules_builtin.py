"""The rule types shipped with the platform.

Null handling is uniform: a null value passes every rule except ``not_null`` and
``completeness``. Otherwise one missing value would trip several rules at once and the
quarantine reason would not say which problem actually mattered.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .config import QualityRule
from .rules import RuleContext, frame_rule, mark_failed, predicate


@predicate("not_null")
def _not_null(rule: QualityRule, df: DataFrame) -> Column:
    columns = rule.params.get("columns") or [rule.params["column"]]
    condition = F.lit(True)
    for column in columns:
        condition = condition & F.col(column).isNotNull()
    return condition


@predicate("range")
def _range(rule: QualityRule, df: DataFrame) -> Column:
    params = rule.params
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
    return column.isNull() | condition


@predicate("timestamp_window")
def _timestamp_window(rule: QualityRule, df: DataFrame) -> Column:
    params = rule.params
    column = F.col(params["column"])
    window = (column >= F.lit(params["min"]).cast("timestamp")) & (
        column < F.lit(params["max"]).cast("timestamp")
    )
    return column.isNull() | window


@predicate("allowed_values")
def _allowed_values(rule: QualityRule, df: DataFrame) -> Column:
    column = F.col(rule.params["column"])
    return column.isNull() | column.isin(rule.params["values"])


@predicate("expression")
def _expression(rule: QualityRule, df: DataFrame) -> Column:
    return F.expr(rule.params["expression"])


@predicate("completeness")
def _completeness(rule: QualityRule, df: DataFrame) -> Column:
    """A row is incomplete when too few of the listed columns carry a value.

    ``not_null`` demands every column; this one demands a quorum, which is what an
    optional-but-not-all-optional block of attributes needs. Columns absent from the
    frame count as missing rather than raising, so a source that stopped sending a
    field is reported as incomplete data instead of crashing the run.
    """
    columns = rule.params.get("columns") or [rule.params["column"]]
    present = [
        F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0)) if c in df.columns else F.lit(0)
        for c in columns
    ]
    count = present[0]
    for column in present[1:]:
        count = count + column

    if "min_present" in rule.params:
        required = int(rule.params["min_present"])
    elif "min_ratio" in rule.params:
        import math

        required = math.ceil(float(rule.params["min_ratio"]) * len(columns))
    else:
        required = len(columns)
    return count >= F.lit(required)


@frame_rule("unique")
def _unique(rule: QualityRule, df: DataFrame, ctx: RuleContext) -> DataFrame:
    columns = rule.params.get("columns") or [rule.params["column"]]
    window = Window.partitionBy(*[F.col(c) for c in columns]).orderBy(F.lit(1))
    return mark_failed(df, rule, F.row_number().over(window) > 1)


@frame_rule("foreign_key")
def _foreign_key(rule: QualityRule, df: DataFrame, ctx: RuleContext) -> DataFrame:
    """
    If the referenced table does not exist yet the rule is skipped and the skip is
    reported, rather than failing the run. On a first load the datasets are ingested in
    one pass and the dimension may not be written at the moment the fact is checked;
    the next run then validates it for real.
    """
    reference = rule.params["references"]
    keys = ctx.table(reference.get("layer", "silver"), reference["table"])
    if keys is None:
        ctx.skip(rule, f"{reference['table']} not built yet")
        return df

    column = rule.params["column"]
    probe = "_fk_" + rule.name
    keys = keys.select(F.col(reference["column"]).alias(probe)).distinct()
    joined = df.join(F.broadcast(keys), df[column] == F.col(probe), "left")
    missing = F.col(column).isNotNull() & F.col(probe).isNull()
    return mark_failed(joined, rule, missing).drop(probe)
