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
    optional-but-not-all-optional block of attributes needs.

    A column that is not in the frame counts as missing, which is the right reading for
    a flag: a source that stopped sending a field is reported as incomplete data rather
    than crashing the run. A rejecting rule cannot be treated that way. Every record
    would fail it and the whole dataset would be deleted, so the rule stops the dataset
    instead and the run record says which columns were not there.
    """
    columns = rule.params.get("columns") or [rule.params["column"]]
    absent = [c for c in columns if c not in df.columns]
    if absent and rule.on_fail == "reject":
        raise ValueError(
            f"rule '{rule.name}': column(s) {', '.join(absent)} are not in the frame. "
            "Rejecting on that would remove every record."
        )

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
    The rule is skipped, and the skip reported, whenever there is nothing to match
    against: no table yet, or a table with no usable keys. On a first load the datasets
    are ingested in one pass and the dimension may not be written at the moment the
    fact is checked; the next run then validates it for real.

    Skipping matters because the alternative is silent. An empty dimension matches no
    record, so every record would be rejected and reported as a genuine violation. A
    dimension that failed to load would take the fact table with it.
    """
    reference = rule.params["references"]
    keys = ctx.table(reference.get("layer", "silver"), reference["table"])
    if keys is None:
        ctx.skip(rule, f"{reference['table']} not built yet")
        return df

    column = rule.params["column"]
    probe = "_fk_" + rule.name
    # Nulls are dropped because equality never matches one anyway. That also makes the
    # emptiness test below mean "no keys to match against" rather than "no rows".
    keys = (
        keys.select(F.col(reference["column"]).alias(probe))
        .where(F.col(probe).isNotNull())
        .distinct()
    )
    if keys.isEmpty():
        ctx.skip(rule, f"{reference['table']}.{reference['column']} holds no values")
        return df

    joined = df.join(F.broadcast(keys), df[column] == F.col(probe), "left")
    missing = F.col(column).isNotNull() & F.col(probe).isNull()
    return mark_failed(joined, rule, missing).drop(probe)
