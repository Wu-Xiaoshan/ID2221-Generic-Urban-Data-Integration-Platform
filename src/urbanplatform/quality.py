"""The validation engine.

Rules are declared in YAML and implemented in rules_builtin.py. This module only
applies them, splits the frame into clean and rejected rows, and reports what each rule
did. It knows no rule type and no dataset.

A rule either rejects a row or flags it. Rejected rows are removed and written to
_meta/quarantine together with the name of the rule that removed them, so a rejection
can be explained later rather than only counted. Flagged rows stay in the table with a
boolean ``dq_<rule>`` column, which is the right treatment for anomalies that are
suspicious but not invalid.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import rules as ruleset
from . import rules_builtin  # noqa: F401  (importing registers the built-in rule types)
from .config import DatasetConfig, QualityRule
from .rules import FAILED, RuleContext


@dataclass
class RuleOutcome:
    """What one rule did on one run. Written to _meta/validation_metrics."""

    name: str
    rule_type: str
    on_fail: str
    status: str = "applied"
    failed_records: int | None = None
    note: str | None = None


@dataclass
class ValidationResult:
    clean: DataFrame
    rejected: DataFrame
    skipped: dict[str, str] = field(default_factory=dict)
    # A rule which produced no column is reported as uncounted instead of as zero failures.
    flag_columns: dict[str, str] = field(default_factory=dict)


def _apply(rule: QualityRule, df: DataFrame, ctx: RuleContext) -> DataFrame:
    frame_rule = ruleset.frame_for(rule.type)
    if frame_rule is not None:
        return frame_rule(rule, df, ctx)

    check = ruleset.predicate_for(rule.type)
    if check is None:
        raise ValueError(
            f"rule '{rule.name}': unknown type '{rule.type}'. "
            f"known types: {', '.join(ruleset.known_types())}"
        )
    return ruleset.mark_failed(df, rule, ~check(rule, df))


def _flag(
    rule: QualityRule, clean: DataFrame, ctx: RuleContext
) -> tuple[DataFrame, str | None]:
    column = f"dq_{rule.name}"
    frame_rule = ruleset.frame_for(rule.type)
    if frame_rule is not None:
        marked = frame_rule(rule, clean.withColumn(FAILED, F.array().cast("array<string>")), ctx)
        if rule.name in ctx.skipped:
            return marked.drop(FAILED), None
        return marked.withColumn(column, F.array_contains(F.col(FAILED), rule.name)).drop(FAILED), column

    check = ruleset.predicate_for(rule.type)
    if check is None:
        raise ValueError(
            f"rule '{rule.name}': unknown type '{rule.type}'. "
            f"known types: {', '.join(ruleset.known_types())}"
        )
    return clean.withColumn(column, ~check(rule, clean)), column


def apply_rules(df: DataFrame, cfg: DatasetConfig, ctx: RuleContext | None = None) -> ValidationResult:
    """Split the frame into clean and rejected rows.

    Rejected rows carry ``_dq_failed_rules``, an array naming every rule they failed.
    """
    ctx = ctx or RuleContext(cfg=cfg)
    df = df.withColumn(FAILED, F.array().cast("array<string>"))

    for rule in cfg.rules:
        if rule.on_fail == "reject":
            df = _apply(rule, df, ctx)

    clean = df.filter(F.size(FAILED) == 0).drop(FAILED)
    rejected = df.filter(F.size(FAILED) > 0)

    # Flags are added only to rows that survived, since a rejected row is not in the
    # table to be inspected.
    flag_columns: dict[str, str] = {}
    for rule in cfg.rules:
        if rule.on_fail == "flag":
            clean, column = _flag(rule, clean, ctx)
            if column is not None:
                flag_columns[rule.name] = column

    return ValidationResult(
        clean=clean,
        rejected=rejected,
        skipped=dict(ctx.skipped),
        flag_columns=flag_columns,
    )


def rule_outcomes(
    result: ValidationResult, cfg: DatasetConfig, count: bool = True
) -> list[RuleOutcome]:
    """One row per rule: whether it ran, and how many records it caught.

    ``status`` is one of

        applied     the rule ran; failed_records is its count, or null when ``count``
                    is off and the aggregations were not paid for
        skipped     the rule could not run, and note says why
        uncounted   the rule ran but its result could not be measured

    ``uncounted`` exists so that a missing count is never written as a zero. A rejected
    record is counted from the failure array, which names every rule that fired, so a
    reject rule absent from it genuinely caught nothing. A flag rule is counted from
    the column the engine added for it, and if that column is not there the count is
    unknown, which is a different fact from zero.
    """
    outcomes = [
        RuleOutcome(
            name=rule.name,
            rule_type=rule.type,
            on_fail=rule.on_fail,
            status="skipped" if rule.name in result.skipped else "applied",
            note=result.skipped.get(rule.name),
        )
        for rule in cfg.rules
    ]
    if not count:
        return outcomes

    rejected = _rejected_counts(result.rejected)
    flagged = _flag_counts(result.clean, result.flag_columns)
    for outcome in outcomes:
        if outcome.status != "applied":
            continue
        if outcome.on_fail == "flag":
            if outcome.name in flagged:
                outcome.failed_records = flagged[outcome.name]
            else:
                outcome.status = "uncounted"
                outcome.note = (
                    f"no dq_{outcome.name} column in the validated frame; "
                    "the rule ran but its failures were not counted"
                )
        else:
            outcome.failed_records = rejected.get(outcome.name, 0)
    return outcomes


def _rejected_counts(rejected: DataFrame) -> dict[str, int]:
    rows = (
        rejected.select(F.explode(F.col(FAILED)).alias("rule"))
        .groupBy("rule")
        .count()
        .collect()
    )
    return {row["rule"]: row["count"] for row in rows}


def _flag_counts(clean: DataFrame, flag_columns: dict[str, str]) -> dict[str, int]:
    flags = [name for name, column in flag_columns.items() if column in clean.columns]
    if not flags:
        return {}
    row = clean.agg(
        *[F.sum(F.col(flag_columns[name]).cast("long")).alias(name) for name in flags]
    ).first()
    return {name: int(row[name] or 0) for name in flags}


def to_quarantine(rejected: DataFrame, cfg: DatasetConfig, run_id: str) -> DataFrame:
    """Reshape rejected rows into the shared quarantine schema.

    The whole source row is serialised to JSON so that one quarantine table can hold
    rejects from every dataset. A per-dataset quarantine schema would multiply tables
    by datasets for no benefit, since the table is read by a human debugging a run.
    """
    payload = [c for c in rejected.columns if c != FAILED]
    return rejected.select(
        F.lit(cfg.name).alias("dataset_name"),
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("rejected_at"),
        F.concat_ws(",", F.col(FAILED)).alias("failed_rules"),
        F.to_json(F.struct(*payload)).alias("row_json"),
    )
