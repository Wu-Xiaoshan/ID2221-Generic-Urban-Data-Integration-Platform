"""Registry of validation rule types.

A rule type is a function registered under a name. Two kinds exist:

``predicate``  returns a Column that is true when the row *satisfies* the rule. Most
               rules are this shape.
``frame``      returns the frame with failures already marked. Needed when the check
               is not row-local: uniqueness is a property of a group, a foreign key
               needs another table.

Adding a rule type means adding a function in rules_builtin.py, or in any module the
platform imports. The engine in quality.py is never touched.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import DatasetConfig, PlatformConfig, QualityRule

FAILED = "_dq_failed_rules"

Predicate = Callable[[QualityRule, DataFrame], Column]
FrameRule = Callable[[QualityRule, DataFrame, "RuleContext"], DataFrame]

_PREDICATES: dict[str, Predicate] = {}
_FRAME_RULES: dict[str, FrameRule] = {}


def predicate(rule_type: str) -> Callable[[Predicate], Predicate]:
    def register(fn: Predicate) -> Predicate:
        _PREDICATES[rule_type] = fn
        return fn

    return register


def frame_rule(rule_type: str) -> Callable[[FrameRule], FrameRule]:
    def register(fn: FrameRule) -> FrameRule:
        _FRAME_RULES[rule_type] = fn
        return fn

    return register


def predicate_for(rule_type: str) -> Predicate | None:
    return _PREDICATES.get(rule_type)


def frame_for(rule_type: str) -> FrameRule | None:
    return _FRAME_RULES.get(rule_type)


def known_types() -> list[str]:
    return sorted(set(_PREDICATES) | set(_FRAME_RULES))


@dataclass
class RuleContext:
    spark: SparkSession | None = None
    platform: PlatformConfig | None = None
    cfg: DatasetConfig | None = None
    skipped: dict[str, str] = field(default_factory=dict)

    def table(self, layer: str, table: str) -> DataFrame | None:
        """A silver/gold table, or None if it has not been built yet."""
        import os

        if self.spark is None or self.platform is None:
            return None
        path = self.platform.table_path(layer, table)
        if not os.path.isdir(path):
            return None
        return self.spark.read.format("delta").load(path)

    def skip(self, rule: QualityRule, reason: str) -> None:
        self.skipped[rule.name] = reason


def mark_failed(df: DataFrame, rule: QualityRule, failing: Column) -> DataFrame:
    """Append the rule's name to FAILED on every row where ``failing`` is true."""
    return df.withColumn(
        FAILED,
        F.when(failing, F.array_union(F.col(FAILED), F.array(F.lit(rule.name)))).otherwise(
            F.col(FAILED)
        ),
    )
