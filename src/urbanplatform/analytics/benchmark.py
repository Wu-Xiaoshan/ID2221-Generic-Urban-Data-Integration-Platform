"""Benchmark harness for the query optimisation experiments.

An experiment is a pair of variants that must return the same answer: a baseline and an
optimised version differing in one thing only, a predicate that can prune, a join hint,
a cached intermediate, a session setting. The harness times both, checks they agree, and
captures both physical plans.

Three things it does deliberately:Interleaved repeats, A discarded warm-up., Median, not mean.
But it cannot control the OS page cache. After the first pass the fact table is
partly in RAM, so these numbers describe warm-cache behaviour.
"""

from __future__ import annotations

import re
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class Variant:
    label: str
    sqls: list[str]
    conf: dict[str, str] = field(default_factory=dict)
    setup: list[str] = field(default_factory=list)
    teardown: list[str] = field(default_factory=list)
    setup_fn: Callable[[SparkSession], None] | None = None
    teardown_fn: Callable[[SparkSession], None] | None = None


@dataclass(frozen=True)
class Experiment:
    name: str
    technique: str
    question: str
    baseline: Variant
    optimized: Variant
    expect_equal: bool = True
    notes: str = ""


@dataclass
class VariantResult:
    label: str
    median_sec: float
    samples: list[float]
    setup_sec: float
    plan: str
    markers: dict[str, Any]
    final_plan: str = ""
    aqe: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExperimentResult:
    experiment: Experiment
    baseline: VariantResult
    optimized: VariantResult
    results_agree: bool
    disagreement: str = ""

    @property
    def speedup(self) -> float:
        if self.optimized.median_sec == 0:
            return float("inf")
        return self.baseline.median_sec / self.optimized.median_sec

    @property
    def delta_pct(self) -> float:
        if self.baseline.median_sec == 0:
            return 0.0
        return 100.0 * (self.optimized.median_sec - self.baseline.median_sec) / self.baseline.median_sec


# session configuration
class conf_scope:
    """Apply session settings for the duration of a block, then put them back.  """
    def __init__(self, spark: SparkSession, settings: dict[str, str]):
        self.spark = spark
        self.settings = settings
        self.previous: dict[str, str | None] = {}

    def __enter__(self) -> None:
        for key, value in self.settings.items():
            try:
                self.previous[key] = self.spark.conf.get(key)
            except Exception:
                self.previous[key] = None
            self.spark.conf.set(key, value)

    def __exit__(self, *exc: object) -> None:
        for key, old in self.previous.items():
            if old is None:
                self.spark.conf.unset(key)
            else:
                self.spark.conf.set(key, old)


# result equivalence
def _sort_key(row: tuple) -> tuple:
    return tuple("" if v is None else str(v) for v in row)


def _values_agree(a: Any, b: Any, tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return a == b
        return abs(fa - fb) <= tol * max(1.0, abs(fa), abs(fb))
    return a == b


def compare_results(left: list[tuple], right: list[tuple], tol: float = 1e-9) -> tuple[bool, str]:
    if len(left) != len(right):
        return False, f"row count differs: {len(left)} vs {len(right)}"

    left_sorted = sorted(left, key=_sort_key)
    right_sorted = sorted(right, key=_sort_key)

    for index, (row_a, row_b) in enumerate(zip(left_sorted, right_sorted)):
        if len(row_a) != len(row_b):
            return False, f"row {index}: column count differs, {len(row_a)} vs {len(row_b)}"
        for column, (value_a, value_b) in enumerate(zip(row_a, row_b)):
            if not _values_agree(value_a, value_b, tol):
                return False, (
                    f"row {index}, column {column}: {value_a!r} vs {value_b!r}"
                )
    return True, ""


# plan capture
_SELECTABLE = re.compile(r"^\s*(with|select)\b", re.IGNORECASE)


def explain_formatted(spark: SparkSession, sql: str) -> str:
    if not _SELECTABLE.match(sql):
        return ""
    return spark.sql("EXPLAIN FORMATTED " + sql).collect()[0][0]


def executed_plan(spark: SparkSession, sql: str) -> str:
    if not _SELECTABLE.match(sql):
        return ""
    df = spark.sql(sql)
    df.collect()
    return df._jdf.queryExecution().executedPlan().toString()  # noqa: SLF001


def aqe_markers(final_plan: str) -> dict[str, Any]:
    """What AQE actually did, read off the post-execution plan."""
    return {
        "final_plan": "isFinalPlan=true" in final_plan,
        "aqe_shuffle_reads": final_plan.count("AQEShuffleRead"),
        "coalesced": "coalesced" in final_plan,
        "skewed": "skewed" in final_plan,
    }


def plan_markers(plan: str) -> dict[str, Any]:
    partition_filters = re.findall(r"PartitionFilters:\s*\[(.*?)\]", plan, re.DOTALL)
    detail = re.compile(r"^\(\d+\)\s+(\w+)", re.MULTILINE)
    nodes = detail.findall(plan)
    return {
        "broadcast_join": "BroadcastHashJoin" in plan,
        "sort_merge_join": "SortMergeJoin" in plan,
        "shuffled_hash_join": "ShuffledHashJoin" in plan,
        "broadcast_exchange": nodes.count("BroadcastExchange"),
        "shuffle_exchange": nodes.count("Exchange"),
        "in_memory_scan": "InMemoryTableScan" in plan or "InMemoryRelation" in plan,
        "adaptive": "AdaptiveSparkPlan" in plan,
        "partition_filters": [p.strip() for p in partition_filters if p.strip()],
        "has_partition_filter": any(p.strip() for p in partition_filters),
    }


# running
def _run_sqls(spark: SparkSession, sqls: list[str]) -> list[list[tuple]]:
    return [[tuple(row) for row in spark.sql(sql).collect()] for sql in sqls]


def _time_variant(spark: SparkSession, variant: Variant) -> float:
    started = time.perf_counter()
    _run_sqls(spark, variant.sqls)
    return time.perf_counter() - started


def _prepare(spark: SparkSession, variant: Variant) -> float:
    if not variant.setup and variant.setup_fn is None:
        return 0.0
    started = time.perf_counter()
    _run_sqls(spark, variant.setup)
    if variant.setup_fn is not None:
        variant.setup_fn(spark)
    return time.perf_counter() - started


def _restore(spark: SparkSession, variant: Variant) -> None:
    for sql in variant.teardown:
        spark.sql(sql)
    if variant.teardown_fn is not None:
        variant.teardown_fn(spark)


def run_experiment(
    spark: SparkSession, experiment: Experiment, repeats: int = 3, verbose: bool = True
) -> ExperimentResult:
    variants = [experiment.baseline, experiment.optimized]
    samples: dict[str, list[float]] = {v.label: [] for v in variants}
    setup_sec: dict[str, float] = {}
    plans: dict[str, str] = {}
    final_plans: dict[str, str] = {}
    outputs: dict[str, list[list[tuple]]] = {}

    if verbose:
        print(f"\n--- {experiment.name} [{experiment.technique}] ---")
        print(f"    {experiment.question}")

    # Warm-up and plan capture.
    for variant in variants:
        spark.catalog.clearCache()
        with conf_scope(spark, variant.conf):
            setup_sec[variant.label] = _prepare(spark, variant)
            outputs[variant.label] = _run_sqls(spark, variant.sqls)
            plans[variant.label] = explain_formatted(spark, variant.sqls[-1])
            final_plans[variant.label] = executed_plan(spark, variant.sqls[-1])
        _restore(spark, variant)

    # Interleaved timed repeats.
    for rep in range(1, repeats + 1):
        for variant in variants:
            spark.catalog.clearCache()
            with conf_scope(spark, variant.conf):
                _prepare(spark, variant)
                samples[variant.label].append(_time_variant(spark, variant))
            _restore(spark, variant)
        if verbose:
            print(f"    pass {rep}/{repeats}")

    spark.catalog.clearCache()

    agree, disagreement = True, ""
    if experiment.expect_equal:
        base_out = outputs[experiment.baseline.label]
        opt_out = outputs[experiment.optimized.label]
        if len(base_out) != len(opt_out):
            agree, disagreement = False, "different number of statements"
        else:
            for index, (left, right) in enumerate(zip(base_out, opt_out)):
                agree, disagreement = compare_results(left, right)
                if not agree:
                    disagreement = f"statement {index + 1}: {disagreement}"
                    break

    def build(variant: Variant) -> VariantResult:
        final = final_plans.get(variant.label, "")
        return VariantResult(
            label=variant.label,
            median_sec=statistics.median(samples[variant.label]),
            samples=samples[variant.label],
            setup_sec=setup_sec[variant.label],
            plan=plans[variant.label],
            markers=plan_markers(plans[variant.label]),
            final_plan=final,
            aqe=aqe_markers(final),
        )

    return ExperimentResult(
        experiment=experiment,
        baseline=build(experiment.baseline),
        optimized=build(experiment.optimized),
        results_agree=agree,
        disagreement=disagreement,
    )
