"""Task 3: run the query optimisation experiments and report the measurements.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.analytics import AnalyticsConfig, register_views  # noqa: E402
from urbanplatform.analytics.benchmark import ExperimentResult, run_experiment  # noqa: E402
from urbanplatform.analytics.experiments import (  # noqa: E402
    aqe_sweep,
    build_experiments,
    materialisation_sweep,
)
from urbanplatform.config import REPO_ROOT, PlatformConfig  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402

REPORT_DIR = os.path.join(REPO_ROOT, "reports", "query_benchmark")



def register_products(spark, platform) -> None:
    """Expose each materialised product as a temp view, as verify_products.py does."""
    from urbanplatform.analytics.products import ProductsConfig

    products = ProductsConfig.load()
    for spec in products.products:
        path = platform.table_path(products.layer, spec.name)
        if not os.path.isdir(path):
            raise SystemExit(f"{path} does not exist -- run jobs/run_products.py first")
        spark.read.format("delta").load(path).createOrReplaceTempView(spec.name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=3, help="timed repeats (default 3)")
    parser.add_argument("--sweep", action="store_true",
                        help="also run AQE on/off across every query in the library")
    parser.add_argument("--products", action="store_true",
                        help="also compare each data product against recomputing it "
                             "(needs jobs/run_products.py to have run)")
    parser.add_argument("--only", nargs="*", help="run only these experiments by name")
    return parser.parse_args()


def _markers_summary(markers: dict) -> str:
    bits = []
    if markers.get("broadcast_join"):
        bits.append("BroadcastHashJoin")
    if markers.get("sort_merge_join"):
        bits.append("SortMergeJoin")
    if markers.get("shuffled_hash_join"):
        bits.append("ShuffledHashJoin")
    if markers.get("in_memory_scan"):
        bits.append("InMemoryTableScan")
    if markers.get("has_partition_filter"):
        bits.append("PartitionFilters")
    shuffles = markers.get("shuffle_exchange", 0)
    bits.append(f"{shuffles} shuffle{'' if shuffles == 1 else 's'}")
    broadcasts = markers.get("broadcast_exchange", 0)
    if broadcasts:
        bits.append(f"{broadcasts} broadcast{'' if broadcasts == 1 else 's'}")
    return ", ".join(bits) if bits else "-"


def _aqe_summary(aqe: dict) -> str:
    """What AQE did, from the post-execution plan.

    EXPLAIN FORMATTED cannot answer this: before execution the plan is
    `isFinalPlan=false` and carries none of AQE's rewrites.
    """
    if not aqe:
        return "-"
    reads = aqe.get("aqe_shuffle_reads", 0)
    if not reads:
        return "none"
    bits = [f"{reads} node{'' if reads == 1 else 's'}"]
    if aqe.get("coalesced"):
        bits.append("coalesced")
    if aqe.get("skewed"):
        bits.append("skew-split")
    return ", ".join(bits)


def render(results: list[ExperimentResult], repeats: int) -> str:
    lines: list[str] = []
    add = lines.append

    add("# Query optimisation measurements")
    add("")
    add(f"Generated {dt.datetime.now():%Y-%m-%d %H:%M}. "
        f"Median of {repeats} interleaved repeats after a discarded warm-up.")
    add("")
    add("| Experiment | Technique | Baseline | Optimised | Speed-up | Change | Same result? |")
    add("|---|---|---:|---:|---:|---:|:--:|")
    for r in results:
        agree = "yes" if r.results_agree else "**NO**"
        add(f"| `{r.experiment.name}` | {r.experiment.technique} "
            f"| {r.baseline.median_sec:.2f} s | {r.optimized.median_sec:.2f} s "
            f"| {r.speedup:.2f}x | {r.delta_pct:+.1f}% | {agree} |")
    add("")

    for r in results:
        add(f"## {r.experiment.name}")
        add("")
        add(f"**{r.experiment.technique}.** {r.experiment.question}")
        if r.experiment.notes:
            add("")
            add(r.experiment.notes)
        add("")
        add("| | Baseline | Optimised |")
        add("|---|---|---|")
        add(f"| variant | {r.baseline.label} | {r.optimized.label} |")
        add(f"| median | {r.baseline.median_sec:.3f} s | {r.optimized.median_sec:.3f} s |")
        add(f"| samples | {', '.join(f'{s:.3f}' for s in r.baseline.samples)} "
            f"| {', '.join(f'{s:.3f}' for s in r.optimized.samples)} |")
        if r.baseline.setup_sec or r.optimized.setup_sec:
            add(f"| setup | {r.baseline.setup_sec:.3f} s | {r.optimized.setup_sec:.3f} s |")
        add(f"| plan | {_markers_summary(r.baseline.markers)} "
            f"| {_markers_summary(r.optimized.markers)} |")
        pf_base = r.baseline.markers.get("partition_filters") or ["-"]
        pf_opt = r.optimized.markers.get("partition_filters") or ["-"]
        add(f"| PartitionFilters | `{'; '.join(pf_base)}` | `{'; '.join(pf_opt)}` |")
        add(f"| AQEShuffleRead (executed plan) | {_aqe_summary(r.baseline.aqe)} "
            f"| {_aqe_summary(r.optimized.aqe)} |")
        add("")
        if r.results_agree:
            add("Results verified identical.")
        else:
            add(f"**Results differ: {r.disagreement}**")
        add("")
    return "\n".join(lines)


def save_plans(results: list[ExperimentResult]) -> None:
    """Both plans per variant: EXPLAIN FORMATTED, and the plan actually executed."""
    for r in results:
        for side, variant in (("baseline", r.baseline), ("optimized", r.optimized)):
            for suffix, plan in (("txt", variant.plan), ("final.txt", variant.final_plan)):
                if not plan:
                    continue
                path = os.path.join(REPORT_DIR, "plans", f"{r.experiment.name}.{side}.{suffix}")
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write(f"# {r.experiment.name} / {variant.label}\n\n{plan}\n")


def main() -> int:
    args = parse_args()
    analytics = AnalyticsConfig.load()
    platform = PlatformConfig.load()

    experiments = build_experiments(analytics)
    if args.sweep:
        experiments += aqe_sweep(analytics)
    if args.products:
        experiments += materialisation_sweep(analytics)
    if args.only:
        known = {e.name for e in experiments}
        unknown = [n for n in args.only if n not in known]
        if unknown:
            raise SystemExit(f"unknown experiment(s): {', '.join(unknown)}\n"
                             f"available: {', '.join(sorted(known))}")
        experiments = [e for e in experiments if e.name in args.only]

    source = analytics.sources["integrated"]
    integrated = platform.table_path(source["layer"], source["table"])
    if not os.path.isdir(integrated):
        raise SystemExit(f"{integrated} does not exist -- run jobs/run_integrate.py first")

    spark = build_session(platform)
    try:
        register_views(spark, platform, analytics)
        if args.products:
            register_products(spark, platform)
        results = [run_experiment(spark, e, repeats=args.repeats) for e in experiments]
    finally:
        spark.stop()

    os.makedirs(REPORT_DIR, exist_ok=True)
    report = render(results, args.repeats)
    report_path = os.path.join(REPORT_DIR, "measurements.md")
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write(report + "\n")
    save_plans(results)

    print("\n" + report)
    print(f"\nwritten: {report_path}")
    print(f"plans:   {os.path.join(REPORT_DIR, 'plans')}")
    failures = [r for r in results if not r.results_agree]
    if failures:
        print(f"\nWARNING: {len(failures)} experiment(s) produced different results:")
        for r in failures:
            print(f"  {r.experiment.name}: {r.disagreement}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
