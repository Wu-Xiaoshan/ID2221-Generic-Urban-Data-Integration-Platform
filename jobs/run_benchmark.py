"""Task 6: rewrite silver/taxi_trips two ways under benchmark/ and measure ingestion time,
storage size, file count and query latency on both. Method and results are in the benchmark report.
"""

from __future__ import annotations

import os
import shutil
import statistics
import sys
import time
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.config import PlatformConfig, TargetSpec  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402
from urbanplatform.writer import optimize, write_delta  # noqa: E402

REPEATS = 3

STRATEGIES = [
    (
        "A: daily partitions",
        TargetSpec(
            table="bench_taxi_daily",
            layer="benchmark",
            mode="overwrite",
            partition_by=["pickup_date"],
        ),
    ),
    (
        "B: unpartitioned + ZORDER",
        TargetSpec(
            table="bench_taxi_zordered",
            layer="benchmark",
            mode="overwrite",
            zorder_by=["pu_location_id"],
        ),
    ),
]

# The three queries Task 6 asks for, plus Q4: Q1 with a single-day predicate added.
# Pruning only applies to a filter, so Q1 vs Q4 is what shows the partition key's worth.
QUERIES = [
    (
        "Q1 trips per borough",
        """
        SELECT z.borough, COUNT(*) AS trip_count
        FROM {trips} t JOIN zones z ON t.pu_location_id = z.location_id
        GROUP BY z.borough
        """,
    ),
    (
        "Q2 avg duration per day",
        """
        SELECT pickup_date, AVG(trip_duration_min) AS avg_duration_min
        FROM {trips}
        GROUP BY pickup_date
        """,
    ),
    (
        "Q3 avg fare per borough",
        """
        SELECT z.borough, AVG(t.fare_amount) AS avg_fare
        FROM {trips} t JOIN zones z ON t.pu_location_id = z.location_id
        GROUP BY z.borough
        """,
    ),
    (
        "Q4 Q1 filtered to one day",
        """
        SELECT z.borough, COUNT(*) AS trip_count
        FROM {trips} t JOIN zones z ON t.pu_location_id = z.location_id
        WHERE t.pickup_date = DATE '{probe_date}'
        GROUP BY z.borough
        """,
    ),
]


def clear(path: str, lakehouse_root: str) -> None:
    """Delete a benchmark table so each strategy is written from scratch."""
    if not os.path.abspath(path).startswith(os.path.abspath(lakehouse_root)):
        raise SystemExit(f"refusing to delete {path}: outside the lakehouse root")
    if os.path.isdir(path):
        shutil.rmtree(path)


def table_metrics(spark, path: str) -> tuple[float, int, float]:
    """(live MB, live file count, on-disk MB including superseded versions)."""
    from delta.tables import DeltaTable

    detail = DeltaTable.forPath(spark, path).detail().select("sizeInBytes", "numFiles").first()

    on_disk = 0
    for dirpath, _, filenames in os.walk(path):
        for name in filenames:
            fp = os.path.join(dirpath, name)
            if not os.path.islink(fp):
                on_disk += os.path.getsize(fp)

    return detail["sizeInBytes"] / 1024**2, detail["numFiles"], on_disk / 1024**2


def build(spark, platform: PlatformConfig, df, target: TargetSpec) -> dict:
    """Write one strategy and measure it. Ingestion time is write + OPTIMIZE."""
    path = platform.table_path(target.layer, target.table)
    clear(path, platform.lakehouse_root)

    start = time.perf_counter()
    write_delta(df, target, path)
    write_sec = time.perf_counter() - start

    start = time.perf_counter()
    optimize(spark, target, path)
    optimize_sec = time.perf_counter() - start

    size_mb, files, on_disk_mb = table_metrics(spark, path)
    return {
        "path": path,
        "write_sec": write_sec,
        "optimize_sec": optimize_sec,
        "ingest_sec": write_sec + optimize_sec,
        "size_mb": size_mb,
        "files": files,
        "on_disk_mb": on_disk_mb,
    }


def time_query(spark, sql: str) -> float:
    start = time.perf_counter()
    spark.sql(sql).collect()
    return time.perf_counter() - start


def measure_queries(spark, views: dict[str, str], probe_date) -> dict[str, dict[str, float]]:
    """Median latency per (strategy, query), interleaved so drift over the run is shared."""
    sql_for = {
        (name, label): body.format(trips=view, probe_date=probe_date)
        for name, view in views.items()
        for label, body in QUERIES
    }

    for sql in sql_for.values():
        spark.sql(sql).collect()  # warm-up, discarded

    samples: dict[tuple[str, str], list[float]] = {key: [] for key in sql_for}
    for rep in range(1, REPEATS + 1):
        for label, _ in QUERIES:
            for name in views:
                samples[(name, label)].append(time_query(spark, sql_for[(name, label)]))
        print(f"  pass {rep}/{REPEATS} done")

    return {
        name: {label: statistics.median(samples[(name, label)]) for label, _ in QUERIES}
        for name in views
    }


def report(results: dict, latencies: dict) -> None:
    """Print the results as a Markdown table, ready to paste into the benchmark report."""
    names = list(results)
    width = max(len(n) for n in names) + 4

    print("\n" + "=" * 78)
    print(
        "Benchmark results (query latency: median of "
        f"{REPEATS} interleaved runs, after a warm-up)"
    )
    print("=" * 78 + "\n")

    def row(metric: str, cells: list[str]) -> None:
        print(f"| {metric} | {' | '.join(cells)} |")

    row("Metric", names)
    print(f"|---|{'---|' * len(names)}")
    row("**Ingestion time** (write + OPTIMIZE)",
        [f"{results[n]['ingest_sec']:.2f} s" for n in names])
    row("&nbsp;&nbsp;· write", [f"{results[n]['write_sec']:.2f} s" for n in names])
    row("&nbsp;&nbsp;· OPTIMIZE", [f"{results[n]['optimize_sec']:.2f} s" for n in names])
    row("**Storage size** (live)", [f"{results[n]['size_mb']:.1f} MB" for n in names])
    row("**Generated files** (live)", [str(results[n]["files"]) for n in names])
    row("On disk before VACUUM", [f"{results[n]['on_disk_mb']:.1f} MB" for n in names])
    for label, _ in QUERIES:
        row(f"**{label}**", [f"{latencies[n][label]:.2f} s" for n in names])

    print("\nRaw:")
    for name in names:
        r = results[name]
        print(f"  {name:<{width}} {r['path']}")
        print(
            f"  {'':<{width}} ingest {r['ingest_sec']:.2f}s "
            f"(write {r['write_sec']:.2f}s + optimize {r['optimize_sec']:.2f}s), "
            f"{r['size_mb']:.1f} MB live in {r['files']} files, "
            f"{r['on_disk_mb']:.1f} MB on disk"
        )
        for label, _ in QUERIES:
            print(f"  {'':<{width}} {label:<28} {latencies[name][label]:.4f} s")


def main() -> int:
    platform = PlatformConfig.load()
    spark = build_session(platform)

    try:
        trips_path = platform.table_path("silver", "taxi_trips")
        zones_path = platform.table_path("silver", "taxi_zones")
        if not os.path.isdir(trips_path):
            raise SystemExit(f"{trips_path} does not exist -- run jobs/run_ingest.py first")

        df_trips = spark.read.format("delta").load(trips_path)
        df_zones = spark.read.format("delta").load(zones_path)
        df_zones.createOrReplaceTempView("zones")

        # A date in the middle of the covered range, so Q4 filters to a partition that
        # exists. The extract is a contiguous 2024-01-01..2024-03-31.
        probe_date = df_trips.selectExpr("min(pickup_date) AS d").first()["d"] + timedelta(days=45)

        results = {}
        for name, target in STRATEGIES:
            print(f"\n[{name}] writing {target.table} ...")
            results[name] = build(spark, platform, df_trips, target)
            r = results[name]
            print(
                f"-> ingest {r['ingest_sec']:.2f}s | {r['size_mb']:.1f} MB live "
                f"({r['on_disk_mb']:.1f} MB on disk) | {r['files']} files"
            )

        views = {}
        for name, target in STRATEGIES:
            view = f"trips_{target.table}"
            spark.read.format("delta").load(results[name]["path"]).createOrReplaceTempView(view)
            views[name] = view

        print(f"\nRunning {len(QUERIES)} queries x {len(views)} strategies, "
              f"probe date {probe_date} ...")
        latencies = measure_queries(spark, views, probe_date)

        report(results, latencies)
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
