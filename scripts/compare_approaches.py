"""Week 4 Task 4: check that Approach A reproduces the platform's dataset and model.

Compares data/approach_a/ (written by scripts/approach_a_from_raw.py) with the platform's
ml/trip_duration_dataset and a saved model version:

- the split boundaries and the rows per split
- the same trip_ids, in the same splits
- every dataset column, row by row (null-safe, doubles to 1e-9)
- the missing-value flags, the vector size and the metrics against the model card
- both models' predictions on the platform's test split

    python scripts/compare_approaches.py                                  # after the default run of A
    python scripts/compare_approaches.py --dataset-version 3 --model-version 1 \
        --report reports/comparison/approach_a_without_update.json        # after A --without-update
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pyspark.sql import functions as F

from urbanplatform.config import REPO_ROOT, PlatformConfig
from urbanplatform.session import build_session

A_DIR = os.path.join(REPO_ROOT, "data", "approach_a")
TOLERANCE = 1e-9


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset-version", type=int, help="the platform dataset's Delta version (default latest)")
    parser.add_argument("--model-version", type=int, default=2)
    parser.add_argument("--report", default=os.path.join(REPO_ROOT, "reports", "comparison", "approach_a.json"),
                        help="Approach A's result file")
    args = parser.parse_args()

    with open(args.report, encoding="utf-8") as fh:
        a_result = json.load(fh)
    platform = PlatformConfig.load()
    model_root = os.path.join(platform.table_path("ml", "models"), "trip_duration_model", f"v{args.model_version}")
    with open(os.path.join(model_root, "model.json"), encoding="utf-8") as fh:
        card = json.load(fh)

    checks: list[tuple[bool, str, str]] = []

    def check(ok: bool, name: str, detail: str) -> None:
        checks.append((ok, name, detail))
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}", flush=True)

    spark = build_session(platform)
    try:
        reader = spark.read.format("delta")
        if args.dataset_version is not None:
            reader = reader.option("versionAsOf", args.dataset_version)
        b = reader.load(platform.table_path("ml", "trip_duration_dataset"))
        a = spark.read.parquet(os.path.join(A_DIR, "trip_duration_dataset"))

        split_prop = json.loads(spark.sql(
            f"DESCRIBE DETAIL delta.`{platform.table_path('ml', 'trip_duration_dataset')}`"
        ).first()["properties"]["ml.split"]) if args.dataset_version is None else None
        if split_prop is not None:
            same = all(a_result["split"][k] == split_prop[k] for k in ("newest", "validation_start", "test_start"))
            check(same, "split boundaries", f"A {a_result['split']}, platform {split_prop}")

        rows_a = {r["split"]: r["count"] for r in a.groupBy("split").count().collect()}
        rows_b = {r["split"]: r["count"] for r in b.groupBy("split").count().collect()}
        check(rows_a == rows_b, "rows per split", f"A {rows_a}, platform {rows_b}")

        only_a = a.join(b, "trip_id", "left_anti").count()
        only_b = b.join(a, "trip_id", "left_anti").count()
        check(only_a == 0 and only_b == 0, "same trips", f"{only_a:,} only in A, {only_b:,} only in the platform")

        columns = [c for c in b.columns if c != "trip_id"]
        missing = [c for c in columns if c not in a.columns]
        check(not missing, "same columns", f"missing in A: {missing or 'none'}")
        joined = a.select("trip_id", *[F.col(c).alias("a_" + c) for c in columns]).join(
            b.select("trip_id", *[F.col(c).alias("b_" + c) for c in columns]), "trip_id")

        def differs(c: str):
            x, y = F.col("a_" + c), F.col("b_" + c)
            if dict(b.dtypes)[c] in ("double", "float"):
                return ~((x.isNull() & y.isNull()) | (F.abs(x - y) <= TOLERANCE))
            return ~x.eqNullSafe(y)

        diffs = joined.agg(*[F.sum(differs(c).cast("long")).alias(c) for c in columns]).first().asDict()
        bad = {c: int(n) for c, n in diffs.items() if n}
        check(not bad, "every column row by row", f"{len(columns)} columns, differing rows: {bad or 'none'}")

        check(sorted(a_result["missing_flags"]) == sorted(card["missing_indicators"]), "missing-value flags",
              f"A {a_result['missing_flags']}, platform {card['missing_indicators']}")

        from pyspark.ml import PipelineModel

        import urbanplatform.ml.features  # noqa: F401  (the platform pipeline's own stages)

        model_a = PipelineModel.load(os.path.join(A_DIR, "model"))
        model_b = PipelineModel.load(os.path.join(model_root, "pipeline"))
        size_a = model_a.stages[-1].numFeatures
        check(size_a == card["vector_size"], "vector size", f"A {size_a}, platform {card['vector_size']}")

        for s in ("validation", "test"):
            ma, mb = a_result[s], card["metrics"][s]
            gap = max(abs(ma[m] - mb[m]) for m in ("rmse", "mae", "r2"))
            check(gap < 1e-4, f"{s} metrics", f"A rmse {ma['rmse']:.6f} mae {ma['mae']:.6f} r2 {ma['r2']:.6f}; "
                                               f"platform rmse {mb['rmse']:.6f} mae {mb['mae']:.6f} "
                                               f"r2 {mb['r2']:.6f}; largest gap {gap:.1e}")

        test = b.where("split = 'test'")
        pa = model_a.transform(test).select("trip_id", F.col("prediction").alias("pa"))
        pb = model_b.transform(test).select("trip_id", F.col("prediction").alias("pb"))
        row = pa.join(pb, "trip_id").agg(F.count(F.lit(1)).alias("n"), F.max(F.abs(F.col("pa") - F.col("pb"))).alias("max"),
                                         F.avg(F.abs(F.col("pa") - F.col("pb"))).alias("mean")).first()
        check(row["max"] < 1e-3, "predictions on the test split",
              f"{row['n']:,} trips, largest difference {row['max']:.2e} min, mean {row['mean']:.2e} min")
    finally:
        spark.stop()

    failed = sum(1 for ok, _, _ in checks if not ok)
    print(f"\n{len(checks) - failed} passed, {failed} failed")
    out = os.path.splitext(args.report)[0] + "_check.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump([{"pass": ok, "check": n, "detail": d} for ok, n, d in checks], fh, indent=1)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
