# Correctness checks for a task's training dataset and feature table.

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from urbanplatform.config import PlatformConfig
from urbanplatform.ml import MLTaskConfig
from urbanplatform.ml.config import SPLITS
from urbanplatform.ml.dataset import LABEL, SPLIT, build_dataset, read_source
from urbanplatform.ml.features import _CAT, FEATURES, learnt_state, prepare
from urbanplatform.ml.featurize import load_feature_pipeline
from urbanplatform.ml.metadata import PROP
from urbanplatform.session import build_session
from urbanplatform.writer import table_properties, table_version


class Checks:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0

    def __call__(self, name: str, condition: bool, detail: str) -> None:
        self.passed += bool(condition)
        self.failed += not condition
        print(f"  {'PASS' if condition else 'FAIL'}  {name}: {detail}")


def fingerprint(df: DataFrame, key: str) -> dict[str, tuple[int, int]]:
    """Per split: row count and an order-independent checksum of the keys."""
    rows = df.groupBy(SPLIT).agg(F.count(F.lit(1)).alias("n"),
                                 F.sum(F.xxhash64(key).cast("decimal(38,0)")).alias("h")).collect()
    return {r[SPLIT]: (int(r["n"]), int(r["h"])) for r in rows}


def main() -> int:
    task = MLTaskConfig.load(sys.argv[1] if len(sys.argv) > 1 else "trip_duration")
    platform = PlatformConfig.load()
    spark = build_session(platform)
    check = Checks()
    ds_path = platform.table_path("ml", task.dataset.output)
    ft_path = platform.table_path("ml", task.features.output)
    try:
        print(f"\ntraining dataset ml/{task.dataset.output}")
        props = table_properties(spark, ds_path)
        check("current configuration", props.get(PROP + "config_fingerprint") == task.dataset_fingerprint,
              f"built under {props.get(PROP + 'config_fingerprint')}, conf/ml is {task.dataset_fingerprint}")
        ds = spark.read.format("delta").load(ds_path)
        expected = {task.key, LABEL, SPLIT, *([task.dataset.split.column] if task.dataset.split.column else []),
                    *task.dataset.columns}
        check("declared columns only", set(ds.columns) == expected,
              f"extra {sorted(set(ds.columns) - expected) or 'none'}, missing {sorted(expected - set(ds.columns)) or 'none'}")
        rows, keys = ds.count(), ds.select(task.key).distinct().count()
        check("one row per key", rows == keys, f"{rows:,} rows, {keys:,} distinct {task.key}")
        check("labels present", ds.filter(F.col(LABEL).isNull()).count() == 0, "no null label")

        split = json.loads(props.get(PROP + "split", "{}"))
        if task.dataset.split.strategy == "chronological":
            col = task.dataset.split.column
            r = {x[SPLIT]: x for x in ds.groupBy(SPLIT).agg(F.min(col).alias("lo"), F.max(col).alias("hi")).collect()}
            ordered = (str(r["train"]["hi"]) < split["validation_start"] <= str(r["validation"]["lo"])
                       and str(r["validation"]["hi"]) < split["test_start"] <= str(r["test"]["lo"]))
            check("splits in time order", ordered,
                  " | ".join(f"{s} {r[s]['lo']}..{r[s]['hi']}" for s in SPLITS if s in r))

        source, version = read_source(spark, platform, task, int(props[PROP + "source_version"]))
        rebuilt, _ = build_dataset(source, task, boundaries=split)
        stored, again = fingerprint(ds, task.key), fingerprint(rebuilt, task.key)
        check("reproducible from gold", stored == again,
              f"rebuilt from {task.source_table} v{version}: "
              + ", ".join(f"{s} {stored.get(s, (0,))[0]:,}" for s in SPLITS))

        print(f"\nfeature table ml/{task.features.output}")
        fprops = table_properties(spark, ft_path)
        ds_version = table_version(spark, ds_path)
        check("built from the current dataset", fprops.get(PROP + "dataset_version") == str(ds_version),
              f"dataset v{fprops.get(PROP + 'dataset_version')}, current v{ds_version}")
        check("current configuration", fprops.get(PROP + "config_fingerprint") == task.features_fingerprint,
              f"built under {fprops.get(PROP + 'config_fingerprint')}, conf/ml is {task.features_fingerprint}")
        ft = spark.read.format("delta").load(ft_path)
        check("one row per dataset row", fingerprint(ft, task.key) == stored, "same keys in the same splits")
        nulls = ft.filter(F.col(FEATURES).isNull() | F.col(LABEL).isNull()).count()
        check("no null vector or label", nulls == 0, f"{nulls:,} rows with a null")

        model, manifest = load_feature_pipeline(platform, task)
        size = ft.select(F.size(vector_to_array(FEATURES))).first()[0]
        check("vector size", size == manifest.size == int(fprops[PROP + "vector_size"]),
              f"vectors {size}, manifest {manifest.size}, table {fprops[PROP + 'vector_size']}")

        state = learnt_state(task.features, model)
        train_prepared = prepare(task.features, ds.filter(F.col(SPLIT) == "train"))
        categorical = [f.name for f in task.features.of_kind("categorical")]
        seen = train_prepared.agg(*[F.collect_set(_CAT + n).alias(n) for n in categorical]).first().asDict()
        mismatched = [n for n in categorical if set(state["labels"][n]) != set(seen[n])]
        check("categories learnt from train only", not mismatched,
              f"{len(categorical)} category lists equal the training values"
              + (f"; differ: {', '.join(mismatched)}" if mismatched else ""))

        probe = ds.where(F.pmod(F.xxhash64(task.key), F.lit(10_000)) == 0)
        rescored = model.transform(probe.drop(LABEL)).select(task.key, F.col(FEATURES).alias("again"))
        joined = ft.join(rescored, task.key)
        n = joined.count()
        diff = joined.filter(vector_to_array(FEATURES) != vector_to_array("again")).count() if n else 0
        check("reloaded pipeline reproduces vectors", n > 0 and diff == 0,
              f"{n:,} sampled rows rescored without their label, {diff} differ")
    finally:
        spark.stop()

    print(f"\n{check.passed} passed, {check.failed} failed")
    return 1 if check.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
