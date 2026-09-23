"""Check that the refreshed gold table and data products equal a rebuild from scratch.

    python scripts/verify_refresh.py

Run jobs/run_refresh.py first. The checks:

  lineage    gold records the current version of every silver table it reads, and the
             current integration config
  gold       gold equals gold rebuilt in memory from the whole of silver, row for row
  products   every product is current, and equals the product recomputed from the whole of
             gold (floating-point columns to within 1e-9, since the order of a sum changes
             its last digits)

It also prints what the latest refresh did. Exits non-zero on failure.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from urbanplatform.analytics import AnalyticsConfig, register_views  # noqa: E402
from urbanplatform.analytics.materialize import product_properties, table_detail  # noqa: E402
from urbanplatform.analytics.products import ProductsConfig, product_sql  # noqa: E402
from urbanplatform.config import PlatformConfig  # noqa: E402
from urbanplatform.integrate import (  # noqa: E402
    FINGERPRINT_PROPERTY,
    LINEAGE_PREFIX,
    IntegrationConfig,
    build_integrated,
)
from urbanplatform.session import build_session  # noqa: E402
from urbanplatform.writer import table_properties, table_version  # noqa: E402

TOLERANCE = 1e-9


class Checks:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0

    def __call__(self, name: str, condition: bool, detail: str) -> None:
        if condition:
            self.passed += 1
            print(f"  PASS  {name}: {detail}")
        else:
            self.failed += 1
            print(f"  FAIL  {name}: {detail}")


def row_hashes(df: DataFrame, key: str, columns: list[str]) -> DataFrame:
    return df.select(key, F.xxhash64(*[F.col(c) for c in columns]).alias("_row_hash"))


def mismatched_rows(stored: DataFrame, fresh: DataFrame, key: list[str]) -> int:
    """Rows present on one side only, or with a value that differs beyond the tolerance."""
    floats = {f.name for f in fresh.schema.fields if f.dataType.typeName() in ("double", "float")}
    s, f = stored.alias("s"), fresh.alias("f")
    joined = s.join(f, [F.col(f"s.{k}") == F.col(f"f.{k}") for k in key], "full_outer")

    differs = F.col(f"s.{key[0]}").isNull() | F.col(f"f.{key[0]}").isNull()
    for column in fresh.columns:
        if column in key:
            continue
        a, b = F.col(f"s.{column}"), F.col(f"f.{column}")
        if column in floats:
            close = F.abs(a - b) <= F.lit(TOLERANCE) * F.greatest(F.lit(1.0), F.abs(b))
            same = (a.isNull() & b.isNull()) | (a.isNotNull() & b.isNotNull() & close)
        else:
            same = a.eqNullSafe(b)
        differs = differs | ~same
    return joined.filter(differs).count()


def main() -> int:
    platform = PlatformConfig.load()
    integration = IntegrationConfig.load()
    analytics = AnalyticsConfig.load()
    products = ProductsConfig.load()
    spark = build_session(platform)
    check = Checks()

    try:
        gold_path = platform.table_path(integration.target.layer, integration.target.table)
        props = table_properties(spark, gold_path)

        print("--- lineage")
        for table in integration.source_tables:
            recorded = props.get(f"{LINEAGE_PREFIX}{table}.version")
            current = table_version(spark, platform.table_path("silver", table))
            check(f"{table} version", recorded == str(current),
                  f"gold records v{recorded}, silver is at v{current}")
        check("integration config", props.get(FINGERPRINT_PROPERTY) == integration.fingerprint,
              f"gold records {props.get(FINGERPRINT_PROPERTY)}, conf/integration.yaml is "
              f"{integration.fingerprint}")

        print("\n--- gold")
        gold = spark.read.format("delta").load(gold_path)
        rebuilt = build_integrated(spark, platform, integration).select(*gold.columns)
        key = integration.fact_key
        n_gold, n_rebuilt = gold.count(), rebuilt.count()
        check("row count", n_gold == n_rebuilt, f"gold {n_gold:,}, rebuilt {n_rebuilt:,}")
        stored_hashes = row_hashes(gold, key, gold.columns)
        rebuilt_hashes = row_hashes(rebuilt, key, gold.columns)
        missing = rebuilt_hashes.subtract(stored_hashes).count()
        extra = stored_hashes.subtract(rebuilt_hashes).count()
        check("equals a full rebuild", missing == 0 and extra == 0,
              f"{missing:,} rebuilt rows not in gold, {extra:,} gold rows not in the rebuild")

        print("\n--- products")
        gold_version = table_version(spark, gold_path)
        register_views(spark, platform, analytics)
        for spec in products.products:
            path = platform.table_path(products.layer, spec.name)
            detail = table_detail(spark, path)
            if detail is None:
                check(f"{spec.name} exists", False, path)
                continue
            built_from = product_properties(detail).get("source_version")
            check(f"{spec.name} current", built_from == str(gold_version),
                  f"built from gold v{built_from}, gold is v{gold_version}")
            stored = spark.read.format("delta").load(path)
            fresh = spark.sql(product_sql(spec.name, analytics, products))
            bad = mismatched_rows(stored, fresh, spec.key)
            check(f"{spec.name} equals a full recompute", bad == 0,
                  f"{stored.count():,} rows, {bad:,} differ")

        print("\n--- latest refresh")
        runs = spark.read.format("delta").load(platform.meta_path("integration_metadata"))
        if "mode" in runs.columns:
            last = runs.orderBy(F.desc("started_at")).first()
            print(f"  gold: {last['mode']} in {last['duration_sec']}s, inserted {last['rows_inserted']}, "
                  f"updated {last['rows_updated']}, deleted {last['rows_deleted']}"
                  + (f" ({last['note']})" if last["note"] else ""))
        meta = spark.read.format("delta").load(platform.meta_path("data_product_metadata"))
        if "refresh_mode" in meta.columns:
            recomputed = (
                meta.filter(F.col("refresh_mode").isin("full", "incremental"))
                .orderBy(F.desc("refreshed_at"))
                .collect()
            )
            shown = set()
            for r in recomputed:
                if r["product_name"] in shown:
                    continue
                shown.add(r["product_name"])
                print(f"  {r['product_name']}: last recomputed {r['refresh_mode']} "
                      f"{r['refreshed_partitions'] or ''} in {r['duration_sec']}s")

        print(f"\n{check.passed} passed, {check.failed} failed")
    finally:
        spark.stop()

    return 1 if check.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
