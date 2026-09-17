"""Materialise the data products as Delta tables and record their metadata.

Metadata is kept in two places. Each product table carries it as Delta table properties
(``product.*``), so the table describes itself wherever it is read from. And
_meta/data_product_metadata gets one row per product per run, so refresh history, sizes and
failures can be queried across all products, the way _meta/integration_metadata works.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..config import PlatformConfig, TargetSpec
from ..reader import schema_fingerprint
from ..writer import optimize
from .config import AnalyticsConfig
from .products import ProductsConfig, ProductSpec, product_sql
from .views import register_views

# Delta reserves the delta. prefix for its own settings.
PROP = "product."


# table state
def table_detail(spark: SparkSession, path: str) -> dict[str, Any] | None:
    """DESCRIBE DETAIL of a Delta table as a dict, or None when no table is there yet."""
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        return None
    return DeltaTable.forPath(spark, path).detail().first().asDict()


def table_version(spark: SparkSession, path: str) -> int:
    from delta.tables import DeltaTable

    return int(DeltaTable.forPath(spark, path).history(1).first()["version"])


def product_properties(detail: dict[str, Any] | None) -> dict[str, str]:
    if not detail:
        return {}
    return {k[len(PROP):]: v for k, v in (detail.get("properties") or {}).items() if k.startswith(PROP)}


def _set_properties(spark: SparkSession, path: str, props: dict[str, str]) -> None:
    def literal(value: str) -> str:
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

    pairs = ", ".join(f"{literal(PROP + k)} = {literal(v)}" for k, v in props.items())
    spark.sql(f"ALTER TABLE delta.`{path.replace(chr(92), '/')}` SET TBLPROPERTIES ({pairs})")


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


# metadata
PRODUCT_METADATA_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("source_table", StringType(), True),
        StructField("source_version", LongType(), True),
        StructField("schema_version", StringType(), True),
        StructField("schema_fingerprint", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("refreshed_at", TimestampType(), True),
        StructField("duration_sec", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("num_files", IntegerType(), True),
        StructField("size_bytes", LongType(), True),
        StructField("target_path", StringType(), True),
        StructField("partition_by", StringType(), True),
        StructField("error", StringType(), True),
    ]
)


@dataclass
class ProductRun:
    run_id: str
    product_name: str
    status: str = "running"  # success | skipped | failed
    source_table: str | None = None
    source_version: int | None = None
    schema_version: str | None = None
    schema_fingerprint: str | None = None
    created_at: Any = None
    refreshed_at: Any = None
    duration_sec: str | None = None
    row_count: int | None = None
    num_files: int | None = None
    size_bytes: int | None = None
    target_path: str | None = None
    partition_by: str | None = None
    error: str | None = None

    def to_row(self) -> tuple:
        data = dataclasses.asdict(self)
        return tuple(data[f.name] for f in PRODUCT_METADATA_SCHEMA.fields)


def write_product_metadata(spark: SparkSession, runs: list[ProductRun], path: str) -> None:
    df = spark.createDataFrame([run.to_row() for run in runs], schema=PRODUCT_METADATA_SCHEMA)
    df.write.format("delta").mode("append").save(path)


# refresh
def refresh_product(
    spark: SparkSession,
    platform: PlatformConfig,
    analytics: AnalyticsConfig,
    products: ProductsConfig,
    spec: ProductSpec,
    run_id: str,
    if_stale: bool = False,
) -> ProductRun:
    source_path = platform.table_path(products.source_layer, products.source_table)
    target_path = platform.table_path(products.layer, spec.name)
    run = ProductRun(
        run_id=run_id,
        product_name=spec.name,
        source_table=f"{products.source_layer}/{products.source_table}",
        schema_version=spec.schema_version,
        target_path=target_path,
        partition_by=",".join(spec.partition_by) or None,
    )
    started = time.perf_counter()
    try:
        run.source_version = table_version(spark, source_path)
        existing = table_detail(spark, target_path)
        props = product_properties(existing)

        # A product built from this version of the gold table under this schema version is
        # already current; --if-stale leaves it alone.
        if (
            if_stale
            and props.get("source_version") == str(run.source_version)
            and props.get("schema_version") == spec.schema_version
        ):
            run.status = "skipped"
            run.schema_fingerprint = props.get("schema_fingerprint")
            run.created_at = dt.datetime.fromisoformat(props["created_at"])
            run.refreshed_at = dt.datetime.fromisoformat(props["refreshed_at"])
            run.row_count = spark.read.format("delta").load(target_path).count()
            run.num_files, run.size_bytes = existing["numFiles"], existing["sizeInBytes"]
            return run

        df = spark.sql(product_sql(spec.name, analytics, products))
        run.schema_fingerprint = schema_fingerprint(df)

        # Columns that change under an unchanged schema_version would break readers silently.
        schema_changed = bool(props) and props.get("schema_fingerprint") != run.schema_fingerprint
        if schema_changed and props.get("schema_version") == spec.schema_version:
            raise ValueError(
                f"the columns of {spec.name} changed but schema_version is still "
                f"{spec.schema_version}; bump it in conf/products.yaml"
            )
        layout_changed = existing is not None and list(existing["partitionColumns"]) != spec.partition_by

        now = _utc_now()
        run.created_at = dt.datetime.fromisoformat(props["created_at"]) if "created_at" in props else now
        run.refreshed_at = now

        writer = df.write.format("delta").mode("overwrite")
        if spec.partition_by:
            writer = writer.partitionBy(*spec.partition_by)
        if schema_changed or layout_changed:
            writer = writer.option("overwriteSchema", "true")
        writer.save(target_path)

        target = TargetSpec(
            table=spec.name,
            layer=products.layer,
            partition_by=spec.partition_by,
            zorder_by=spec.zorder_by,
        )
        optimize(spark, target, target_path)

        _set_properties(
            spark,
            target_path,
            {
                "description": spec.description,
                "grain": spec.grain,
                "source": run.source_table,
                "source_version": str(run.source_version),
                "schema_version": spec.schema_version,
                "schema_fingerprint": run.schema_fingerprint,
                "created_at": run.created_at.isoformat(),
                "refreshed_at": run.refreshed_at.isoformat(),
            },
        )

        detail = table_detail(spark, target_path)
        run.row_count = spark.read.format("delta").load(target_path).count()
        run.num_files, run.size_bytes = detail["numFiles"], detail["sizeInBytes"]
        run.status = "success"
    except Exception as exc:
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
    finally:
        run.duration_sec = f"{time.perf_counter() - started:.2f}"
    return run


def refresh_products(
    spark: SparkSession,
    platform: PlatformConfig,
    analytics: AnalyticsConfig,
    products: ProductsConfig,
    names: list[str] | None = None,
    if_stale: bool = False,
) -> list[ProductRun]:
    specs = [products.get(n) for n in names] if names else products.products
    register_views(spark, platform, analytics)

    run_id = uuid.uuid4().hex[:12]
    runs = []
    for spec in specs:
        print(f"--- {spec.name}")
        runs.append(refresh_product(spark, platform, analytics, products, spec, run_id, if_stale))
    write_product_metadata(spark, runs, platform.meta_path("data_product_metadata"))
    return runs
