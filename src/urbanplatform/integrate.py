# Task 5: the integration pipeline.
# Joins the four silver tables into gold/integrated_taxi_trips, one row per trip
# with pickup-hour weather/air quality and the zone + borough of both endpoints.

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
import time
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import REPO_ROOT, PlatformConfig, TargetSpec
from .writer import optimize, write_delta

# configuration
@dataclass(frozen=True)
class ContextSpec:
    name: str
    table: str
    hour_column: str
    max_gap_hours: int
    columns: dict[str, str]  # source name -> target name


@dataclass(frozen=True)
class ZoneLookup:
    fact_key: str
    prefix: str


@dataclass(frozen=True)
class ZoneSpec:
    table: str
    key: str
    columns: list[str]
    lookups: list[ZoneLookup]


@dataclass(frozen=True)
class IntegrationConfig:
    name: str
    description: str
    fact_table: str
    fact_hour_column: str
    context: list[ContextSpec]
    zones: ZoneSpec
    target: TargetSpec

    @staticmethod
    def load(path: str | None = None) -> "IntegrationConfig":
        path = path or os.path.join(REPO_ROOT, "conf", "integration.yaml")
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        zones = raw["zones"]
        return IntegrationConfig(
            name=raw["name"],
            description=raw.get("description", ""),
            fact_table=raw["fact"]["table"],
            fact_hour_column=raw["fact"]["hour_column"],
            context=[
                ContextSpec(
                    name=c["name"],
                    table=c["table"],
                    hour_column=c["hour_column"],
                    max_gap_hours=int(c.get("max_gap_hours", 0)),
                    columns=c["columns"],
                )
                for c in raw.get("context", [])
            ],
            zones=ZoneSpec(
                table=zones["table"],
                key=zones["key"],
                columns=zones["columns"],
                lookups=[ZoneLookup(fact_key=l["fact_key"], prefix=l["prefix"]) for l in zones["lookups"]],
            ),
            target=TargetSpec(**raw["target"]),
        )

# the join
def densify_context(ctx: DataFrame, spec: ContextSpec) -> DataFrame:
    hour = spec.hour_column
    present = ctx.select(hour, *spec.columns.keys()).withColumn("_present", F.lit(True))

    gap = spec.max_gap_hours
    bounds = present.select(
        F.expr(f"min({hour}) - make_interval(0,0,0,0,{gap},0,0)").alias("lo"),
        F.expr(f"max({hour}) + make_interval(0,0,0,0,{gap},0,0)").alias("hi"),
    )
    spine = bounds.select(
        F.explode(F.expr("sequence(lo, hi, interval 1 hour)")).alias(hour)
    )
    dense = (
        spine.join(present, hour, "left")
        .withColumn("_observed", F.coalesce(F.col("_present"), F.lit(False)))
        .withColumn("_observed_hour", F.when(F.col("_observed"), F.col(hour)))
    )

    back = Window.orderBy(hour).rowsBetween(-gap, 0)
    fwd = Window.orderBy(hour).rowsBetween(0, gap)

    dense = (
        dense.withColumn("_prev_hour", F.last("_observed_hour", ignorenulls=True).over(back))
        .withColumn("_next_hour", F.first("_observed_hour", ignorenulls=True).over(fwd))
        .withColumn("_back_dist", (F.unix_timestamp(hour) - F.unix_timestamp("_prev_hour")) / 3600)
        .withColumn("_fwd_dist", (F.unix_timestamp("_next_hour") - F.unix_timestamp(hour)) / 3600)
    )
    use_back = F.col("_prev_hour").isNotNull() & (
        F.col("_next_hour").isNull() | (F.col("_back_dist") <= F.col("_fwd_dist"))
    )
    use_fwd = F.col("_next_hour").isNotNull()

    for src, tgt in spec.columns.items():
        prev_v = F.last(src, ignorenulls=True).over(back)
        next_v = F.first(src, ignorenulls=True).over(fwd)
        dense = dense.withColumn(
            tgt,
            F.when(F.col("_observed"), F.col(src))
            .when(use_back, prev_v)
            .when(use_fwd, next_v),
        )

    dense = dense.withColumn(
        f"{spec.name}_offset_hours",
        F.when(F.col("_observed"), F.lit(0.0))
        .when(use_back, F.col("_back_dist"))
        .when(use_fwd, F.col("_fwd_dist")),
    ).withColumn(
        f"{spec.name}_is_missing", F.col(f"{spec.name}_offset_hours").isNull()
    )

    keep = [hour, *spec.columns.values(), f"{spec.name}_offset_hours", f"{spec.name}_is_missing"]
    return dense.select(*keep)


def attach_context(trips: DataFrame, dense: DataFrame, spec: ContextSpec, fact_hour: str) -> DataFrame:
    joined = trips.join(
        F.broadcast(dense.withColumnRenamed(spec.hour_column, fact_hour)),
        fact_hour,
        "left",
    )
    return joined.withColumn(
        f"{spec.name}_is_missing", F.coalesce(F.col(f"{spec.name}_is_missing"), F.lit(True))
    )


def attach_zones(trips: DataFrame, zones: DataFrame, spec: ZoneSpec) -> DataFrame:
    base = zones.select(spec.key, *spec.columns)
    for lookup in spec.lookups:
        renamed = base.withColumnRenamed(spec.key, lookup.fact_key)
        for col in spec.columns:
            renamed = renamed.withColumnRenamed(col, f"{lookup.prefix}_{col}")
        trips = trips.join(F.broadcast(renamed), lookup.fact_key, "left")
    return trips


def build_integrated(spark: SparkSession, platform: PlatformConfig, cfg: IntegrationConfig) -> DataFrame:
    def silver(table: str) -> DataFrame:
        return spark.read.format("delta").load(platform.table_path("silver", table))

    trips = silver(cfg.fact_table)
    for spec in cfg.context:
        dense = densify_context(silver(spec.table), spec)
        trips = attach_context(trips, dense, spec, cfg.fact_hour_column)
    trips = attach_zones(trips, silver(cfg.zones.table), cfg.zones)
    return trips


# metadata
INTEGRATION_METADATA_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), True),
        StructField("duration_sec", StringType(), True),
        StructField("trips_in", LongType(), True),
        StructField("trips_out", LongType(), True),
        StructField("unlocated_trips", LongType(), True),
        StructField("output_files", IntegerType(), True),
        StructField("context_coverage", StringType(), True),
        StructField("error", StringType(), True),
    ]
)


@dataclass
class IntegrationRun:
    run_id: str
    target_table: str
    status: str = "running"
    started_at: Any = None
    finished_at: Any = None
    duration_sec: str | None = None
    trips_in: int | None = None
    trips_out: int | None = None
    unlocated_trips: int | None = None
    output_files: int | None = None
    context_coverage: dict[str, dict[str, int]] = field(default_factory=dict)
    error: str | None = None

    def to_row(self) -> tuple:
        data = dataclasses.asdict(self)
        data["context_coverage"] = json.dumps(self.context_coverage, separators=(",", ":"))
        return tuple(data[f.name] for f in INTEGRATION_METADATA_SCHEMA.fields)


def _coverage(df: DataFrame, cfg: IntegrationConfig) -> tuple[dict[str, dict[str, int]], int]:
    # One pass over the result: per-context exact/filled/missing counts, plus unlocated trips.
    aggs = [F.count(F.lit(1)).alias("_total"), F.sum(F.col("is_unlocated").cast("long")).alias("_unlocated")]
    for spec in cfg.context:
        off = F.col(f"{spec.name}_offset_hours")
        miss = F.col(f"{spec.name}_is_missing")
        aggs += [
            F.sum((off == 0).cast("long")).alias(f"{spec.name}_exact"),
            F.sum((off > 0).cast("long")).alias(f"{spec.name}_filled"),
            F.sum(miss.cast("long")).alias(f"{spec.name}_missing"),
        ]
    row = df.agg(*aggs).first()
    coverage = {
        spec.name: {
            "exact": int(row[f"{spec.name}_exact"] or 0),
            "filled": int(row[f"{spec.name}_filled"] or 0),
            "missing": int(row[f"{spec.name}_missing"] or 0),
        }
        for spec in cfg.context
    }
    return coverage, int(row["_unlocated"] or 0)


def write_integration_metadata(spark: SparkSession, run: IntegrationRun, path: str) -> None:
    df = spark.createDataFrame([run.to_row()], schema=INTEGRATION_METADATA_SCHEMA)
    df.write.format("delta").mode("append").save(path)


# orchestration
def run_integration(spark: SparkSession, platform: PlatformConfig, cfg: IntegrationConfig) -> IntegrationRun:
    run = IntegrationRun(
        run_id=uuid.uuid4().hex[:12],
        target_table=cfg.target.table,
        started_at=dt.datetime.now(),
    )
    started = time.perf_counter()
    try:
        target_path = platform.table_path(cfg.target.layer, cfg.target.table)

        result = build_integrated(spark, platform, cfg).cache()
        run.trips_out = result.count()
        run.trips_in = spark.read.format("delta").load(
            platform.table_path("silver", cfg.fact_table)
        ).count()

        run.context_coverage, run.unlocated_trips = _coverage(result, cfg)

        write_delta(result, cfg.target, target_path)
        optimize(spark, cfg.target, target_path)
        result.unpersist()

        from delta.tables import DeltaTable

        run.output_files = (
            DeltaTable.forPath(spark, target_path).detail().first()["numFiles"]
        )
        run.status = "success"
    except Exception as exc:
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
    finally:
        run.finished_at = dt.datetime.now()
        run.duration_sec = f"{time.perf_counter() - started:.2f}"
        write_integration_metadata(spark, run, platform.meta_path("integration_metadata"))
    return run
