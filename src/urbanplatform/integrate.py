# Task 5: the integration pipeline.
# Joins the four silver tables into gold/integrated_taxi_trips, one row per trip
# with pickup-hour weather/air quality and the zone + borough of both endpoints.
#
# Week 3: gold records the silver versions it was built from, and refresh_integration
# brings it up to date from the silver tables' change feed instead of rebuilding it.

from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
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
from .writer import (
    enable_change_data_feed,
    merge_commit,
    optimize,
    partition_predicate,
    read_changes,
    replaced_between,
    set_table_properties,
    table_properties,
    table_version,
    write_delta,
)

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
    fact_key: str
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
            fact_key=raw["fact"]["key"],
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

    @property
    def fingerprint(self) -> str:
        """A hash of everything that decides gold's contents: tables, columns, gap limits.

        Recorded on gold. When it changes, rows already in gold were built under a
        different definition, so the refresh rebuilds gold instead of patching it.
        """
        definition = dataclasses.asdict(self)
        definition.pop("description")
        payload = json.dumps(definition, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode()).hexdigest()[:12]

    @property
    def source_tables(self) -> list[str]:
        """The silver tables gold is built from, fact table first."""
        tables = [self.fact_table, *(c.table for c in self.context), self.zones.table]
        return list(dict.fromkeys(tables))


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


def build_integrated(
    spark: SparkSession,
    platform: PlatformConfig,
    cfg: IntegrationConfig,
    trips: DataFrame | None = None,
) -> DataFrame:
    """Enrich ``trips`` (by default the whole silver fact table) with context and zones.

    The context tables are always densified in full: they are a few thousand rows, and a
    trip's fill value can come from an hour on either side of it.
    """
    def silver(table: str) -> DataFrame:
        return spark.read.format("delta").load(platform.table_path("silver", table))

    if trips is None:
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
        # Added in Week 3. mode is full, incremental or skipped; note says why a refresh fell
        # back to a full rebuild. The row counts are gold rows changed by an incremental run.
        StructField("mode", StringType(), True),
        StructField("note", StringType(), True),
        StructField("rows_inserted", LongType(), True),
        StructField("rows_updated", LongType(), True),
        StructField("rows_deleted", LongType(), True),
        StructField("source_versions", StringType(), True),
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
    mode: str = "full"
    note: str | None = None
    rows_inserted: int | None = None
    rows_updated: int | None = None
    rows_deleted: int | None = None
    source_versions: dict[str, int] = field(default_factory=dict)

    def to_row(self) -> tuple:
        data = dataclasses.asdict(self)
        data["context_coverage"] = json.dumps(self.context_coverage, separators=(",", ":"))
        data["source_versions"] = (
            json.dumps(self.source_versions, separators=(",", ":")) if self.source_versions else None
        )
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
    # mergeSchema lets the Week 1 table gain the Week 3 columns on its next append.
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


# lineage: which silver versions gold was built from
LINEAGE_PREFIX = "integration.source."
FINGERPRINT_PROPERTY = "integration.config_fingerprint"


def _silver_path(platform: PlatformConfig, table: str) -> str:
    return platform.table_path("silver", table)


def _prepare_sources(spark: SparkSession, platform: PlatformConfig, cfg: IntegrationConfig) -> dict[str, int]:
    """Switch on the change feed of every source table and return their current versions.

    Turning the feed on before recording a version is what makes the recorded version a
    valid starting point: the next refresh reads the feed from the commit after it.
    """
    versions = {}
    for table in cfg.source_tables:
        path = _silver_path(platform, table)
        enable_change_data_feed(spark, path)
        versions[table] = table_version(spark, path)
    return versions


def _record_lineage(spark: SparkSession, path: str, cfg: IntegrationConfig, versions: dict[str, int]) -> None:
    props = {f"{LINEAGE_PREFIX}{table}.version": str(v) for table, v in versions.items()}
    props[FINGERPRINT_PROPERTY] = cfg.fingerprint
    set_table_properties(spark, path, props)


def _recorded_lineage(props: dict[str, str], cfg: IntegrationConfig) -> dict[str, int] | None:
    recorded = {}
    for table in cfg.source_tables:
        value = props.get(f"{LINEAGE_PREFIX}{table}.version")
        if value is None:
            return None
        recorded[table] = int(value)
    return recorded


# orchestration
def run_integration(
    spark: SparkSession,
    platform: PlatformConfig,
    cfg: IntegrationConfig,
    note: str | None = None,
) -> IntegrationRun:
    """Rebuild gold from the whole of silver, and record the versions it was built from."""
    run = IntegrationRun(
        run_id=uuid.uuid4().hex[:12],
        target_table=cfg.target.table,
        started_at=dt.datetime.now(),
        mode="full",
        note=note,
    )
    started = time.perf_counter()
    try:
        target_path = platform.table_path(cfg.target.layer, cfg.target.table)
        run.source_versions = _prepare_sources(spark, platform, cfg)

        result = build_integrated(spark, platform, cfg).cache()
        run.trips_out = result.count()
        run.trips_in = spark.read.format("delta").load(
            platform.table_path("silver", cfg.fact_table)
        ).count()

        run.context_coverage, run.unlocated_trips = _coverage(result, cfg)

        write_delta(result, cfg.target, target_path)
        optimize(spark, cfg.target, target_path)
        result.unpersist()

        enable_change_data_feed(spark, target_path)
        _record_lineage(spark, target_path, cfg, run.source_versions)
        run.output_files = _num_files(spark, target_path)
        run.status = "success"
    except Exception as exc:
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
    finally:
        run.finished_at = dt.datetime.now()
        run.duration_sec = f"{time.perf_counter() - started:.2f}"
        if platform.monitoring_enabled:
            write_integration_metadata(spark, run, platform.meta_path("integration_metadata"))
    return run


def _num_files(spark: SparkSession, path: str) -> int:
    from delta.tables import DeltaTable

    return DeltaTable.forPath(spark, path).detail().first()["numFiles"]


class _Rebuild(Exception):
    """Raised while planning a refresh when only a full rebuild gives a correct gold."""


@dataclass
class _ChangeSet:
    current: dict[str, int]
    upserts: DataFrame | None       # silver fact rows to (re)enrich and write
    deletes: DataFrame | None       # fact keys and partitions to remove from gold
    affected_hours: DataFrame | None  # fact hours whose context values may have changed


def _latest_fact_changes(changes: DataFrame, key: str) -> DataFrame:
    """One row per key: its last change in the range.

    A key can change several times between two refreshes, for instance inserted, then
    removed by a RESTORE, then inserted again. Only its final state matters. Within one
    commit a surviving insert wins over a delete.
    """
    order = Window.partitionBy(key).orderBy(
        F.desc("_commit_version"), F.when(F.col("_change_type") == "delete", 1).otherwise(0)
    )
    return (
        changes.filter(F.col("_change_type") != "update_preimage")
        .withColumn("_rank", F.row_number().over(order))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def _plan_changes(
    spark: SparkSession,
    platform: PlatformConfig,
    cfg: IntegrationConfig,
    recorded: dict[str, int],
    gold_columns: list[str],
) -> _ChangeSet:
    """Work out what changed in silver since gold was last built, from the change feeds."""
    current = _prepare_sources(spark, platform, cfg)
    changed = [t for t in cfg.source_tables if current[t] != recorded[t]]

    if cfg.zones.table in changed:
        # Every trip carries zone names, and the zone table is rewritten whole on each load.
        raise _Rebuild(f"{cfg.zones.table} changed (v{recorded[cfg.zones.table]} -> v{current[cfg.zones.table]})")
    for table in changed:
        replaced = replaced_between(spark, _silver_path(platform, table), recorded[table] + 1, current[table])
        if replaced:
            raise _Rebuild(f"{table} was overwritten at {replaced}")

    fact_columns = spark.read.format("delta").load(_silver_path(platform, cfg.fact_table)).columns
    new_columns = [c for c in fact_columns if c not in gold_columns]
    if new_columns:
        raise _Rebuild(f"{cfg.fact_table} gained columns {new_columns} that gold does not have")

    def changes(table: str) -> DataFrame:
        try:
            df = read_changes(spark, _silver_path(platform, table), recorded[table] + 1, current[table])
            df.limit(1).collect()  # surfaces a range the feed does not cover
            return df
        except Exception as exc:  # noqa: BLE001
            raise _Rebuild(f"no change feed for {table} from v{recorded[table] + 1}: {type(exc).__name__}") from exc

    upserts = deletes = affected = None
    if cfg.fact_table in changed:
        cdf_columns = ["_change_type", "_commit_version", "_commit_timestamp"]
        latest = _latest_fact_changes(changes(cfg.fact_table), cfg.fact_key)
        upserts = latest.filter(F.col("_change_type") != "delete").drop(*cdf_columns)
        deletes = latest.filter(F.col("_change_type") == "delete").select(
            cfg.fact_key, *cfg.target.partition_by
        )

    # A context value can reach a trip up to max_gap_hours away (the nearest-hour fill), so
    # a changed context hour affects the trips within that distance of it.
    for spec in cfg.context:
        if spec.table not in changed:
            continue
        gap = spec.max_gap_hours
        hours = (
            changes(spec.table)
            .select(F.col(spec.hour_column).alias("_hour"))
            .distinct()
            .select(
                F.explode(
                    F.sequence(
                        F.col("_hour") - F.expr(f"INTERVAL {gap} HOURS"),
                        F.col("_hour") + F.expr(f"INTERVAL {gap} HOURS"),
                        F.expr("INTERVAL 1 HOUR"),
                    )
                ).alias(cfg.fact_hour_column)
            )
        )
        affected = hours if affected is None else affected.unionByName(hours)
    if affected is not None:
        affected = affected.distinct()

    return _ChangeSet(current=current, upserts=upserts, deletes=deletes, affected_hours=affected)


def refresh_integration(spark: SparkSession, platform: PlatformConfig, cfg: IntegrationConfig) -> IntegrationRun:
    """Bring gold up to date with silver, touching only the trips that changed.

    Reads the silver change feeds from the versions gold records, recomputes the trips
    that were added or changed and the trips whose pickup-hour context changed, and
    applies them in one MERGE. Falls back to run_integration whenever the change feed
    cannot say what changed.
    """
    from delta.tables import DeltaTable

    # Timed from here, so the duration includes working out what changed.
    started_at, started = dt.datetime.now(), time.perf_counter()
    target_path = platform.table_path(cfg.target.layer, cfg.target.table)
    if not DeltaTable.isDeltaTable(spark, target_path):
        return run_integration(spark, platform, cfg, note="gold does not exist yet")
    props = table_properties(spark, target_path)
    recorded = _recorded_lineage(props, cfg)
    if recorded is None:
        return run_integration(spark, platform, cfg, note="gold does not record its source versions")
    if props.get(FINGERPRINT_PROPERTY) != cfg.fingerprint:
        return run_integration(spark, platform, cfg, note="conf/integration.yaml changed")

    gold_columns = spark.read.format("delta").load(target_path).columns
    try:
        plan = _plan_changes(spark, platform, cfg, recorded, gold_columns)
    except _Rebuild as reason:
        return run_integration(spark, platform, cfg, note=str(reason))

    run = IntegrationRun(
        run_id=uuid.uuid4().hex[:12],
        target_table=cfg.target.table,
        started_at=started_at,
        mode="incremental",
        source_versions=plan.current,
    )
    try:
        parts = [plan.upserts] if plan.upserts is not None else []
        if plan.affected_hours is not None:
            fact = spark.read.format("delta").load(_silver_path(platform, cfg.fact_table))
            parts.append(fact.join(F.broadcast(plan.affected_hours), cfg.fact_hour_column, "left_semi"))

        source = None
        if parts:
            trips = parts[0]
            for extra in parts[1:]:
                trips = trips.unionByName(extra)
            enriched = build_integrated(spark, platform, cfg, trips=trips.dropDuplicates([cfg.fact_key]))
            source = enriched.select(*gold_columns).withColumn("_op", F.lit("upsert"))
        if plan.deletes is not None:
            removals = plan.deletes.withColumn("_op", F.lit("delete"))
            source = removals if source is None else source.unionByName(removals, allowMissingColumns=True)

        source = source.cache() if source is not None else None
        pending = source.count() if source is not None else 0
        if pending == 0:
            run.mode = "skipped"
            run.rows_inserted = run.rows_updated = run.rows_deleted = 0
        else:
            upserted = source.filter(F.col("_op") == "upsert")
            run.trips_in = upserted.count()
            run.context_coverage, run.unlocated_trips = _coverage(upserted, cfg)

            keys = [cfg.fact_key, *[c for c in cfg.target.partition_by if c != cfg.fact_key]]
            condition = " AND ".join(f"t.`{c}` = s.`{c}`" for c in keys)
            assignments = {c: f"s.`{c}`" for c in gold_columns}
            before = table_version(spark, target_path)
            (
                DeltaTable.forPath(spark, target_path).alias("t")
                .merge(source.alias("s"), condition)
                .whenMatchedDelete(condition="s._op = 'delete'")
                .whenMatchedUpdate(condition="s._op = 'upsert'", set=assignments)
                .whenNotMatchedInsert(condition="s._op = 'upsert'", values=assignments)
                .execute()
            )
            commit = merge_commit(spark, target_path, before)
            metrics = (commit["operationMetrics"] or {}) if commit is not None else {}
            run.rows_inserted = int(metrics.get("numTargetRowsInserted", 0))
            run.rows_updated = int(metrics.get("numTargetRowsUpdated", 0))
            run.rows_deleted = int(metrics.get("numTargetRowsDeleted", 0))

            if cfg.target.partition_by:
                touched = source.select(*cfg.target.partition_by).distinct().collect()
                where = partition_predicate(cfg.target.partition_by, touched)
                if where:
                    optimize(spark, cfg.target, target_path, where)
        if source is not None:
            source.unpersist()

        # Only when a source moved on: an unchanged gold keeps its version, so the products
        # can see at once that they are still current.
        if plan.current != recorded:
            _record_lineage(spark, target_path, cfg, plan.current)
        run.trips_out = spark.read.format("delta").load(target_path).count()
        run.output_files = _num_files(spark, target_path)
        run.status = "success"
    except Exception as exc:
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
    finally:
        run.finished_at = dt.datetime.now()
        run.duration_sec = f"{time.perf_counter() - started:.2f}"
        if platform.monitoring_enabled:
            write_integration_metadata(spark, run, platform.meta_path("integration_metadata"))
    return run
