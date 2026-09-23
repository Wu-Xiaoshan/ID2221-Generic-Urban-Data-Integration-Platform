"""Delta writer: partitioning, write mode, and post-write maintenance."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import DatasetConfig, TargetSpec

_AUTO_MERGE = "spark.databricks.delta.schema.autoMerge.enabled"


@dataclass(frozen=True)
class MergeResult:
    """What one incremental merge did to its table."""

    version: int
    source_rows: int
    inserted: int
    optimized_partitions: list[str]

    @property
    def duplicates(self) -> int:
        return self.source_rows - self.inserted


def write_delta(df: DataFrame, target: TargetSpec, path: str) -> None:
    writer = df.write.format("delta").mode(target.mode)
    if target.partition_by:
        writer = writer.partitionBy(*target.partition_by)
    if target.replace_where:
        writer = writer.option("replaceWhere", target.replace_where)
    writer.save(path)


def optimize(spark: SparkSession, target: TargetSpec, path: str, where: str | None = None) -> None:
    """Compact small files, and Z-ORDER where the config asks for it.

    Z-ORDER is how this platform handles high-cardinality filtering. Partitioning on a
    column like pu_location_id would create hundreds of tiny directories; Z-ordering
    gets most of the file skipping with none of that.

    ``where`` limits the work to some partitions. An incremental update passes the
    partitions it wrote to, so an update never rewrites data it did not touch.
    """
    from delta.tables import DeltaTable

    builder = DeltaTable.forPath(spark, path).optimize()
    if where:
        builder = builder.where(where)
    if target.zorder_by:
        builder.executeZOrderBy(*target.zorder_by)
    else:
        builder.executeCompaction()


def _sql_path(path: str) -> str:
    return path.replace("\\", "/")


def enable_change_data_feed(spark: SparkSession, path: str) -> None:
    """Turn on the Change Data Feed, so every later commit records the rows it changed.

    A metadata-only change: no data file is rewritten. It is what lets the analytical
    layer refresh from the rows an update inserted instead of rescanning the table.
    """
    from delta.tables import DeltaTable

    properties = DeltaTable.forPath(spark, path).detail().first()["properties"] or {}
    if properties.get("delta.enableChangeDataFeed") != "true":
        spark.sql(
            f"ALTER TABLE delta.`{_sql_path(path)}` "
            "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )


def table_version(spark: SparkSession, path: str) -> int:
    from delta.tables import DeltaTable

    return int(DeltaTable.forPath(spark, path).history(1).first()["version"])


def table_properties(spark: SparkSession, path: str) -> dict[str, str]:
    from delta.tables import DeltaTable

    return dict(DeltaTable.forPath(spark, path).detail().first()["properties"] or {})


def set_table_properties(spark: SparkSession, path: str, props: dict[str, str]) -> None:
    def literal(value: str) -> str:
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"

    pairs = ", ".join(f"{literal(k)} = {literal(v)}" for k, v in props.items())
    spark.sql(f"ALTER TABLE delta.`{_sql_path(path)}` SET TBLPROPERTIES ({pairs})")


def read_changes(spark: SparkSession, path: str, start: int, end: int) -> DataFrame:
    """The Change Data Feed of commits ``start`` to ``end``, both included.

    Raises if the feed does not cover that range, for instance because it was switched on
    later. Callers treat that as "the changes are unknown" and fall back to a full rebuild.
    """
    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start)
        .option("endingVersion", end)
        .load(path)
    )


# Operations that replace a table's contents wholesale. Their change feed lists every row
# as deleted and inserted again, so reading it costs more than rebuilding downstream.
_REPLACING_OPERATIONS = {"CREATE OR REPLACE TABLE AS SELECT", "REPLACE TABLE AS SELECT"}


def replaced_between(spark: SparkSession, path: str, start: int, end: int) -> str | None:
    """The first commit in ``start``..``end`` that overwrote the whole table, if any."""
    from delta.tables import DeltaTable

    history = (
        DeltaTable.forPath(spark, path).history()
        .filter((F.col("version") >= start) & (F.col("version") <= end))
        .orderBy("version")
        .collect()
    )
    for row in history:
        params = row["operationParameters"] or {}
        if row["operation"] in _REPLACING_OPERATIONS or (
            row["operation"] == "WRITE" and params.get("mode") == "Overwrite"
        ):
            return f"v{row['version']} {row['operation']}"
    return None


def merge_commit(spark: SparkSession, path: str, after_version: int):
    """The first MERGE committed after ``after_version``, or None if the merge committed nothing.

    Looked up by operation, not taken as the latest commit: auto-compaction can commit an
    OPTIMIZE straight after the merge, and that commit carries no merge metrics.
    """
    from delta.tables import DeltaTable

    return (
        DeltaTable.forPath(spark, path).history()
        .filter((F.col("version") > after_version) & (F.col("operation") == "MERGE"))
        .orderBy("version")
        .first()
    )


def partition_predicate(columns: list[str], rows) -> str | None:
    """A predicate matching the given partition values, for OPTIMIZE or replaceWhere."""
    if not rows:
        return None
    return " OR ".join(
        "(" + " AND ".join(f"`{c}` = '{row[c]}'" for c in columns) + ")" for row in rows
    )


def _inserted_partitions(spark: SparkSession, target: TargetSpec, path: str, version: int) -> str | None:
    """A predicate matching the partitions that commit ``version`` inserted rows into."""
    changes = read_changes(spark, path, version, version).filter(F.col("_change_type") == "insert")
    return partition_predicate(
        target.partition_by, changes.select(*target.partition_by).distinct().collect()
    )


def merge_delta(spark: SparkSession, df: DataFrame, target: TargetSpec, path: str) -> MergeResult:
    """Insert the rows whose key is not in the table yet. Leave every other row alone.

    There is no WHEN MATCHED clause, so a row already in the table is never rewritten:
    a re-delivered record is ignored and the stored one stays exactly as it was. That
    also makes re-applying a release a no-op.

    Schema evolution is additive only. autoMerge is switched on for this one statement
    so that a new source column is added to the table (existing rows read it as null),
    and switched back afterwards so no other write in the session can widen a table by
    accident. A change Delta cannot reconcile, such as a column changing type, still
    fails the merge, and the run is recorded as failed.
    """
    from delta.tables import DeltaTable

    if not target.merge_key:
        raise ValueError(f"merge into {target.table} needs a merge_key")
    if not DeltaTable.isDeltaTable(spark, path):
        raise ValueError(f"{path} is not a Delta table yet; run the full ingestion first")

    enable_change_data_feed(spark, path)
    table = DeltaTable.forPath(spark, path)
    before = int(table.history(1).first()["version"])

    # The partition columns join the key when they are not part of it: a duplicate is
    # necessarily in the same partition, and naming it lets the merge skip the others.
    columns = target.merge_key + [c for c in target.partition_by if c not in target.merge_key]
    condition = " AND ".join(f"t.`{c}` = s.`{c}`" for c in columns)

    previous = spark.conf.get(_AUTO_MERGE, "false")
    spark.conf.set(_AUTO_MERGE, "true")
    try:
        (
            table.alias("t")
            .merge(df.alias("s"), condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
    finally:
        spark.conf.set(_AUTO_MERGE, previous)

    commit = merge_commit(spark, path, before)
    if commit is None:
        # Delta commits nothing when every source row matched, as on a re-applied
        # release, so there are no metrics and the source is counted directly.
        return MergeResult(version=before, source_rows=df.count(), inserted=0, optimized_partitions=[])

    version = int(commit["version"])
    metrics = commit["operationMetrics"] or {}
    inserted = int(metrics.get("numTargetRowsInserted", 0))
    source_rows = int(metrics.get("numSourceRows", 0))

    optimized: list[str] = []
    if inserted:
        if target.partition_by:
            where = _inserted_partitions(spark, target, path, version)
            if where:
                optimize(spark, target, path, where)
                optimized = [where]
        else:
            optimize(spark, target, path)
    return MergeResult(version=version, source_rows=source_rows, inserted=inserted,
                       optimized_partitions=optimized)


def write_dataset(
    spark: SparkSession, df: DataFrame, cfg: DatasetConfig, path: str
) -> MergeResult | None:
    """Write one dataset's result. Returns what the merge did, or None for a full write."""
    if cfg.target.mode == "merge":
        return merge_delta(spark, df, cfg.target, path)
    write_delta(df, cfg.target, path)
    optimize(spark, cfg.target, path)
    return None
