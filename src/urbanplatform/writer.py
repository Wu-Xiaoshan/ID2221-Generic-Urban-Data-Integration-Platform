"""Delta writer: partitioning, write mode, and post-write maintenance."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from .config import DatasetConfig, TargetSpec


def write_delta(df: DataFrame, target: TargetSpec, path: str) -> None:
    writer = df.write.format("delta").mode(target.mode)
    if target.partition_by:
        writer = writer.partitionBy(*target.partition_by)
    if target.replace_where:
        writer = writer.option("replaceWhere", target.replace_where)
    writer.save(path)


def optimize(spark: SparkSession, target: TargetSpec, path: str) -> None:
    """Compact small files, and Z-ORDER where the config asks for it.

    Z-ORDER is how this platform handles high-cardinality filtering. Partitioning on a
    column like pu_location_id would create hundreds of tiny directories; Z-ordering
    gets most of the file skipping with none of that.
    """
    from delta.tables import DeltaTable

    table = DeltaTable.forPath(spark, path)
    if target.zorder_by:
        table.optimize().executeZOrderBy(*target.zorder_by)
    else:
        table.optimize().executeCompaction()


def write_dataset(spark: SparkSession, df: DataFrame, cfg: DatasetConfig, path: str) -> None:
    write_delta(df, cfg.target, path)
    optimize(spark, cfg.target, path)
