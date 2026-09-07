"""Ingestion metadata and quarantine tables.

One row per dataset per run, appended to _meta/ingestion_metadata. The schema is the
same for every dataset, so the table answers questions across the whole platform:
which datasets ran, how long they took, how many rows each rejected, and whether a
source schema changed.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

INGESTION_METADATA_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), True),
        StructField("duration_sec", StringType(), True),
        StructField("source_format", StringType(), True),
        StructField("source_files", StringType(), True),
        StructField("source_filter", StringType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_written", LongType(), True),
        StructField("records_rejected", LongType(), True),
        StructField("schema_version", StringType(), True),
        StructField("schema_column_count", IntegerType(), True),
        StructField("schema_drift", StringType(), True),
        StructField("target_path", StringType(), True),
        StructField("write_mode", StringType(), True),
        StructField("partition_by", StringType(), True),
        StructField("error", StringType(), True),
    ]
)


@dataclass
class IngestionRun:
    """Accumulates the statistics for one dataset's run."""

    run_id: str
    dataset_name: str
    status: str = "running"
    started_at: Any = None
    finished_at: Any = None
    duration_sec: str | None = None
    source_format: str | None = None
    source_files: str | None = None
    source_filter: str | None = None
    records_read: int | None = None
    records_written: int | None = None
    records_rejected: int | None = None
    schema_version: str | None = None
    schema_column_count: int | None = None
    schema_drift: str | None = None
    target_path: str | None = None
    write_mode: str | None = None
    partition_by: str | None = None
    error: str | None = None
    rejections_by_rule: dict[str, int] = field(default_factory=dict)

    def set_schema_report(self, report: dict) -> None:
        self.schema_version = report["fingerprint"]
        self.schema_column_count = report["column_count"]
        drift = {k: v for k, v in report.items() if k not in ("fingerprint", "column_count") and v}
        self.schema_drift = json.dumps(drift, separators=(",", ":")) if drift else None

    def to_row(self) -> tuple:
        data = asdict(self)
        data.pop("rejections_by_rule")
        return tuple(data[f.name] for f in INGESTION_METADATA_SCHEMA.fields)


def write_metadata(spark: SparkSession, runs: list[IngestionRun], path: str) -> None:
    rows = [run.to_row() for run in runs]
    df = spark.createDataFrame(rows, schema=INGESTION_METADATA_SCHEMA)
    df.write.format("delta").mode("append").save(path)


def write_quarantine(quarantined: DataFrame, path: str) -> None:
    quarantined.write.format("delta").mode("append").partitionBy("dataset_name").save(path)
