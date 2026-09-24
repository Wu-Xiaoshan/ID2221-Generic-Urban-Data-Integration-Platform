"""Ingestion, validation and quarantine metadata tables.

One row per dataset per run, appended to _meta/ingestion_metadata. The schema is the
same for every dataset, so the table answers questions across the whole platform:
which datasets ran, how long they took, how many rows each rejected, and whether a
source schema changed.

_meta/validation_metrics sits one grain below it, one row per rule per run, and
_meta/quarantine one grain below that, one row per rejected record.
"""

from __future__ import annotations

import datetime as dt
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
        # Added in Week 3 for incremental updates. Rows written before then read them
        # as null. For a merge, records_written equals records_inserted.
        StructField("release", StringType(), True),
        StructField("records_inserted", LongType(), True),
        StructField("records_duplicate", LongType(), True),
        # Schema changes the dataset's policy did not accept. Recorded whether or not
        # they stopped the run, so "flag" is visible without reading the logs.
        StructField("schema_violations", StringType(), True),
    ]
)

# One row per rule per dataset per run. Kept apart from the run record because the grain differs:
# a run has one duration but twenty rules. Joining the two answers which dataset fails validation most often and which rule is responsible.
VALIDATION_METRICS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("release", StringType(), True),
        StructField("recorded_at", TimestampType(), False),
        StructField("rule_name", StringType(), False),
        StructField("rule_type", StringType(), False),
        StructField("on_fail", StringType(), False),
        StructField("status", StringType(), False),
        StructField("failed_records", LongType(), True),
        StructField("records_read", LongType(), True),
        StructField("note", StringType(), True),
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
    release: str | None = None
    records_inserted: int | None = None
    records_duplicate: int | None = None
    schema_violations: str | None = None
    rule_outcomes: list[Any] = field(default_factory=list)

    def set_schema_report(self, report: dict) -> None:
        self.schema_version = report["fingerprint"]
        self.schema_column_count = report["column_count"]
        ignored = ("fingerprint", "column_count", "violations")
        drift = {k: v for k, v in report.items() if k not in ignored and v}
        self.schema_drift = json.dumps(drift, separators=(",", ":")) if drift else None
        violations = report.get("violations")
        self.schema_violations = "; ".join(violations) if violations else None

    def to_row(self) -> tuple:
        data = asdict(self)
        data.pop("rule_outcomes")
        return tuple(data[f.name] for f in INGESTION_METADATA_SCHEMA.fields)


def write_metadata(spark: SparkSession, runs: list[IngestionRun], path: str) -> None:
    rows = [run.to_row() for run in runs]
    df = spark.createDataFrame(rows, schema=INGESTION_METADATA_SCHEMA)
    # mergeSchema lets a table written by an older version of this schema gain the new
    # columns on its next append, instead of refusing the write.
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def write_validation_metrics(spark: SparkSession, runs: list[IngestionRun], path: str) -> None:
    """Append one row per rule for every run that produced outcomes."""
    now = dt.datetime.now()
    rows = [
        (
            run.run_id,
            run.dataset_name,
            run.release,
            now,
            outcome.name,
            outcome.rule_type,
            outcome.on_fail,
            outcome.status,
            outcome.failed_records,
            run.records_read,
            outcome.note,
        )
        for run in runs
        for outcome in run.rule_outcomes
    ]
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=VALIDATION_METRICS_SCHEMA)
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)


def write_quarantine(quarantined: DataFrame, path: str) -> None:
    quarantined.write.format("delta").mode("append").partitionBy("dataset_name").save(path)
