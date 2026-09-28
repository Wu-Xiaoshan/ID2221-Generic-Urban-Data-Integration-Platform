# _meta/ml_metadata: one row per ML build step (dataset or features) per run.

from __future__ import annotations

import dataclasses
import datetime as dt
import json
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

ML_METADATA_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("task", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), True),
        StructField("duration_sec", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("source_version", LongType(), True),
        StructField("output_table", StringType(), True),
        StructField("output_version", LongType(), True),
        StructField("config_fingerprint", StringType(), True),
        StructField("rows_in", LongType(), True),
        StructField("rows_train", LongType(), True),
        StructField("rows_validation", LongType(), True),
        StructField("rows_test", LongType(), True),
        StructField("details", StringType(), True),
        StructField("error", StringType(), True),
    ]
)

PROP = "ml."


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


@dataclass
class MLRun:
    run_id: str
    task: str
    stage: str
    status: str = "running"
    started_at: Any = None
    finished_at: Any = None
    duration_sec: str | None = None
    source_table: str | None = None
    source_version: int | None = None
    output_table: str | None = None
    output_version: int | None = None
    config_fingerprint: str | None = None
    rows_in: int | None = None
    rows_train: int | None = None
    rows_validation: int | None = None
    rows_test: int | None = None
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def split_rows(self, counts: dict[str, int]) -> None:
        self.rows_train = counts.get("train", 0)
        self.rows_validation = counts.get("validation", 0)
        self.rows_test = counts.get("test", 0)

    def to_row(self) -> tuple:
        data = dataclasses.asdict(self)
        data["details"] = json.dumps(self.details, separators=(",", ":"), default=str) if self.details else None
        return tuple(data[f.name] for f in ML_METADATA_SCHEMA.fields)


def write_ml_metadata(spark: SparkSession, run: MLRun, path: str) -> None:
    df = spark.createDataFrame([run.to_row()], schema=ML_METADATA_SCHEMA)
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
