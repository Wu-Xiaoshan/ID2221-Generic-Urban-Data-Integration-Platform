"""Generic source reader and schema validation.

Dispatches on the declared format and applies the declared source filter during the
scan. Nothing here is dataset-aware.
"""

from __future__ import annotations

import glob
import hashlib
import json

from pyspark.sql import DataFrame, SparkSession

from .config import DatasetConfig


def read_source(spark: SparkSession, cfg: DatasetConfig) -> tuple[DataFrame, list[str]]:
    """Read the dataset's source files. Returns the frame and the files matched."""
    source = cfg.source
    files = sorted(glob.glob(source.abs_path))
    if not files:
        raise SystemExit(f"[{cfg.name}] no files matched {source.abs_path}")

    reader = spark.read.format(source.format)
    for key, value in source.options.items():
        reader = reader.option(key, str(value))
    df = reader.load(files)

    if source.filter:
        # Applied here rather than downstream so the predicate reaches the scan.
        df = df.filter(source.filter)

    return df, files


def schema_fingerprint(df: DataFrame) -> str:
    """A short, stable hash of the frame's column names and types.

    Recorded on every run so that a change in the source layout is visible in
    _meta/ingestion_metadata even when the pipeline itself did not fail.
    """
    payload = json.dumps(
        [[f.name, f.dataType.simpleString()] for f in df.schema.fields],
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


def validate_schema(df: DataFrame, cfg: DatasetConfig) -> dict:
    """Check the source layout against the config.

    Missing required columns abort the run; everything else is reported. New columns
    are expected over time (the taxi feed gained a fee column in each of 2015, 2019,
    2022 and 2025), so their appearance is recorded rather than treated as an error.
    """
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expected = cfg.expected_columns

    missing = [c for c in expected if c not in actual]
    unexpected = [c for c in actual if expected and c not in expected]
    type_mismatches = {
        c: {"expected": t, "actual": actual[c]}
        for c, t in expected.items()
        if c in actual and actual[c] != t
    }

    missing_required = [c for c in cfg.required_columns if c not in actual]
    if missing_required:
        raise SystemExit(
            f"[{cfg.name}] required column(s) missing from the source: "
            f"{', '.join(missing_required)}"
        )

    return {
        "fingerprint": schema_fingerprint(df),
        "column_count": len(actual),
        "missing_columns": missing,
        "unexpected_columns": unexpected,
        "type_mismatches": type_mismatches,
    }
