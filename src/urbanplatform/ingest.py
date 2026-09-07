"""The ingestion pipeline.

One function runs every dataset. It receives a DatasetConfig and never branches on
which dataset it is; the only dataset-specific behaviour reaches it through the
registry in ``transforms/``.

    read -> validate schema -> standardize -> pre-quality transform
         -> quality checks -> post-quality transform -> write Delta -> record metadata
"""

from __future__ import annotations

import datetime as dt
import os
import time
import traceback
import uuid

from pyspark.sql import DataFrame, SparkSession

from . import transforms  # noqa: F401  (importing registers the dataset rules)
from .config import DatasetConfig, PlatformConfig
from .metadata import IngestionRun, write_metadata, write_quarantine
from .quality import apply_rules, to_quarantine
from .reader import read_source, validate_schema
from .registry import apply_post_quality, apply_pre_quality, registered
from .standardize import standardize
from .writer import write_dataset


def ingest_dataset(
    spark: SparkSession,
    platform: PlatformConfig,
    cfg: DatasetConfig,
    run_id: str,
) -> tuple[IngestionRun, DataFrame | None]:
    """Ingest one dataset. Returns its run record and any quarantined rows."""
    run = IngestionRun(
        run_id=run_id,
        dataset_name=cfg.name,
        started_at=dt.datetime.now(),
        source_format=cfg.source.format,
        source_filter=cfg.source.filter,
        write_mode=cfg.target.mode,
        partition_by=",".join(cfg.target.partition_by) or None,
    )
    started = time.perf_counter()
    quarantined = None

    try:
        target_path = platform.table_path(cfg.target.layer, cfg.target.table)
        run.target_path = target_path

        raw, files = read_source(spark, cfg)
        run.source_files = ";".join(os.path.basename(f) for f in files)

        run.set_schema_report(validate_schema(raw, cfg))

        # Cached because the row count and the write both consume it, and for
        # air_quality re-reading means re-scanning 2.2 GB.
        df = apply_pre_quality(standardize(raw, cfg), cfg).cache()
        run.records_read = df.count()

        clean, rejected = apply_rules(df, cfg)
        run.records_rejected = rejected.count()
        if run.records_rejected:
            quarantined = to_quarantine(rejected, cfg, run_id)

        result = apply_post_quality(clean, cfg)
        write_dataset(spark, result, cfg, target_path)
        run.records_written = spark.read.format("delta").load(target_path).count()

        df.unpersist()
        run.status = "success"

    except Exception as exc:  # noqa: BLE001 - the run record must survive any failure
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()

    run.finished_at = dt.datetime.now()
    run.duration_sec = f"{time.perf_counter() - started:.2f}"
    return run, quarantined


def ingest_all(
    spark: SparkSession,
    platform: PlatformConfig,
    configs: list[DatasetConfig],
) -> list[IngestionRun]:
    """Ingest every configured dataset, then write the metadata for the whole run."""
    run_id = uuid.uuid4().hex[:12]
    print(f"run {run_id}: {len(configs)} dataset(s)\n")

    runs: list[IngestionRun] = []
    quarantines: list[DataFrame] = []

    for cfg in configs:
        stages = registered(cfg.name) or ["none"]
        print(f"--- {cfg.name}  (source: {cfg.source.format}, "
              f"dataset-specific stages: {', '.join(stages)})")
        run, quarantined = ingest_dataset(spark, platform, cfg, run_id)
        runs.append(run)
        if quarantined is not None:
            quarantines.append(quarantined)

        if run.status == "success":
            print(f"    read {run.records_read:,}  ->  written {run.records_written:,}  "
                  f"(rejected {run.records_rejected:,})  in {run.duration_sec}s")
            if run.schema_drift:
                print(f"    schema drift: {run.schema_drift}")
        else:
            print(f"    FAILED: {run.error}")

    if quarantines:
        combined = quarantines[0]
        for extra in quarantines[1:]:
            combined = combined.unionByName(extra)
        write_quarantine(combined, platform.meta_path("quarantine"))

    write_metadata(spark, runs, platform.meta_path("ingestion_metadata"))
    return runs
