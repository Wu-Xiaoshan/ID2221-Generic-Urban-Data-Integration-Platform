"""The ingestion pipeline.

One function runs every dataset. It receives a DatasetConfig and never branches on
which dataset it is; the only dataset-specific behaviour reaches it through the
registry in ``transforms/``.

    read -> validate schema -> standardize -> pre-quality transform
         -> quality checks -> post-quality transform -> write Delta -> record metadata

An incremental update runs the same function. ``DatasetConfig.for_release`` points the
config at the release's files and sets the write mode to merge; nothing else differs.
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
from .metadata import (
    IngestionRun,
    write_metadata,
    write_quarantine,
    write_validation_metrics,
)
from .quality import apply_rules, rule_outcomes, to_quarantine
from .reader import SchemaViolation, read_source, validate_schema
from .registry import apply_post_quality, apply_pre_quality, registered
from .rules import RuleContext
from .standardize import standardize
from .writer import write_dataset


def ingest_dataset(
    spark: SparkSession,
    platform: PlatformConfig,
    cfg: DatasetConfig,
    run_id: str,
    release: str | None = None,
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
        release=release,
    )
    started = time.perf_counter()
    quarantined = None

    try:
        target_path = platform.table_path(cfg.target.layer, cfg.target.table)
        run.target_path = target_path

        raw, files = read_source(spark, cfg)
        run.source_files = ";".join(os.path.basename(f) for f in files)

        try:
            run.set_schema_report(validate_schema(raw, cfg))
        except SchemaViolation as exc:
            run.set_schema_report(exc.report)
            raise

        # Cached because the row count and the write both consume it, and for
        # air_quality re-reading means re-scanning 2.2 GB.
        df = apply_pre_quality(standardize(raw, cfg), cfg).cache()
        run.records_read = df.count()

        if platform.validation_enabled:
            ctx = RuleContext(spark=spark, platform=platform, cfg=cfg)
            validated = apply_rules(df, cfg, ctx)
            rejected = validated.rejected
            if platform.validation_metrics_enabled:
                # The row count and the per-rule counts are two passes over the same rows; without metrics there is only one, and no reason to cache.
                rejected = rejected.cache()
            run.records_rejected = rejected.count()
            run.rule_outcomes = rule_outcomes(
                validated, cfg, count=platform.validation_metrics_enabled
            )
            for skipped, reason in validated.skipped.items():
                print(f"    rule '{skipped}' skipped: {reason}")
            if run.records_rejected:
                # Materialised now, while the rejected rows are still cached. The
                # quarantine is written after every dataset has run, and by then the
                # source frame is no longer cached, so a lazy frame would recompute it.
                quarantined = to_quarantine(rejected, cfg, run_id).cache()
                quarantined.count()
            rejected.unpersist()
            clean = validated.clean
        else:
            clean, run.records_rejected = df, 0

        result = apply_post_quality(clean, cfg)
        merged = write_dataset(spark, result, cfg, target_path)
        if merged is None:
            run.records_written = spark.read.format("delta").load(target_path).count()
        else:
            run.records_written = run.records_inserted = merged.inserted
            run.records_duplicate = merged.duplicates

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
    release: str | None = None,
) -> list[IngestionRun]:
    """Ingest every configured dataset, then write the metadata for the whole run.

    ``release`` names the incremental release being applied, if any. It is recorded on
    every run row so updates can be told apart from full loads.
    """
    run_id = uuid.uuid4().hex[:12]
    label = f" (release {release})" if release else ""
    print(f"run {run_id}{label}: {len(configs)} dataset(s)\n")

    runs: list[IngestionRun] = []
    quarantines: list[DataFrame] = []

    for cfg in configs:
        stages = registered(cfg.name) or ["none"]
        print(f"--- {cfg.name}  (source: {cfg.source.format}, "
              f"dataset-specific stages: {', '.join(stages)})")
        run, quarantined = ingest_dataset(spark, platform, cfg, run_id, release)
        runs.append(run)
        if quarantined is not None:
            quarantines.append(quarantined)

        if run.status == "success":
            if run.records_inserted is None:
                outcome = f"written {run.records_written:,}"
            else:
                outcome = (f"inserted {run.records_inserted:,}, "
                           f"duplicates ignored {run.records_duplicate:,}")
            print(f"    read {run.records_read:,}  ->  {outcome}  "
                  f"(rejected {run.records_rejected:,})  in {run.duration_sec}s")
            if run.schema_drift:
                print(f"    schema drift: {run.schema_drift}")
        else:
            print(f"    FAILED: {run.error}")
        if run.schema_violations:
            print(f"    schema policy: {run.schema_violations}")

    if quarantines:
        combined = quarantines[0]
        for extra in quarantines[1:]:
            combined = combined.unionByName(extra)
        write_quarantine(combined, platform.meta_path("quarantine"))
        for quarantined in quarantines:
            quarantined.unpersist()

    if platform.monitoring_enabled:
        write_metadata(spark, runs, platform.meta_path("ingestion_metadata"))
        write_validation_metrics(spark, runs, platform.meta_path("validation_metrics"))
    return runs
