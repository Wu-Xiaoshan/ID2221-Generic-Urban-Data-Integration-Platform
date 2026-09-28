"""Every pipeline in the platform already writes a run record, but each writes its own
shape: ingestion counts records, integration counts trips, a data product counts rows.
This module puts one view over all of them, ``pipeline_runs``, so a question about the
platform is one query rather than three.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession

from .config import REPO_ROOT, PlatformConfig

META_TABLES = {
    "ingestion_metadata": "ingestion_metadata",
    "validation_metrics": "validation_metrics",
    "quarantine": "quarantine",
    "integration_metadata": "integration_metadata",
    "data_product_metadata": "data_product_metadata",
    "ml_metadata": "ml_metadata",
}

# Columns the run tables gained after Week 1. A table whose last write predates them
# does not have them at all, and a query naming one would fail rather than return
# nulls, so they are filled in here. Only late additions need an entry.
_LATE_COLUMNS = {
    "ingestion_metadata": {
        "release": "string",
        "records_inserted": "bigint",
        "records_duplicate": "bigint",
        "schema_violations": "string",
    },
    "integration_metadata": {
        "mode": "string",
        "note": "string",
        "rows_inserted": "bigint",
        "rows_updated": "bigint",
        "rows_deleted": "bigint",
        "source_versions": "string",
    },
    "data_product_metadata": {
        "refresh_mode": "string",
        "refreshed_partitions": "string",
        "note": "string",
        "started_at": "timestamp",
        "finished_at": "timestamp",
    },
}

# Each pipeline's run table, mapped onto the shared column list. duration_sec is stored
# as a string everywhere (it is formatted at write time), so it is cast here.
#
# mode separates runs that did different amounts of work: a full load or rebuild, an
# incremental update or refresh, and a refresh that found nothing to do. Their durations
# are only comparable within one mode.
_PIPELINE_RUN_PARTS = {
    "ingestion_metadata": """
        SELECT run_id,
               CASE WHEN release IS NULL THEN 'ingest' ELSE 'update' END AS pipeline,
               CASE WHEN release IS NULL THEN 'full' ELSE 'incremental' END AS mode,
               dataset_name AS entity,
               status,
               started_at,
               finished_at,
               CAST(duration_sec AS DOUBLE) AS duration_sec,
               records_read,
               records_written,
               records_rejected,
               schema_version,
               release
        FROM ingestion_metadata
    """,
    "integration_metadata": """
        SELECT run_id,
               'integrate' AS pipeline,
               COALESCE(mode, 'full') AS mode,
               target_table AS entity,
               status,
               started_at,
               finished_at,
               CAST(duration_sec AS DOUBLE) AS duration_sec,
               trips_in AS records_read,
               trips_out AS records_written,
               CAST(NULL AS BIGINT) AS records_rejected,
               CAST(NULL AS STRING) AS schema_version,
               CAST(NULL AS STRING) AS release
        FROM integration_metadata
    """,
    "data_product_metadata": """
        SELECT run_id,
               'product' AS pipeline,
               COALESCE(refresh_mode, 'full') AS mode,
               product_name AS entity,
               status,
               -- Rows written before run times were recorded fall back to refreshed_at: the
               -- run's time for a refresh, but an earlier time for a skipped run.
               COALESCE(started_at, refreshed_at) AS started_at,
               COALESCE(finished_at, refreshed_at) AS finished_at,
               CAST(duration_sec AS DOUBLE) AS duration_sec,
               CAST(NULL AS BIGINT) AS records_read,
               row_count AS records_written,
               CAST(NULL AS BIGINT) AS records_rejected,
               schema_version,
               CAST(NULL AS STRING) AS release
        FROM data_product_metadata
    """,
    # Week 4. Both ML steps rebuild their table in full; the configuration fingerprint takes
    # the place of a schema version.
    "ml_metadata": """
        SELECT run_id,
               CONCAT('ml_', stage) AS pipeline,
               'full' AS mode,
               output_table AS entity,
               status,
               started_at,
               finished_at,
               CAST(duration_sec AS DOUBLE) AS duration_sec,
               rows_in AS records_read,
               rows_train + rows_validation + rows_test AS records_written,
               CAST(NULL AS BIGINT) AS records_rejected,
               config_fingerprint AS schema_version,
               CAST(NULL AS STRING) AS release
        FROM ml_metadata
    """,
}


@dataclass
class MonitoringQuery:
    name: str
    question: str
    sql: str
    requires: list[str] = field(default_factory=list)


@dataclass
class QueryResult:
    query: MonitoringQuery
    columns: list[str] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    skipped: str | None = None
    error: str | None = None


def load_queries(path: str | None = None) -> list[MonitoringQuery]:
    path = path or os.path.join(REPO_ROOT, "conf", "monitoring.yaml")
    with open(path, "r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    return [
        MonitoringQuery(
            name=q["name"],
            question=q.get("question", ""),
            sql=q["sql"].strip(),
            requires=list(q.get("requires", []) or []),
        )
        for q in raw.get("queries", [])
    ]


def _conform(df: DataFrame, view: str) -> DataFrame:
    from pyspark.sql import functions as F

    missing = {
        column: sql_type
        for column, sql_type in _LATE_COLUMNS.get(view, {}).items()
        if column not in df.columns
    }
    for column, sql_type in missing.items():
        df = df.withColumn(column, F.lit(None).cast(sql_type))
    return df


def register_views(spark: SparkSession, platform: PlatformConfig) -> list[str]:
    """Register a temp view per _meta table that exists, plus pipeline_runs."""
    available = []
    for view, table in META_TABLES.items():
        path = platform.meta_path(table)
        if not os.path.isdir(path):
            continue
        _conform(spark.read.format("delta").load(path), view).createOrReplaceTempView(view)
        available.append(view)

    parts = [sql for view, sql in _PIPELINE_RUN_PARTS.items() if view in available]
    if parts:
        spark.sql(" UNION ALL ".join(parts)).createOrReplaceTempView("pipeline_runs")
        available.append("pipeline_runs")
    return available


def run_queries(
    spark: SparkSession, queries: list[MonitoringQuery], available: list[str], limit: int = 40
) -> list[QueryResult]:
    results = []
    for query in queries:
        missing = [v for v in query.requires if v not in available]
        if missing:
            results.append(
                QueryResult(query=query, skipped=f"needs {', '.join(missing)}, not written yet")
            )
            continue
        try:
            df: DataFrame = spark.sql(query.sql)
            rows = [tuple(r) for r in df.limit(limit).collect()]
            results.append(QueryResult(query=query, columns=df.columns, rows=rows))
        except Exception as exc:  # noqa: BLE001 - one bad query must not stop the report
            results.append(QueryResult(query=query, error=f"{type(exc).__name__}: {exc}"))
    return results


def _cell(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def format_table(columns: list[str], rows: list[tuple]) -> str:
    """A fixed-width table for the console."""
    if not rows:
        return "    (no rows)"
    cells = [[_cell(v) for v in row] for row in rows]
    widths = [
        max(len(columns[i]), max(len(row[i]) for row in cells)) for i in range(len(columns))
    ]
    header = "  ".join(c.ljust(w) for c, w in zip(columns, widths))
    rule = "  ".join("-" * w for w in widths)
    body = "\n".join("  ".join(c.ljust(w) for c, w in zip(row, widths)) for row in cells)
    return f"    {header}\n    {rule}\n" + "\n".join("    " + line for line in body.split("\n"))


def render_markdown(results: list[QueryResult]) -> str:
    lines = ["# Platform monitoring report", ""]
    for result in results:
        lines.append(f"## {result.query.name}")
        if result.query.question:
            lines.append("")
            lines.append(f"_{result.query.question}_")
        lines.append("")
        if result.skipped:
            lines.append(f"Skipped: {result.skipped}")
        elif result.error:
            lines.append(f"Failed: {result.error}")
        elif not result.rows:
            lines.append("No rows.")
        else:
            lines.append("| " + " | ".join(result.columns) + " |")
            lines.append("|" + "|".join(["---"] * len(result.columns)) + "|")
            for row in result.rows:
                lines.append("| " + " | ".join(_cell(v) for v in row) + " |")
        lines.append("")
    return "\n".join(lines)
