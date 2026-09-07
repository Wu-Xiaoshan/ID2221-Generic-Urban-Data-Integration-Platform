"""The common data model (Task 4), implemented as generic, config-driven steps.

Column naming is mechanical rather than a per-column list, because the sources are
inconsistent in ways a hand-written list would not survive: ``Airport_fee`` is the only
capitalised column of 19 in the taxi feed, and the EPA columns are space-separated
title case. One rule handles all of them.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import DatasetConfig, TimestampSpec

_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")
_CAMEL_BOUNDARY = re.compile(r"(.)([A-Z][a-z]+)")
_LOWER_UPPER = re.compile(r"([a-z0-9])([A-Z])")


def to_snake_case(name: str) -> str:
    """``PULocationID`` -> ``pu_location_id``, ``Date GMT`` -> ``date_gmt``."""
    name = _NON_ALNUM.sub("_", name.strip())
    name = _CAMEL_BOUNDARY.sub(r"\1_\2", name)
    name = _LOWER_UPPER.sub(r"\1_\2", name)
    return re.sub(r"_+", "_", name).strip("_").lower()


def standardize_names(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    """Apply the explicit rename map, then snake_case whatever it did not cover.

    The rename map is for columns whose *name* needs a semantic change (``wspd`` ->
    ``wind_speed_kmh``). Everything else is handled by the mechanical rule, so a new
    upstream column arrives correctly named without anyone editing the config.
    """
    for source, target in cfg.rename.items():
        if source in df.columns:
            df = df.withColumnRenamed(source, target)

    renamed_targets = set(cfg.rename.values())
    for column in df.columns:
        if column in renamed_targets:
            continue
        snake = to_snake_case(column)
        if snake != column:
            df = df.withColumnRenamed(column, snake)
    return df


def _build_timestamp(spec: TimestampSpec):
    """Build one timestamp column expression from whichever source form is declared."""
    if spec.from_column:
        column = F.col(spec.from_column)
        expr = F.to_timestamp(column, spec.format) if spec.format else column.cast("timestamp")

    elif spec.from_parts:
        parts = spec.from_parts
        expr = F.make_timestamp(
            F.col(parts["year"]).cast("int"),
            F.col(parts["month"]).cast("int"),
            F.col(parts["day"]).cast("int"),
            F.col(parts.get("hour", F.lit(0))).cast("int") if "hour" in parts else F.lit(0),
            F.lit(int(parts.get("minute_default", 0))),
            F.lit(int(parts.get("second_default", 0))).cast("decimal(16,6)"),
        )

    else:
        joined = F.concat_ws(" ", *[F.col(c) for c in spec.from_concat])
        expr = F.to_timestamp(joined, spec.format) if spec.format else F.to_timestamp(joined)

    # tz names the timezone the source values are written in. UTC means they are
    # already absolute; anything else is a wall clock that has to be converted.
    if spec.tz.upper() != "UTC":
        expr = F.to_utc_timestamp(expr, spec.tz)
    return expr


def normalize_timestamps(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    """Derive every declared timestamp column, all of them UTC.

    The three sources encode time three different ways — one timestamp column, four
    integer columns, and a date column plus a time column — and two of them are not in
    UTC. Declaring the shape in YAML keeps that variety out of the code.
    """
    for spec in cfg.timestamps:
        df = df.withColumn(spec.target, _build_timestamp(spec))
    return df


def apply_casts(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    for column, dtype in cfg.cast.items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(dtype))
    return df


def drop_columns(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    present = [c for c in cfg.drop if c in df.columns]
    return df.drop(*present) if present else df


def standardize(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    """Names, then timestamps, then types, then drops.

    Timestamps and casts reference the *standardized* names, so the config only ever
    uses one naming space.
    """
    df = standardize_names(df, cfg)
    df = normalize_timestamps(df, cfg)
    df = apply_casts(df, cfg)
    return drop_columns(df, cfg)
