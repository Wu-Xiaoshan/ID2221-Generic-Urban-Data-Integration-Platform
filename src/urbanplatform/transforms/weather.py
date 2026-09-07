"""Dataset-specific rules for weather."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import DatasetConfig
from ..registry import pre_quality

# Values sourced from dwd_mosmix are numerical forecast-model output, not observations.
# Every other source value is a real measurement.
_MODELLED_SOURCE = "dwd_mosmix"

# Measurements that keep a provenance flag. coco is 98% modelled and cldc 44%, while the
# other four are essentially fully observed — but the mix varies with station outages,
# so the flag is derived per row rather than assumed per column.
_FLAGGED = {
    "temp_c": "temp_source",
    "humidity_pct": "rhum_source",
    "precipitation_mm": "prcp_source",
    "wind_speed_kmh": "wspd_source",
    "pressure_hpa": "pres_source",
    "cloud_cover_oktas": "cldc_source",
    "condition_code": "coco_source",
}


@pre_quality("weather")
def derive_provenance_flags(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    """Turn the ``*_source`` columns into ``*_is_modelled`` booleans, then drop them.

    Nothing in the values themselves distinguishes a measurement from a model estimate,
    so without this the two would be indistinguishable downstream. Any analysis that
    correlates taxi demand with 'weather conditions' can then exclude the columns that
    are not observations.
    """
    for measurement, source_column in _FLAGGED.items():
        if measurement in df.columns and source_column in df.columns:
            df = df.withColumn(
                f"{measurement}_is_modelled",
                F.coalesce(F.col(source_column) == F.lit(_MODELLED_SOURCE), F.lit(False)),
            )

    df = df.withColumn("obs_hour", F.date_trunc("hour", F.col("obs_ts")))
    df = df.withColumn("obs_date", F.to_date("obs_ts"))

    source_columns = [c for c in df.columns if c.endswith("_source")]
    return df.drop(*source_columns) if source_columns else df
