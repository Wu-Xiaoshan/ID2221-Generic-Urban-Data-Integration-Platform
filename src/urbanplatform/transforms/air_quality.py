"""Dataset-specific rules for air_quality."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import DatasetConfig
from ..registry import post_quality, pre_quality


@pre_quality("air_quality")
def derive_site_id(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    """Compose the site identifier and the hourly join key.

    The site is identified by three zero-padded string columns, which is why the reader
    keeps them as strings: parsing them as integers loses the padding and merges
    distinct sites.
    """
    df = df.withColumn(
        "site_id", F.concat(F.col("state_code"), F.col("county_code"), F.col("site_num"))
    )
    return df.withColumn("obs_hour", F.date_trunc("hour", F.col("obs_ts")))


@post_quality("air_quality")
def aggregate_city_hourly(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    """Collapse individual monitor readings into one city-wide value per hour.

    This runs after the quality checks, and the order matters: the rules are written
    against per-reading columns (pm25_ugm3_raw, site_id, poc) that this aggregation
    removes. Checking a null or a negative measurement is only possible while the
    individual readings still exist.

    The mean is taken over readings rather than over sites. That is safe here because
    every NYC site runs a single instrument: measured across all 8,784 hours,
    reading_count equals site_count. Sites with two instruments do exist in the wider
    file (Erie county has one) but they fall outside the NYC filter. site_count and
    reading_count are both kept so the assumption stays visible, and a future extract
    that broke it would show up as the two columns diverging.

    A city-wide mean rather than a per-zone value is a deliberate limit. Six monitors
    serve 265 taxi zones, and Manhattan — where most pickups happen — has none, so
    assigning each trip its nearest monitor would look precise without being so.
    """
    return (
        df.groupBy("obs_hour")
        .agg(
            F.avg("pm25_ugm3_raw").alias("pm25_ugm3"),
            F.min("pm25_ugm3_raw").alias("pm25_min_ugm3"),
            F.max("pm25_ugm3_raw").alias("pm25_max_ugm3"),
            F.countDistinct("site_id").alias("site_count"),
            F.count(F.lit(1)).alias("reading_count"),
        )
        .withColumn("obs_date", F.to_date("obs_hour"))
    )
