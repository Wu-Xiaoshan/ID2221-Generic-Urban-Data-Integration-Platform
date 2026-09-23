"""
Defining the derived dimensions once, here, is what keeps the six queries readable and
consistent.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from ..config import PlatformConfig
from .config import AnalyticsConfig, bucket_case, family_case


def register_sources(spark: SparkSession, platform: PlatformConfig, cfg: AnalyticsConfig) -> None:
    for name, src in cfg.sources.items():
        path = platform.table_path(src["layer"], src["table"])
        spark.read.format("delta").load(path).createOrReplaceTempView(f"src_{name}")


def enriched_trips_sql(cfg: AnalyticsConfig, months: list[str] | None = None) -> str:
    """gold/integrated_taxi_trips + the derived dimensions the analyses share.

    ``months`` restricts the view to those pickup_year_month partitions. An incremental
    product refresh uses it to run a product's unchanged SQL over the changed months only.
    """
    tz = cfg.reporting_timezone
    where = ""
    if months:
        where = "WHERE pickup_year_month IN (" + ", ".join(f"'{m}'" for m in months) + ")"
    return f"""
    CREATE OR REPLACE TEMP VIEW trips AS
    SELECT
        *,
        HOUR(FROM_UTC_TIMESTAMP(pickup_ts, '{tz}'))  AS local_hour,
        DATE_FORMAT(pickup_date, 'EEEE')             AS day_of_week,
        DAYOFWEEK(pickup_date)                       AS day_of_week_num,
        CASE WHEN DAYOFWEEK(pickup_date) IN (1, 7) THEN 'weekend'
             ELSE 'weekday' END                      AS day_type,
        {family_case("weather_condition_code", cfg.weather_families)} AS weather_family,
        {bucket_case("weather_precipitation_mm", cfg.precip_buckets)} AS precip_bucket,
        {bucket_case("aq_pm25_ugm3", cfg.aq_quartiles)}               AS aq_quartile,
        {bucket_case("aq_pm25_ugm3", cfg.aq_epa_categories)}          AS aq_epa_category
    FROM src_integrated
    {where}
    """


def hourly_demand_sql(cfg: AnalyticsConfig) -> str:
    return """
    CREATE OR REPLACE TEMP VIEW hourly_demand AS
    WITH base AS (
        SELECT
            pickup_hour,
            COUNT(*)              AS trip_count,
            MAX(aq_pm25_ugm3)     AS pm25,
            MAX(aq_pm25_max_ugm3) AS pm25_max,
            MAX(local_hour)       AS local_hour,
            MAX(day_type)         AS day_type,
            MAX(aq_quartile)      AS aq_quartile,
            MAX(aq_epa_category)  AS aq_epa_category
        FROM trips
        WHERE NOT air_quality_is_missing
        GROUP BY pickup_hour
    )
    SELECT
        *,
        trip_count / AVG(trip_count) OVER (PARTITION BY local_hour, day_type)
            AS demand_index
    FROM base
    """


def register_views(
    spark: SparkSession,
    platform: PlatformConfig,
    cfg: AnalyticsConfig,
    months: list[str] | None = None,
) -> None:
    register_sources(spark, platform, cfg)
    spark.sql(enriched_trips_sql(cfg, months))
    spark.sql(hourly_demand_sql(cfg))
