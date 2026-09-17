"""The analytical data products: what each one computes.

Each product is one Spark SQL statement over the views in views.py, so it shares the derived
dimensions, sentinels and thresholds of the analytical queries instead of redefining them.
Writing the tables and their metadata is materialize.py's job.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field

import yaml

from ..config import REPO_ROOT
from .config import AnalyticsConfig
from .queries import build_queries


@dataclass(frozen=True)
class ProductSpec:
    name: str
    description: str
    grain: str
    audience: str
    schema_version: str
    key: list[str]
    partition_by: list[str] = field(default_factory=list)
    zorder_by: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ProductsConfig:
    source_layer: str
    source_table: str
    layer: str
    products: list[ProductSpec]
    thresholds: dict[str, float] = field(default_factory=dict)

    @staticmethod
    def load(path: str | None = None) -> ProductsConfig:
        path = path or os.path.join(REPO_ROOT, "conf", "products.yaml")
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        products = []
        for name, spec in raw["products"].items():
            if name not in SQL_BUILDERS:
                raise ValueError(
                    f"product '{name}' in conf/products.yaml has no SQL in products.py; "
                    f"known: {', '.join(SQL_BUILDERS)}"
                )
            products.append(
                ProductSpec(
                    name=name,
                    description=spec["description"],
                    grain=spec["grain"],
                    audience=spec["audience"],
                    schema_version=str(spec["schema_version"]),
                    key=list(spec["key"]),
                    partition_by=list(spec.get("partition_by") or []),
                    zorder_by=list(spec.get("zorder_by") or []),
                )
            )
        return ProductsConfig(
            source_layer=raw["source"]["layer"],
            source_table=raw["source"]["table"],
            layer=raw["layer"],
            products=products,
            thresholds={str(k): float(v) for k, v in (raw.get("thresholds") or {}).items()},
        )

    def threshold(self, name: str) -> float:
        try:
            return self.thresholds[name]
        except KeyError:
            raise KeyError(f"threshold '{name}' is not in conf/products.yaml") from None

    def get(self, name: str) -> ProductSpec:
        for spec in self.products:
            if spec.name == name:
                return spec
        raise KeyError(f"unknown product '{name}'; known: {', '.join(p.name for p in self.products)}")


# Trip measures shared by the products. Distance follows Q2's exclusions: zero-distance trips
# and distances above the plausibility cap. Speed is total distance over total time of the
# trips whose own speed is plausible, so a few seconds-long trips cannot dominate it the way
# they dominate a mean of per-trip speeds.
def _trip_profile(cfg: AnalyticsConfig, products: ProductsConfig) -> str:
    cap = cfg.threshold("max_plausible_distance_mi")
    max_speed = products.threshold("max_plausible_speed_mph")
    distance_ok = f"NOT dq_zero_distance AND trip_distance_mi <= {cap}"
    speed_ok = (f"{distance_ok} AND trip_duration_min > 0 "
                f"AND trip_distance_mi / (trip_duration_min / 60.0) <= {max_speed}")
    return f"""
           ROUND(AVG(CASE WHEN {distance_ok} THEN trip_distance_mi END), 3)          AS avg_distance_mi,
           ROUND(AVG(CASE WHEN trip_duration_min > 0 THEN trip_duration_min END), 2) AS avg_duration_min,
           ROUND(60.0 * SUM(CASE WHEN {speed_ok} THEN trip_distance_mi END)
                      / SUM(CASE WHEN {speed_ok} THEN trip_duration_min END), 2)    AS avg_speed_mph,
           ROUND(AVG(CASE WHEN NOT dq_negative_total THEN total_amount END), 2)      AS avg_total_amount_usd"""


def daily_mobility_summary_sql(cfg: AnalyticsConfig, products: ProductsConfig) -> str:
    # Weather and PM2.5 are hourly, city-wide values repeated on every trip. They are reduced
    # to one row per hour first, so a busy hour does not weigh more in the daily average.
    # passenger_count is left out: it is null on 7.87% of trips, so a daily sum would undercount.
    return f"""
    WITH trip_days AS (
        SELECT pickup_year_month,
               pickup_date,
               pickup_borough,
               COUNT(*)                     AS trip_count,
               ROUND(SUM(total_amount), 2)  AS total_amount_usd,
               {_trip_profile(cfg, products)}
        FROM trips
        WHERE pu_location_id NOT IN ({cfg.sentinels})
        GROUP BY pickup_year_month, pickup_date, pickup_borough
    ),
    context_hours AS (
        SELECT pickup_hour,
               MAX(pickup_date)                                                    AS pickup_date,
               MAX(CASE WHEN NOT weather_is_missing THEN weather_temp_c END)           AS temp_c,
               MAX(CASE WHEN NOT weather_is_missing THEN weather_precipitation_mm END) AS precipitation_mm,
               MAX(CASE WHEN NOT weather_is_missing THEN weather_family END)           AS weather_family,
               MAX(CASE WHEN NOT air_quality_is_missing THEN aq_pm25_ugm3 END)         AS pm25_ugm3
        FROM trips
        GROUP BY pickup_hour
    ),
    family_hours AS (
        SELECT pickup_date, weather_family, COUNT(*) AS hours
        FROM context_hours
        WHERE weather_family IS NOT NULL
        GROUP BY pickup_date, weather_family
    ),
    context_days AS (
        SELECT c.pickup_date,
               COUNT(c.temp_c)                  AS weather_hours,
               ROUND(AVG(c.temp_c), 1)          AS avg_temp_c,
               ROUND(SUM(c.precipitation_mm), 1) AS precipitation_mm,
               ROUND(AVG(c.pm25_ugm3), 2)       AS avg_pm25_ugm3,
               -- the family with most hours; ties go to the later name, so reruns agree
               MAX(STRUCT(f.hours, f.weather_family)).weather_family AS dominant_weather_family
        FROM context_hours c
        LEFT JOIN family_hours f ON c.pickup_date = f.pickup_date
                                AND c.weather_family = f.weather_family
        GROUP BY c.pickup_date
    )
    SELECT t.pickup_year_month,
           t.pickup_date,
           DATE_FORMAT(t.pickup_date, 'EEEE') AS day_of_week,
           t.pickup_borough,
           t.trip_count,
           ROUND(100.0 * t.trip_count / SUM(t.trip_count) OVER (PARTITION BY t.pickup_date), 2)
               AS share_of_city_pct,
           t.total_amount_usd,
           t.avg_total_amount_usd,
           t.avg_distance_mi,
           t.avg_duration_min,
           t.avg_speed_mph,
           c.weather_hours,
           c.avg_temp_c,
           c.precipitation_mm,
           c.dominant_weather_family,
           c.avg_pm25_ugm3
    FROM trip_days t
    LEFT JOIN context_days c ON t.pickup_date = c.pickup_date
    """


def zone_monthly_statistics_sql(cfg: AnalyticsConfig, products: ProductsConfig) -> str:
    # Q1 as it stands, so the product and the analytical query cannot drift apart, extended
    # with the trip profile and the busiest local hour of each zone and month.
    q1 = {q.name: q.sql for q in build_queries(cfg)}["q1_zone_monthly_demand"]
    return f"""
    WITH demand AS ({q1}),
    profile AS (
        SELECT pickup_year_month,
               pu_location_id,
               ROUND(100.0 * AVG(CASE WHEN day_type = 'weekend' THEN 1 ELSE 0 END), 2)
                   AS weekend_share_pct,
               {_trip_profile(cfg, products)}
        FROM trips
        WHERE pu_location_id NOT IN ({cfg.sentinels})
        GROUP BY pickup_year_month, pu_location_id
    ),
    hourly AS (
        SELECT pickup_year_month, pu_location_id, local_hour, COUNT(*) AS trips
        FROM trips
        WHERE pu_location_id NOT IN ({cfg.sentinels})
        GROUP BY pickup_year_month, pu_location_id, local_hour
    ),
    peak AS (
        -- ties go to the later hour, so reruns agree
        SELECT pickup_year_month,
               pu_location_id,
               MAX(STRUCT(trips, local_hour)).local_hour AS peak_local_hour
        FROM hourly
        GROUP BY pickup_year_month, pu_location_id
    )
    SELECT d.pickup_year_month,
           d.pu_location_id,
           d.pickup_zone,
           d.pickup_borough,
           d.trip_count,
           d.dropoff_count,
           d.trips_per_day,
           d.share_of_borough_pct,
           d.rank_in_month,
           p.weekend_share_pct,
           p.avg_distance_mi,
           p.avg_duration_min,
           p.avg_speed_mph,
           p.avg_total_amount_usd,
           k.peak_local_hour
    FROM demand d
    JOIN profile p ON d.pickup_year_month = p.pickup_year_month
                  AND d.pu_location_id    = p.pu_location_id
    JOIN peak k    ON d.pickup_year_month = k.pickup_year_month
                  AND d.pu_location_id    = k.pu_location_id
    """


def weather_impact_summary_sql(cfg: AnalyticsConfig, products: ProductsConfig) -> str:
    # The rate uses Q4's denominator: the hours the family occurred city-wide. Every zone is
    # kept; Q4's support floors (families_observed, zone_trip_count) are columns to filter on.
    return f"""
    WITH family_hours AS (
        SELECT weather_family,
               COUNT(DISTINCT pickup_hour) AS family_hours
        FROM trips
        WHERE NOT weather_is_missing
          AND weather_family IS NOT NULL
        GROUP BY weather_family
        HAVING COUNT(DISTINCT pickup_hour) >= {cfg.threshold("min_hours_per_cell")}
    ),
    cell AS (
        SELECT pu_location_id,
               weather_family,
               MAX(pickup_zone)    AS pickup_zone,
               MAX(pickup_borough) AS pickup_borough,
               COUNT(*)            AS trip_count,
               {_trip_profile(cfg, products)}
        FROM trips
        WHERE pu_location_id NOT IN ({cfg.sentinels})
          AND NOT weather_is_missing
          AND weather_family IS NOT NULL
        GROUP BY pu_location_id, weather_family
    ),
    rate AS (
        SELECT c.*,
               f.family_hours,
               c.trip_count / f.family_hours AS trips_per_hour
        FROM cell c
        JOIN family_hours f ON c.weather_family = f.weather_family
    )
    SELECT pu_location_id,
           pickup_zone,
           pickup_borough,
           weather_family,
           family_hours,
           trip_count,
           trips_per_hour,
           ROUND(trips_per_hour / AVG(trips_per_hour) OVER (PARTITION BY pu_location_id), 4)
               AS demand_index,
           COUNT(*) OVER (PARTITION BY pu_location_id)        AS families_observed,
           SUM(trip_count) OVER (PARTITION BY pu_location_id) AS zone_trip_count,
           avg_distance_mi,
           avg_duration_min,
           avg_speed_mph,
           avg_total_amount_usd
    FROM rate
    """


def hourly_air_quality_demand_sql(cfg: AnalyticsConfig, products: ProductsConfig) -> str:
    # hourly_demand materialised, renamed to the platform's unit-suffix convention. Values are
    # left unrounded so Q3's correlations can be recomputed from the table exactly.
    tz = cfg.reporting_timezone
    return f"""
    SELECT DATE_FORMAT(FROM_UTC_TIMESTAMP(pickup_hour, '{tz}'), 'yyyy-MM') AS pickup_year_month,
           TO_DATE(FROM_UTC_TIMESTAMP(pickup_hour, '{tz}'))              AS pickup_date,
           pickup_hour,
           local_hour,
           day_type,
           trip_count,
           demand_index,
           pm25     AS pm25_ugm3,
           pm25_max AS pm25_max_ugm3,
           aq_quartile,
           aq_epa_category
    FROM hourly_demand
    """


SQL_BUILDERS: dict[str, Callable[[AnalyticsConfig, ProductsConfig], str]] = {
    "daily_mobility_summary": daily_mobility_summary_sql,
    "zone_monthly_statistics": zone_monthly_statistics_sql,
    "weather_impact_summary": weather_impact_summary_sql,
    "hourly_air_quality_demand": hourly_air_quality_demand_sql,
}


def product_sql(name: str, cfg: AnalyticsConfig, products: ProductsConfig) -> str:
    return SQL_BUILDERS[name](cfg, products)
