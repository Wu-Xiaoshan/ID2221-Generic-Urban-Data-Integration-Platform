"""The analytical query library"""

from __future__ import annotations

from dataclasses import dataclass

from .config import AnalyticsConfig, family_case


@dataclass(frozen=True)
class QueryDef:
    name: str
    analysis: str  # which of the six assignment analyses this serves
    title: str
    sql: str
    optimisation: str = ""  # the Task 3 technique this query is the vehicle for


def build_queries(cfg: AnalyticsConfig) -> list[QueryDef]:
    sentinels = cfg.sentinels
    min_hours_family = cfg.threshold("min_hours_per_family")
    min_hours_cell = cfg.threshold("min_hours_per_cell")
    min_families = cfg.threshold("min_families_per_zone")
    min_trips_zone = cfg.threshold("min_trips_per_zone")
    peak_n = cfg.threshold("peak_hours_per_day")

    # Q1
    q1 = f"""
    WITH pickups AS (
        SELECT pickup_year_month,
               pu_location_id            AS location_id,
               MAX(pickup_zone)          AS zone,
               MAX(pickup_borough)       AS borough,
               COUNT(*)                  AS trip_count
        FROM trips
        WHERE pu_location_id NOT IN ({sentinels})
        GROUP BY pickup_year_month, pu_location_id
    ),
    dropoffs AS (
        SELECT pickup_year_month,
               do_location_id AS location_id,
               COUNT(*)       AS dropoff_count
        FROM trips
        WHERE do_location_id NOT IN ({sentinels})
        GROUP BY pickup_year_month, do_location_id
    )
    SELECT p.pickup_year_month,
           p.location_id AS pu_location_id,
           p.zone        AS pickup_zone,
           p.borough     AS pickup_borough,
           p.trip_count,
           COALESCE(d.dropoff_count, 0) AS dropoff_count,
           ROUND(p.trip_count
                 / DAY(LAST_DAY(TO_DATE(CONCAT(p.pickup_year_month, '-01')))), 1)
               AS trips_per_day,
           ROUND(100.0 * p.trip_count
                 / SUM(p.trip_count) OVER (PARTITION BY p.pickup_year_month, p.borough), 2)
               AS share_of_borough_pct,
           RANK() OVER (PARTITION BY p.pickup_year_month ORDER BY p.trip_count DESC)
               AS rank_in_month
    FROM pickups p
    LEFT JOIN dropoffs d
           ON p.pickup_year_month = d.pickup_year_month
          AND p.location_id       = d.location_id
    ORDER BY p.pickup_year_month, rank_in_month
    """

    q1_month = f"""
    SELECT pickup_year_month,
           pu_location_id,
           MAX(pickup_zone)    AS pickup_zone,
           MAX(pickup_borough) AS pickup_borough,
           COUNT(*)            AS trip_count
    FROM trips
    WHERE pickup_year_month = '2024-02'
      AND pu_location_id NOT IN ({sentinels})
    GROUP BY pickup_year_month, pu_location_id
    ORDER BY trip_count DESC
    """

    # Q2. Two exclusions.
    cap = cfg.threshold("max_plausible_distance_mi")
    plausible = f"CASE WHEN trip_distance_mi <= {cap} THEN trip_distance_mi END"
    q2_metrics = f"""
           COUNT(*)                                    AS trip_count,
           COUNT(DISTINCT pickup_hour)                 AS hours_observed,
           SUM(CASE WHEN trip_distance_mi > {cap} THEN 1 ELSE 0 END)
               AS distance_outliers_excluded,
           ROUND(AVG({plausible}), 3)                  AS avg_distance_mi,
           ROUND(PERCENTILE_APPROX({plausible}, 0.5), 3) AS median_distance_mi,
           ROUND(STDDEV_SAMP({plausible}), 3)          AS stddev_distance_mi,
           ROUND(AVG(trip_duration_min), 2)            AS avg_duration_min,
           ROUND(AVG(CASE WHEN trip_distance_mi <= {cap}
                          THEN trip_distance_mi / (trip_duration_min / 60.0) END), 2)
               AS avg_speed_mph
    """

    q2 = f"""
    SELECT weather_family,
    {q2_metrics}
    FROM trips
    WHERE NOT weather_is_missing
      AND NOT dq_zero_distance
      AND weather_family IS NOT NULL
      AND trip_duration_min > 0
    GROUP BY weather_family
    HAVING COUNT(DISTINCT pickup_hour) >= {min_hours_family}
    ORDER BY avg_distance_mi DESC
    """
    q2_silver = f"""
    SELECT w.weather_family,
    {q2_metrics}
    FROM src_trips t
    JOIN (
        SELECT obs_hour,
               {family_case("condition_code", cfg.weather_families)} AS weather_family
        FROM src_weather
    ) w ON t.pickup_hour = w.obs_hour
    WHERE NOT t.dq_zero_distance
      AND w.weather_family IS NOT NULL
      AND t.trip_duration_min > 0
    GROUP BY w.weather_family
    HAVING COUNT(DISTINCT t.pickup_hour) >= {min_hours_family}
    ORDER BY avg_distance_mi DESC
    """

    # Q2, cross-check cut.
    q2_precip = f"""
    SELECT precip_bucket,
    {q2_metrics}
    FROM trips
    WHERE NOT weather_is_missing
      AND NOT dq_zero_distance
      AND precip_bucket IS NOT NULL
      AND trip_duration_min > 0
    GROUP BY precip_bucket
    HAVING COUNT(DISTINCT pickup_hour) >= {min_hours_family}
    ORDER BY avg_distance_mi DESC
    """

    # Q3. All three read hourly_demand.
    q3_buckets = """
    SELECT aq_quartile,
           COUNT(*)                      AS hours,
           ROUND(MIN(pm25), 2)           AS pm25_min,
           ROUND(MAX(pm25), 2)           AS pm25_max,
           ROUND(AVG(trip_count), 1)     AS avg_trips_per_hour,
           ROUND(AVG(demand_index), 4)   AS avg_demand_index
    FROM hourly_demand
    GROUP BY aq_quartile
    ORDER BY aq_quartile
    """

    q3_corr = """
    SELECT COUNT(*)                            AS hours,
           ROUND(AVG(pm25), 2)                 AS avg_pm25,
           ROUND(CORR(trip_count, pm25), 4)    AS corr_raw,
           ROUND(CORR(demand_index, pm25), 4)  AS corr_controlled
    FROM hourly_demand
    """

    q3_epa = """
    SELECT aq_epa_category,
           COUNT(*)                  AS hours,
           ROUND(AVG(trip_count), 1) AS avg_trips_per_hour
    FROM hourly_demand
    GROUP BY aq_epa_category
    ORDER BY hours DESC
    """

    # Q4. Two normalisations carry this query. The rate divides by the hours the family
    # occurred city-wide, not by the hours the zone had a pickup: the latter leaves the
    # zone's zero-trip hours out of the denominator and overstates its rate.
    q4 = f"""
    WITH family_hours AS (
        SELECT weather_family,
               COUNT(DISTINCT pickup_hour) AS hours
        FROM trips
        WHERE NOT weather_is_missing
          AND weather_family IS NOT NULL
        GROUP BY weather_family
    ),
    cell AS (
        SELECT pu_location_id,
               weather_family,
               MAX(pickup_zone)            AS pickup_zone,
               MAX(pickup_borough)         AS pickup_borough,
               COUNT(*)                    AS trips
        FROM trips
        WHERE pu_location_id NOT IN ({sentinels})
          AND NOT weather_is_missing
          AND weather_family IS NOT NULL
        GROUP BY pu_location_id, weather_family
    ),
    rate AS (
        SELECT c.*, f.hours, c.trips / f.hours AS trips_per_hour
        FROM cell c
        JOIN family_hours f ON c.weather_family = f.weather_family
        WHERE f.hours >= {min_hours_cell}
    ),
    zone AS (
        SELECT pu_location_id,
               MAX(pickup_zone)                        AS pickup_zone,
               MAX(pickup_borough)                     AS pickup_borough,
               COUNT(*)                                AS families_observed,
               SUM(trips)                              AS total_trips,
               AVG(trips_per_hour)                     AS mean_tph,
               STDDEV_SAMP(trips_per_hour)             AS sd_tph,
               MAX_BY(weather_family, trips_per_hour)  AS busiest_family,
               MIN_BY(weather_family, trips_per_hour)  AS quietest_family,
               MAX(trips_per_hour)                     AS max_tph,
               MIN(trips_per_hour)                     AS min_tph
        FROM rate
        GROUP BY pu_location_id
    )
    SELECT pu_location_id,
           pickup_zone,
           pickup_borough,
           families_observed,
           total_trips,
           ROUND(mean_tph, 2)              AS mean_trips_per_hour,
           ROUND(sd_tph, 2)                AS stddev_trips_per_hour,
           ROUND(sd_tph / mean_tph, 4)     AS coef_of_variation,
           busiest_family,
           quietest_family,
           ROUND(max_tph / min_tph, 2)     AS max_min_ratio
    FROM zone
    WHERE families_observed >= {min_families}
      AND total_trips       >= {min_trips_zone}
      AND mean_tph > 0
    ORDER BY coef_of_variation DESC
    """

    # Q5. Occurrences are counted per weekday over the whole window.
    q5 = f"""
    WITH occurrences AS (
        SELECT day_of_week_num,
               COUNT(DISTINCT pickup_date) AS occurrences
        FROM trips
        GROUP BY day_of_week_num
    ),
    cell AS (
        SELECT day_of_week_num,
               MAX(day_of_week) AS day_of_week,
               local_hour,
               COUNT(*)         AS total_trips
        FROM trips
        GROUP BY day_of_week_num, local_hour
    ),
    ranked AS (
        SELECT c.day_of_week_num,
               c.day_of_week,
               c.local_hour,
               c.total_trips,
               o.occurrences,
               ROUND(c.total_trips / o.occurrences, 1) AS avg_trips_per_occurrence,
               RANK() OVER (PARTITION BY c.day_of_week_num
                            ORDER BY c.total_trips DESC) AS rank_in_day,
               ROUND(100.0 * c.total_trips
                     / SUM(c.total_trips) OVER (PARTITION BY c.day_of_week_num), 2)
                   AS pct_of_day_total
        FROM cell c
        JOIN occurrences o ON c.day_of_week_num = o.day_of_week_num
    )
    SELECT day_of_week, local_hour, avg_trips_per_occurrence, total_trips,
           rank_in_day, pct_of_day_total
    FROM ranked
    WHERE rank_in_day <= {peak_n}
    ORDER BY day_of_week_num, rank_in_day
    """

    # Q6
    q6_monthly = f"""
    WITH monthly AS (
        SELECT pickup_year_month,
               pickup_borough,
               COUNT(*) AS trip_count,
               COUNT(*) / DAY(LAST_DAY(TO_DATE(CONCAT(pickup_year_month, '-01'))))
                   AS trips_per_day
        FROM trips
        WHERE pu_location_id NOT IN ({sentinels})
        GROUP BY pickup_year_month, pickup_borough
    )
    SELECT pickup_year_month,
           pickup_borough,
           trip_count,
           ROUND(trips_per_day, 1) AS trips_per_day,
           ROUND(100.0 * (trips_per_day
                 - LAG(trips_per_day) OVER (PARTITION BY pickup_borough
                                            ORDER BY pickup_year_month))
                 / LAG(trips_per_day) OVER (PARTITION BY pickup_borough
                                            ORDER BY pickup_year_month), 2)
               AS mom_change_pct,
           ROUND(100.0 * trip_count
                 / SUM(trip_count) OVER (PARTITION BY pickup_year_month), 2)
               AS pct_of_city
    FROM monthly
    ORDER BY pickup_borough, pickup_year_month
    """

    q6_daily = """
    WITH daily AS (
        SELECT pickup_date, COUNT(*) AS trip_count
        FROM trips
        GROUP BY pickup_date
    )
    SELECT pickup_date,
           trip_count,
           ROUND(AVG(trip_count) OVER (ORDER BY pickup_date
                                       ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1)
               AS ma7
    FROM daily
    ORDER BY pickup_date
    """

    return [
        QueryDef("q1_zone_monthly_demand", "1", "Monthly taxi demand per zone", q1,
                 "partition pruning (see q1_zone_demand_one_month)"),
        QueryDef("q1_zone_demand_one_month", "1", "Zone demand, February only", q1_month,
                 "partition pruning"),
        QueryDef("q2_weather_distance", "2", "Trip distance by weather family", q2,
                 "baseline for the broadcast-join comparison"),
        QueryDef("q2_weather_distance_silver", "2",
                 "Trip distance by weather family, joined from silver", q2_silver,
                 "broadcast join"),
        QueryDef("q2_precip_distance", "2", "Trip distance by precipitation bucket", q2_precip,
                 "cross-check cut, measured rather than modelled"),
        QueryDef("q3_aq_demand_buckets", "3", "Demand by PM2.5 quartile", q3_buckets,
                 "caching (hourly_demand)"),
        QueryDef("q3_aq_demand_correlation", "3", "PM2.5 vs demand, raw and controlled",
                 q3_corr, "caching (hourly_demand)"),
        QueryDef("q3_aq_epa_categories", "3", "Demand by EPA AQI category (negative result)",
                 q3_epa, "caching (hourly_demand)"),
        QueryDef("q4_weather_sensitive_zones", "4", "Zones ranked by weather sensitivity", q4,
                 "AQE (skew, shuffle-partition coalescing)"),
        QueryDef("q5_peak_hours", "5", "Peak travel hours per weekday", q5,
                 "control case: expect no effect"),
        QueryDef("q6_monthly_trend", "6", "Monthly demand trend by borough", q6_monthly,
                 "partition pruning, unfiltered contrast"),
        QueryDef("q6_daily_trend", "6", "Daily demand with 7-day moving average", q6_daily,
                 "partition pruning, unfiltered contrast"),
    ]
