"""The optimisation experiments.
Each experiment isolates one technique: baseline and optimised differ in exactly one
thing, and both must return the same answer. Anything that changes two things at once
cannot attribute the difference in time to either.
"""

from __future__ import annotations

from .benchmark import Experiment, Variant
from .config import AnalyticsConfig
from .queries import build_queries
from .views import hourly_demand_sql

# Delta's default.
BROADCAST_DEFAULT = "33554432"
BROADCAST_DISABLED = "-1"


def build_experiments(cfg: AnalyticsConfig) -> list[Experiment]:
    sql = {q.name: q.sql for q in build_queries(cfg)}
    sentinels = cfg.sentinels

    # 1. Partition pruning
    pruning_baseline = f"""
    SELECT pickup_year_month,
           pu_location_id,
           MAX(pickup_zone)    AS pickup_zone,
           MAX(pickup_borough) AS pickup_borough,
           COUNT(*)            AS trip_count
    FROM trips
    WHERE pickup_date >= DATE '2024-02-01'
      AND pickup_date <= DATE '2024-02-29'
      AND pu_location_id NOT IN ({sentinels})
    GROUP BY pickup_year_month, pu_location_id
    ORDER BY trip_count DESC
    """

    partition_pruning = Experiment(
        name="partition_pruning_vs_skipping",
        technique="Partition pruning vs Delta data skipping",
        question="Does expressing the month as the partition key beat expressing it as a date range?",
        baseline=Variant("date range on pickup_date", [pruning_baseline]),
        optimized=Variant("equality on pickup_year_month", [sql["q1_zone_demand_one_month"]]),
        notes="Identical result sets; the only difference is which column carries the restriction.",
    )

    pruning_blind = f"""
    SELECT pickup_year_month,
           pu_location_id,
           MAX(pickup_zone)    AS pickup_zone,
           MAX(pickup_borough) AS pickup_borough,
           COUNT(*)            AS trip_count
    FROM trips
    WHERE MONTH(pickup_date) = 2
      AND pu_location_id NOT IN ({sentinels})
    GROUP BY pickup_year_month, pu_location_id
    ORDER BY trip_count DESC
    """

    partition_pruning_blind = Experiment(
        name="partition_pruning_vs_full_scan",
        technique="Partition pruning",
        question="What does pruning save when nothing else can skip the data?",
        baseline=Variant("MONTH(pickup_date) = 2, unskippable", [pruning_blind]),
        optimized=Variant("equality on pickup_year_month", [sql["q1_zone_demand_one_month"]]),
        notes="Wrapping the column in a function is what makes the baseline honest: "
              "Spark cannot rewrite MONTH(col) = 2 into a range, so neither partition "
              "elimination nor file statistics can eliminate anything.",
    )

    prejoined = Experiment(
        name="prejoined_vs_runtime_join",
        technique="Materialisation (Week 1's integrated table)",
        question="What did pre-joining weather into the gold table buy?",
        baseline=Variant("join weather at query time, from silver",
                         [sql["q2_weather_distance_silver"]]),
        optimized=Variant("read the pre-joined gold table", [sql["q2_weather_distance"]]),
    )

    # 2. Broadcast join
    broadcast = Experiment(
        name="broadcast_join",
        technique="Broadcast join",
        question="What does broadcasting the weather dimension save over shuffling both sides?",
        baseline=Variant(
            "shuffle join (threshold = -1)",
            [sql["q2_weather_distance_silver"]],
            conf={"spark.sql.autoBroadcastJoinThreshold": BROADCAST_DISABLED},
        ),
        optimized=Variant(
            "broadcast join (threshold = 32 MB)",
            [sql["q2_weather_distance_silver"]],
            conf={"spark.sql.autoBroadcastJoinThreshold": BROADCAST_DEFAULT},
        ),
    )

    # 3. Caching
    q3 = [
        sql["q3_aq_demand_buckets"],
        sql["q3_aq_demand_correlation"],
        sql["q3_aq_epa_categories"],
    ]

    def cache_hourly_demand(spark) -> None:
        cached = spark.table("hourly_demand").cache()
        cached.count()  # materialise now
        cached.createOrReplaceTempView("hourly_demand")

    def restore_hourly_demand(spark) -> None:
        spark.catalog.clearCache()
        spark.sql(hourly_demand_sql(cfg))  # put the uncached definition back

    caching = Experiment(
        name="caching",
        technique="Caching",
        question="Does caching the hourly intermediate pay for itself across three consumers?",
        baseline=Variant("uncached, three consumers", q3),
        optimized=Variant(
            "cached hourly_demand, three consumers",
            q3,
            setup_fn=cache_hourly_demand,
            teardown_fn=restore_hourly_demand,
        ),
    )

    # 4. Adaptive Query Execution
    aqe = Experiment(
        name="aqe",
        technique="Adaptive Query Execution",
        question="Does AQE help Q4's shuffle-heavy aggregations?",
        baseline=Variant(
            "AQE disabled",
            [sql["q4_weather_sensitive_zones"]],
            conf={"spark.sql.adaptive.enabled": "false"},
        ),
        optimized=Variant(
            "AQE enabled",
            [sql["q4_weather_sensitive_zones"]],
            conf={"spark.sql.adaptive.enabled": "true"},
        ),
        notes="AQE's skew handling only splits partitions of shuffle joins, so Q4's skewed "
              "aggregations cannot trigger it.",
    )

    # 5. Control
    control = Experiment(
        name="control_tiny_join",
        technique="Control (broadcast setting on a join of two tiny aggregates)",
        question="Does a changed plan always mean a changed runtime?",
        baseline=Variant(
            "threshold = -1",
            [sql["q5_peak_hours"]],
            conf={"spark.sql.autoBroadcastJoinThreshold": BROADCAST_DISABLED},
        ),
        optimized=Variant(
            "threshold = 32 MB",
            [sql["q5_peak_hours"]],
            conf={"spark.sql.autoBroadcastJoinThreshold": BROADCAST_DEFAULT},
        ),
        notes="The plan changes and the runtime does not. Judge noise from the samples of "
              "each row, not from this row alone.",
    )

    return [
        partition_pruning,
        partition_pruning_blind,
        broadcast,
        prejoined,
        caching,
        aqe,
        control,
    ]


def aqe_sweep(cfg: AnalyticsConfig) -> list[Experiment]:
    # AQE on and off across the whole library.
    experiments = []
    for query in build_queries(cfg):
        experiments.append(
            Experiment(
                name=f"aqe_{query.name}",
                technique="AQE sweep",
                question=f"AQE on/off for {query.name}",
                baseline=Variant(
                    "AQE off", [query.sql], conf={"spark.sql.adaptive.enabled": "false"}
                ),
                optimized=Variant(
                    "AQE on", [query.sql], conf={"spark.sql.adaptive.enabled": "true"}
                ),
            )
        )
    return experiments


def materialisation_sweep(cfg: AnalyticsConfig) -> list[Experiment]:
    """Task 5: what a materialised product saves over recomputing it from the trips.

    Each pair answers the same question and returns the same rows. The baseline derives
    them from the 9.5M-row gold table; the optimised side derives them from the product,
    which has between 767 and 2,183 rows. The projections match the ones
    scripts/verify_products.py already proves equal, so a disagreement here is a
    regression rather than a difference in what was asked.
    """
    sql = {q.name: q.sql for q in build_queries(cfg)}
    min_families = cfg.threshold("min_families_per_zone")
    min_trips = cfg.threshold("min_trips_per_zone")

    zone_monthly = Experiment(
        name="product_zone_monthly_statistics",
        technique="Materialised product vs recomputation",
        question="What does reading zone_monthly_statistics save over recomputing Q1?",
        baseline=Variant("compute Q1 from the gold table", [f"""
            SELECT pickup_year_month, pu_location_id, trip_count, dropoff_count
            FROM ({sql['q1_zone_monthly_demand']})
            ORDER BY pickup_year_month, pu_location_id"""]),
        optimized=Variant("read zone_monthly_statistics", ["""
            SELECT pickup_year_month, pu_location_id, trip_count, dropoff_count
            FROM zone_monthly_statistics
            ORDER BY pickup_year_month, pu_location_id"""]),
    )

    weather_impact = Experiment(
        name="product_weather_impact_summary",
        technique="Materialised product vs recomputation",
        question="What does weather_impact_summary save over recomputing Q4's ranking?",
        baseline=Variant("compute Q4 from the gold table", [f"""
            SELECT pu_location_id, coef_of_variation
            FROM ({sql['q4_weather_sensitive_zones']})
            ORDER BY pu_location_id"""]),
        optimized=Variant("recompute from weather_impact_summary", [f"""
            SELECT pu_location_id,
                   ROUND(STDDEV_SAMP(trips_per_hour) / AVG(trips_per_hour), 4) AS coef_of_variation
            FROM weather_impact_summary
            GROUP BY pu_location_id
            HAVING COUNT(*) >= {min_families}
               AND SUM(trip_count) >= {min_trips}
            ORDER BY pu_location_id"""]),
    )

    air_quality = Experiment(
        name="product_hourly_air_quality_demand",
        technique="Materialised product vs recomputation",
        question="What does hourly_air_quality_demand save over recomputing Q3's correlations?",
        baseline=Variant("compute Q3 from the gold table", [f"""
            SELECT corr_raw, corr_controlled FROM ({sql['q3_aq_demand_correlation']})"""]),
        optimized=Variant("recompute from hourly_air_quality_demand", ["""
            SELECT ROUND(CORR(trip_count, pm25_ugm3), 4)   AS corr_raw,
                   ROUND(CORR(demand_index, pm25_ugm3), 4) AS corr_controlled
            FROM hourly_air_quality_demand"""]),
    )

    return [zone_monthly, weather_impact, air_quality]
