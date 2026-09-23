"""Correctness checks for the analytical layer."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.analytics import AnalyticsConfig, build_queries, register_views  # noqa: E402
from urbanplatform.config import PlatformConfig  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402


class Checks:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0

    def __call__(self, name: str, condition: bool, detail: str) -> None:
        if condition:
            self.passed += 1
            print(f"  PASS  {name}: {detail}")
        else:
            self.failed += 1
            print(f"  FAIL  {name}: {detail}")


def main() -> int:
    analytics = AnalyticsConfig.load()
    platform = PlatformConfig.load()
    spark = build_session(platform)
    check = Checks()

    try:
        register_views(spark, platform, analytics)
        sql = {q.name: q.sql for q in build_queries(analytics)}
        rows = lambda s: spark.sql(s).collect()  # noqa: E731
        scalar = lambda s: rows(s)[0][0]  # noqa: E731

        # Gold must hold every silver trip exactly once, however many releases were applied.
        total = scalar("SELECT COUNT(*) FROM trips")
        silver = spark.read.format("delta").load(platform.table_path("silver", "taxi_trips")).count()
        check("gold row count", total == silver, f"{total:,} in gold, {silver:,} in silver")

        # The window is whatever the data covers; the checks below need it to have no gaps.
        window_days = scalar("SELECT DATEDIFF(MAX(pickup_date), MIN(pickup_date)) + 1 FROM trips")
        observed_days = scalar("SELECT COUNT(DISTINCT pickup_date) FROM trips")
        check("date coverage", observed_days == window_days,
              f"{observed_days} days with trips in a {window_days}-day window")

        # Q1 must account for every non-sentinel pickup exactly once: no double counting
        q1_sum = sum(r["trip_count"] for r in rows(sql["q1_zone_monthly_demand"]))
        direct = scalar("SELECT COUNT(*) FROM trips WHERE pu_location_id NOT IN (264, 265)")
        check("Q1 conservation", q1_sum == direct,
              f"Q1 sums to {q1_sum:,}, direct count {direct:,}")
        trapped = scalar("""SELECT COUNT(*) FROM trips
                            WHERE pu_location_id NOT IN (264, 265) AND is_unlocated""")
        check("is_unlocated over-filtering", trapped > 0,
              f"{trapped:,} trips have a valid pickup but would be dropped by NOT is_unlocated")

        # The timezone conversion
        shifts = sorted(r["h"] for r in rows(
            "SELECT DISTINCT (HOUR(pickup_ts) - local_hour + 24) % 24 AS h FROM trips"))
        check("UTC to local shift", shifts == [4, 5],
              f"observed {shifts} (EDT = 4, EST = 5; a 0 would mean no conversion)")

        # Q5's per-occurrence normalisation
        occurrences = rows("""SELECT day_of_week_num, COUNT(DISTINCT pickup_date) AS n
                              FROM trips GROUP BY day_of_week_num""")
        counts = sorted(r["n"] for r in occurrences)
        check("Q5 weekday occurrences", sum(counts) == window_days,
              f"{sum(counts)} days, per weekday {counts}")

        hourly = scalar("SELECT COUNT(*) FROM hourly_demand")
        distinct_hours = scalar("SELECT COUNT(DISTINCT pickup_hour) FROM hourly_demand")
        check("hourly_demand grain", hourly == distinct_hours,
              f"{hourly} rows, {distinct_hours} distinct hours (one row per hour)")

        # Q4 must honour its configured support floors rather than merely declare them.
        q4 = rows(sql["q4_weather_sensitive_zones"])
        floors_held = all(
            r["families_observed"] >= analytics.threshold("min_families_per_zone")
            and r["total_trips"] >= analytics.threshold("min_trips_per_zone")
            and r["coef_of_variation"] >= 0
            for r in q4
        )
        check("Q4 support thresholds", floors_held,
              f"{len(q4)} zones survive; all clear the configured floors")

        daily = rows(sql["q6_daily_trend"])
        check("Q6 daily coverage", len(daily) == window_days, f"{len(daily)} daily rows")

        # The pre-joined and query-time-joined implementations of Q2 must agree.
        gold_rows = rows(sql["q2_weather_distance"])
        silver_rows = rows(sql["q2_weather_distance_silver"])
        check("Q2 gold == silver",
              [tuple(r) for r in gold_rows] == [tuple(r) for r in silver_rows],
              f"{len(gold_rows)} rows identical")
        excluded = sum(r["distance_outliers_excluded"] for r in gold_rows)
        check("distance cap active", excluded > 0,
              f"{excluded} implausible-distance rows excluded from distance metrics")

        print(f"\n{check.passed} passed, {check.failed} failed")
    finally:
        spark.stop()

    return 1 if check.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
