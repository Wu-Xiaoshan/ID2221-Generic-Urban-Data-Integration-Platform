"""Correctness checks for the analytical data products.

Each product is checked against the analytical query it materialises or against the gold
table directly, plus its grain, its metadata and its freshness. Run jobs/run_products.py first.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.analytics import AnalyticsConfig, build_queries, register_views  # noqa: E402
from urbanplatform.analytics.materialize import (  # noqa: E402
    product_properties,
    table_detail,
    table_version,
)
from urbanplatform.analytics.products import ProductsConfig  # noqa: E402
from urbanplatform.config import PlatformConfig  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402

REQUIRED_PROPERTIES = ("source", "source_version", "created_at", "refreshed_at", "schema_version")


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
    products = ProductsConfig.load()
    spark = build_session(platform)
    check = Checks()

    try:
        register_views(spark, platform, analytics)
        sql = {q.name: q.sql for q in build_queries(analytics)}
        rows = lambda s: spark.sql(s).collect()  # noqa: E731
        scalar = lambda s: rows(s)[0][0]  # noqa: E731

        gold_version = table_version(
            spark, platform.table_path(products.source_layer, products.source_table)
        )
        metadata_path = platform.meta_path("data_product_metadata")
        spark.read.format("delta").load(metadata_path).createOrReplaceTempView("product_metadata")

        # Generic checks: every product exists, carries its metadata, is current, and has
        # exactly one row per key.
        for spec in products.products:
            path = platform.table_path(products.layer, spec.name)
            detail = table_detail(spark, path)
            check(f"{spec.name} exists", detail is not None, path)
            if detail is None:
                continue
            spark.read.format("delta").load(path).createOrReplaceTempView(spec.name)

            props = product_properties(detail)
            missing = [p for p in REQUIRED_PROPERTIES if p not in props]
            check(f"{spec.name} metadata", not missing,
                  f"missing {missing}" if missing else
                  f"created {props['created_at']}, refreshed {props['refreshed_at']}")
            check(f"{spec.name} schema version", props.get("schema_version") == spec.schema_version,
                  f"table v{props.get('schema_version')}, conf/products.yaml v{spec.schema_version}")
            check(f"{spec.name} freshness", props.get("source_version") == str(gold_version),
                  f"built from gold v{props.get('source_version')}, gold is v{gold_version}")

            key = ", ".join(spec.key)
            count = scalar(f"SELECT COUNT(*) FROM {spec.name}")
            duplicates = scalar(
                f"SELECT COUNT(*) FROM (SELECT {key} FROM {spec.name} GROUP BY {key} HAVING COUNT(*) > 1)"
            )
            check(f"{spec.name} grain", duplicates == 0,
                  f"{count:,} rows, {duplicates} duplicate ({key}) keys")

            latest = rows(f"""
                SELECT status, row_count FROM product_metadata
                WHERE product_name = '{spec.name}' AND status <> 'failed'
                ORDER BY refreshed_at DESC, run_id DESC LIMIT 1""")
            check(f"{spec.name} metadata row", bool(latest) and latest[0]["row_count"] == count,
                  f"latest run {latest[0]['status']}, {latest[0]['row_count']:,} rows recorded"
                  if latest else "no successful run recorded")

        # daily_mobility_summary: every located pickup counted once, on every day of the window.
        daily_total = scalar("SELECT SUM(trip_count) FROM daily_mobility_summary")
        located = scalar(f"SELECT COUNT(*) FROM trips WHERE pu_location_id NOT IN ({analytics.sentinels})")
        check("daily_mobility_summary conservation", daily_total == located,
              f"sums to {daily_total:,}, gold has {located:,} located pickups")
        days = scalar("SELECT COUNT(DISTINCT pickup_date) FROM daily_mobility_summary")
        gold_days = scalar("SELECT COUNT(DISTINCT pickup_date) FROM trips")
        check("daily_mobility_summary coverage", days == gold_days,
              f"{days} days, gold has trips on {gold_days}")

        # zone_monthly_statistics: identical demand to Q1.
        q1 = {(r["pickup_year_month"], r["pu_location_id"]): (r["trip_count"], r["dropoff_count"])
              for r in rows(sql["q1_zone_monthly_demand"])}
        product = {(r["pickup_year_month"], r["pu_location_id"]): (r["trip_count"], r["dropoff_count"])
                   for r in rows("SELECT pickup_year_month, pu_location_id, trip_count, dropoff_count "
                                 "FROM zone_monthly_statistics")}
        check("zone_monthly_statistics == Q1", product == q1, f"{len(product)} zone-months compared")

        # weather_impact_summary: Q4's ranking recomputed from the product alone.
        q4 = {r["pu_location_id"]: r["coef_of_variation"] for r in rows(sql["q4_weather_sensitive_zones"])}
        recomputed = {r["pu_location_id"]: r["cv"] for r in rows(f"""
            SELECT pu_location_id,
                   ROUND(STDDEV_SAMP(trips_per_hour) / AVG(trips_per_hour), 4) AS cv
            FROM weather_impact_summary
            GROUP BY pu_location_id
            HAVING COUNT(*) >= {analytics.threshold("min_families_per_zone")}
               AND SUM(trip_count) >= {analytics.threshold("min_trips_per_zone")}""")}
        check("weather_impact_summary reproduces Q4", recomputed == q4,
              f"{len(recomputed)} zones, coefficient of variation identical")

        # hourly_air_quality_demand: the same hours and Q3's correlations.
        hours = scalar("SELECT COUNT(*) FROM hourly_air_quality_demand")
        expected = scalar("SELECT COUNT(*) FROM hourly_demand")
        check("hourly_air_quality_demand grain", hours == expected, f"{hours} hours, hourly_demand has {expected}")
        q3 = rows(sql["q3_aq_demand_correlation"])[0]
        again = rows("""SELECT ROUND(CORR(trip_count, pm25_ugm3), 4)   AS corr_raw,
                               ROUND(CORR(demand_index, pm25_ugm3), 4) AS corr_controlled
                        FROM hourly_air_quality_demand""")[0]
        check("hourly_air_quality_demand reproduces Q3",
              (again["corr_raw"], again["corr_controlled"]) == (q3["corr_raw"], q3["corr_controlled"]),
              f"corr_raw {again['corr_raw']}, corr_controlled {again['corr_controlled']}")

        print(f"\n{check.passed} passed, {check.failed} failed")
    finally:
        spark.stop()

    return 1 if check.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
