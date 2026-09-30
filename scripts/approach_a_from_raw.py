"""Week 4 Task 4, Approach A: the trip-duration model built straight from the raw files.

The same training dataset, features and model as Tasks 1-3, but without the platform: no
silver or gold table, no conf/ YAML and no code from src/urbanplatform. Everything the
platform does for this one model is written out here by hand: reading the four raw
sources and the release v2 taxi file, cleaning them, joining weather, air quality and
zones to each trip, filtering and splitting, the feature stages and the regression.

Every rule below reproduces a decision the platform already made, so that both approaches
train on the same rows and the comparison is about the work, not about different data.
Where a rule is not obvious from the raw data, the comment says what goes wrong without it.

    python scripts/approach_a_from_raw.py                   # original files and release v2
    python scripts/approach_a_from_raw.py --without-update  # original files only

Writes the dataset and the fitted model under data/approach_a/, and the metrics and stage
timings to reports/comparison/approach_a.json. scripts/compare_approaches.py checks the
result against the platform's dataset and model.
"""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import os
import shutil
import sys
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(REPO, "data", "raw")
UPDATE = os.path.join(REPO, "data", "updates", "v2", "taxi_trips")
OUT = os.path.join(REPO, "data", "approach_a")
NY = "America/New_York"

# Zones 264 (Unknown) and 265 (Outside of NYC) are not places.
SENTINEL_ZONES = (264, 265)
AIRPORT_ZONES = (1, 132, 138)
HOLIDAYS = ["2024-01-01", "2024-01-15", "2024-02-19", "2024-05-27", "2024-06-19", "2024-07-04",
            "2024-09-02", "2024-10-14", "2024-11-11", "2024-11-28", "2024-12-25"]
WEATHER_FAMILIES = {
    "Clear": [1, 2], "Cloudy": [3, 4], "Fog": [5, 6], "Rain": [7, 8, 9, 17, 18],
    "Snow/Ice": [10, 11, 12, 13, 14, 15, 16, 19, 20, 21, 22, 24], "Storm": [23, 25, 26, 27],
}
# A context value is carried to an hour with no observation from at most this many hours away.
MAX_GAP_HOURS = 3


def session():
    """Local Spark with the platform's settings, so the timings compare like with like."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    if os.name == "nt" and os.environ.get("HADOOP_HOME"):
        os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("approach-a").master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "1g")
        # Every timestamp below is UTC; the local clock is only ever applied explicitly.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", str(32 * 1024 * 1024))
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------------------- loading and cleaning

def load_zones(spark):
    # "N/A" is a borough name in this file, not a missing value, so only "" means null.
    df = (spark.read.option("header", True).option("inferSchema", True).option("nullValue", "")
          .csv(os.path.join(RAW, "taxi_zones", "taxi_zone_lookup.csv")))
    return df.select(F.col("LocationID").cast("int").alias("location_id"), F.col("Borough").alias("borough"))


def load_trips(spark, with_update: bool, zone_ids: list[int]):
    files = sorted(glob.glob(os.path.join(RAW, "taxi_trips", "*.parquet")))
    if with_update:
        files += sorted(glob.glob(os.path.join(UPDATE, "*.parquet")))
    raw = spark.read.parquet(*files)

    # The timestamps are New York wall-clock time with no offset. March 2024 crosses the
    # DST change, so they are converted with the zone, not with a fixed -5 hours.
    df = raw.select(
        F.col("VendorID").alias("vendor_id"),
        F.to_utc_timestamp(F.col("tpep_pickup_datetime").cast("timestamp"), NY).alias("pickup_ts"),
        F.to_utc_timestamp(F.col("tpep_dropoff_datetime").cast("timestamp"), NY).alias("dropoff_ts"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").alias("trip_distance_mi"),
        F.col("RatecodeID").cast("int").alias("ratecode_id"),
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
        F.col("total_amount"),
    )
    # The feed has no trip identifier. A hash of the columns that identify a trip gives the
    # same key to the same trip in both files, so the copies release v2 re-delivers collapse.
    df = df.withColumn("trip_id", F.xxhash64(*[F.col(c).cast("string") for c in (
        "vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "total_amount")]))
    df = df.withColumn("trip_duration_min",
                       (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0)

    valid = (
        F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull()
        & F.col("pu_location_id").isNotNull() & F.col("do_location_id").isNotNull()
        # The files hold a few rows dated 2002-2009. The window is the covered local period,
        # 1 January to 7 April, written as UTC instants because pickup_ts is already UTC.
        & (F.col("pickup_ts") >= F.lit("2024-01-01 05:00:00").cast("timestamp"))
        & (F.col("pickup_ts") < F.lit("2024-04-08 04:00:00").cast("timestamp"))
        & (F.col("trip_duration_min") > 0) & (F.col("trip_duration_min") <= 1440)
        & (F.col("trip_distance_mi").isNull() | (F.col("trip_distance_mi") >= 0))
        & F.col("pu_location_id").isin(zone_ids) & F.col("do_location_id").isin(zone_ids)
    )
    # One row per trip_id. A duplicate is dropped whatever else is wrong with either copy.
    first = F.row_number().over(Window.partitionBy("trip_id").orderBy(F.lit(1))) == 1
    df = df.withColumn("_first", first).where(valid & F.col("_first")).drop("_first")

    local = F.from_utc_timestamp(F.col("pickup_ts"), NY)
    return df.select(
        "trip_id", "pickup_ts", "trip_duration_min", "trip_distance_mi", "passenger_count", "vendor_id",
        "ratecode_id", "pu_location_id", "do_location_id",
        # The date is New York's, so the last evening of a day stays on that day.
        F.to_date(local).alias("pickup_date"),
        # The join hour is UTC, because weather and air quality are stamped in UTC.
        F.date_trunc("hour", F.col("pickup_ts")).alias("pickup_hour"),
        (F.col("pu_location_id").isin(*SENTINEL_ZONES) | F.col("do_location_id").isin(*SENTINEL_ZONES))
        .alias("is_unlocated"),
        # A negative total is a refund or dispute, not a trip anyone should learn from.
        # A null total is not a negative one.
        F.coalesce(F.col("total_amount") < 0, F.lit(False)).alias("negative_total"),
    )


def load_weather(spark):
    df = (spark.read.option("header", True).option("inferSchema", True)
          .csv(os.path.join(RAW, "weather", "*.csv")))
    # Four integer columns, already UTC.
    obs = F.make_timestamp("year", "month", "day", "hour", F.lit(0), F.lit(0).cast("decimal(16,6)"))
    df = df.select(
        F.date_trunc("hour", obs).alias("obs_hour"),
        F.col("temp").alias("temp_c"), F.col("rhum").alias("humidity_pct"),
        F.col("prcp").alias("precipitation_mm"), F.col("wdir").alias("wind_direction_deg"),
        F.col("wspd").alias("wind_speed_kmh"), F.col("cldc").cast("int").alias("cloud_cover_oktas"),
        F.col("coco").cast("int").alias("condition_code"),
    )

    def within(c, lo, hi=None):
        inside = F.col(c) >= lo
        if hi is not None:
            inside = inside & (F.col(c) <= hi)
        return F.col(c).isNull() | inside

    first = F.row_number().over(Window.partitionBy("obs_hour").orderBy(F.lit(1))) == 1
    valid = (
        F.col("obs_hour").isNotNull() & F.col("temp_c").isNotNull()
        & (F.col("obs_hour") >= F.lit("2024-01-01").cast("timestamp"))
        & (F.col("obs_hour") < F.lit("2025-02-01").cast("timestamp"))
        & within("temp_c", -40, 55) & within("humidity_pct", 0, 100)
        & within("wind_direction_deg", 0, 360) & within("precipitation_mm", 0)
    )
    return df.withColumn("_first", first).where(valid & F.col("_first")).drop("_first")


def load_air_quality(spark):
    # The EPA's national file: 8.1 million rows, of which New York City is 52 thousand.
    # Read as strings, because the site codes are zero-padded.
    df = (spark.read.option("header", True).option("inferSchema", False)
          .csv(os.path.join(RAW, "air_quality", "*.csv"))
          .where("`State Code` = '36' AND `County Code` IN ('005','047','061','081','085')"))
    # GMT, not "Local": EPA local time is EST all year, an hour off New York from 10 March.
    obs = F.to_timestamp(F.concat_ws(" ", "Date GMT", "Time GMT"), "yyyy-MM-dd HH:mm")
    df = df.select(
        F.concat("State Code", "County Code", "Site Num").alias("site_id"),
        F.col("Parameter Code").alias("parameter_code"), F.col("POC").cast("int").alias("poc"),
        obs.alias("obs_ts"), F.col("Sample Measurement").cast("double").alias("pm25"),
        F.col("Units of Measure").alias("unit"),
    )
    # Co-located instruments share a site and an hour; POC tells them apart.
    first = F.row_number().over(
        Window.partitionBy("site_id", "parameter_code", "poc", "obs_ts").orderBy(F.lit(1))) == 1
    valid = (
        F.col("site_id").isNotNull() & F.col("poc").isNotNull() & F.col("obs_ts").isNotNull()
        & F.col("pm25").isNotNull()
        & (F.col("obs_ts") >= F.lit("2024-01-01").cast("timestamp"))
        & (F.col("obs_ts") < F.lit("2025-02-02").cast("timestamp"))
        & (F.col("pm25") >= 0) & (F.col("pm25") <= 1000)
        & (F.col("unit").isNull() | (F.col("unit") == "Micrograms/cubic meter (LC)"))
    )
    readings = df.withColumn("_first", first).where(valid & F.col("_first"))
    # Six monitors for 265 zones and none in Manhattan: one city-wide value per hour.
    return (readings.groupBy(F.date_trunc("hour", "obs_ts").alias("obs_hour"))
            .agg(F.avg("pm25").alias("pm25_ugm3")))


# ---------------------------------------------------------------------------- integration

def fill_hours(df, columns: dict[str, str], name: str):
    """One row per hour from the first to the last observation, gaps filled from the nearest
    observed hour up to MAX_GAP_HOURS away (the earlier one on a tie), else left missing."""
    gap = MAX_GAP_HOURS
    present = df.select("obs_hour", *columns).withColumn("_obs", F.lit(True))
    spine = present.select(F.explode(F.expr(
        f"sequence(min(obs_hour) - make_interval(0,0,0,0,{gap},0,0), "
        f"max(obs_hour) + make_interval(0,0,0,0,{gap},0,0), interval 1 hour)")).alias("obs_hour"))
    dense = (spine.join(present, "obs_hour", "left")
             .withColumn("_obs", F.coalesce("_obs", F.lit(False)))
             .withColumn("_obs_hour", F.when(F.col("_obs"), F.col("obs_hour"))))

    back = Window.orderBy("obs_hour").rowsBetween(-gap, 0)
    fwd = Window.orderBy("obs_hour").rowsBetween(0, gap)
    prev_hour = F.last("_obs_hour", ignorenulls=True).over(back)
    next_hour = F.first("_obs_hour", ignorenulls=True).over(fwd)
    dense = (dense.withColumn("_prev", prev_hour).withColumn("_next", next_hour)
             .withColumn("_back", (F.unix_timestamp("obs_hour") - F.unix_timestamp("_prev")) / 3600)
             .withColumn("_fwd", (F.unix_timestamp("_next") - F.unix_timestamp("obs_hour")) / 3600))
    use_back = F.col("_prev").isNotNull() & (F.col("_next").isNull() | (F.col("_back") <= F.col("_fwd")))
    use_fwd = F.col("_next").isNotNull()

    out = [F.col("obs_hour").alias("pickup_hour")]
    for src, tgt in columns.items():
        out.append(F.when(F.col("_obs"), F.col(src))
                   .when(use_back, F.last(src, ignorenulls=True).over(back))
                   .when(use_fwd, F.first(src, ignorenulls=True).over(fwd)).alias(tgt))
    out.append((~F.col("_obs") & ~use_back & ~use_fwd).alias(f"{name}_is_missing"))
    return dense.select(*out)


def integrate(trips, weather, air_quality, zones):
    weather = fill_hours(weather, {
        "temp_c": "weather_temp_c", "humidity_pct": "weather_humidity_pct",
        "precipitation_mm": "weather_precipitation_mm", "wind_speed_kmh": "weather_wind_speed_kmh",
        "cloud_cover_oktas": "weather_cloud_cover_oktas", "condition_code": "weather_condition_code",
    }, "weather")
    air_quality = fill_hours(air_quality, {"pm25_ugm3": "aq_pm25_ugm3"}, "air_quality")
    df = (trips.join(F.broadcast(weather), "pickup_hour", "left")
          .join(F.broadcast(air_quality), "pickup_hour", "left")
          .withColumn("weather_is_missing", F.coalesce("weather_is_missing", F.lit(True)))
          .withColumn("air_quality_is_missing", F.coalesce("air_quality_is_missing", F.lit(True))))
    for side, key in (("pickup", "pu_location_id"), ("dropoff", "do_location_id")):
        z = zones.select(F.col("location_id").alias(key), F.col("borough").alias(f"{side}_borough"))
        df = df.join(F.broadcast(z), key, "left")
    return df


# ---------------------------------------------------------------------------- dataset

DATASET_COLUMNS = [
    "pickup_date", "pickup_ts", "trip_distance_mi", "passenger_count", "vendor_id", "ratecode_id",
    "pu_location_id", "do_location_id", "pickup_borough", "dropoff_borough", "weather_temp_c",
    "weather_humidity_pct", "weather_precipitation_mm", "weather_wind_speed_kmh",
    "weather_cloud_cover_oktas", "weather_condition_code", "weather_is_missing", "aq_pm25_ugm3",
    "air_quality_is_missing",
]


def usable(df):
    """Trips a duration model should learn from: located, plausible, not refunds."""
    speed = F.col("trip_distance_mi") / (F.col("trip_duration_min") / 60)
    keep = (~F.col("is_unlocated") & F.col("trip_duration_min").between(1, 180)
            & (F.col("trip_distance_mi") > 0) & (F.col("trip_distance_mi") <= 100)
            & speed.between(1, 80) & ~F.col("negative_total") & F.col("trip_duration_min").isNotNull())
    return df.where(F.coalesce(keep, F.lit(False)))


def build_dataset(spark, with_update: bool) -> dict:
    zones = load_zones(spark)
    zone_ids = [r[0] for r in zones.where(F.col("location_id").isNotNull()).collect()]
    trips = load_trips(spark, with_update, zone_ids)
    kept = usable(integrate(trips, load_weather(spark), load_air_quality(spark), zones))

    staging = os.path.join(OUT, "_staging")
    kept.select("trip_id", F.col("trip_duration_min").cast("double").alias("label"), *DATASET_COLUMNS) \
        .write.mode("overwrite").parquet(staging)
    kept = spark.read.parquet(staging)

    # Chronological split: the last 14 days are test, the 14 before them validation.
    newest = kept.agg(F.max("pickup_date")).first()[0]
    test_start = newest - dt.timedelta(days=13)
    validation_start = test_start - dt.timedelta(days=14)
    split = (F.when(F.col("pickup_date") < F.lit(validation_start), "train")
             .when(F.col("pickup_date") < F.lit(test_start), "validation").otherwise("test"))
    kept.withColumn("split", split).write.mode("overwrite").partitionBy("split") \
        .parquet(os.path.join(OUT, "trip_duration_dataset"))
    shutil.rmtree(staging)
    return {"newest": str(newest), "validation_start": str(validation_start), "test_start": str(test_start)}


# ---------------------------------------------------------------------------- features and model

NUMERIC = ["trip_distance_mi", "passenger_count", "pickup_hour_sin", "pickup_hour_cos", "weather_temp_c",
           "weather_humidity_pct", "weather_precipitation_mm", "weather_wind_speed_kmh",
           "weather_cloud_cover_oktas", "aq_pm25_ugm3"]
CATEGORICAL = ["vendor_id", "ratecode_id", "pickup_hour_of_day", "pickup_day_of_week", "pickup_month",
               "pu_location_id", "do_location_id", "pickup_borough", "dropoff_borough", "borough_pair",
               "weather_family"]
BINARY = ["pickup_is_weekend", "pickup_is_holiday", "same_borough", "pickup_is_airport", "dropoff_is_airport",
          "is_precipitating", "weather_is_missing", "air_quality_is_missing"]


def derived_features() -> str:
    """Calendar, zone-pair and weather features as one SQL select over the dataset."""
    local = f"from_utc_timestamp(pickup_ts, '{NY}')"
    day_fraction = f"((hour({local}) * 60 + minute({local})) / 1440D)"
    holidays = ", ".join(f"DATE '{d}'" for d in HOLIDAYS)
    airports = ", ".join(str(z) for z in AIRPORT_ZONES)
    family = "CASE " + " ".join(f"WHEN weather_condition_code IN ({', '.join(map(str, codes))}) THEN '{name}'"
                                for name, codes in WEATHER_FAMILIES.items()) + " END"
    return f"""SELECT *,
        hour({local}) AS pickup_hour_of_day,
        dayofweek({local}) AS pickup_day_of_week,
        month({local}) AS pickup_month,
        dayofweek({local}) IN (1, 7) AS pickup_is_weekend,
        to_date({local}) IN ({holidays}) AS pickup_is_holiday,
        sin(2 * pi() * {day_fraction}) AS pickup_hour_sin,
        cos(2 * pi() * {day_fraction}) AS pickup_hour_cos,
        pickup_borough = dropoff_borough AS same_borough,
        concat_ws(' -> ', pickup_borough, dropoff_borough) AS borough_pair,
        pu_location_id IN ({airports}) AS pickup_is_airport,
        do_location_id IN ({airports}) AS dropoff_is_airport,
        {family} AS weather_family
    FROM __THIS__"""


def typed_features() -> str:
    """Numeric as double (log1p for the skewed two), categorical as string with a missing
    level, binary as 0/1. Passenger count 0 and rate code 99 are the feed's 'unknown'."""
    num = {
        "trip_distance_mi": "log1p(CAST(trip_distance_mi AS DOUBLE))",
        "passenger_count": "CAST(CASE WHEN passenger_count IN (0) THEN NULL ELSE passenger_count END AS DOUBLE)",
        "weather_precipitation_mm": "log1p(CAST(weather_precipitation_mm AS DOUBLE))",
    }
    cat = {"ratecode_id": "CASE WHEN ratecode_id IN (99) THEN NULL ELSE ratecode_id END"}
    parts = [f"{num.get(n, f'CAST({n} AS DOUBLE)')} AS n_{n}" for n in NUMERIC]
    parts += [f"COALESCE(CAST({cat.get(n, n)} AS STRING), '__missing__') AS c_{n}" for n in CATEGORICAL]
    parts += [f"CAST(COALESCE(CAST({n} AS BOOLEAN), false) AS DOUBLE) AS b_{n}" for n in BINARY]
    return ("SELECT *, coalesce(weather_precipitation_mm > 0, weather_family IN ('Rain', 'Snow/Ice')) "
            "AS is_precipitating FROM __THIS__", "SELECT *, " + ", ".join(parts) + " FROM __THIS__")


def fit_model(train):
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.feature import (Imputer, OneHotEncoder, SQLTransformer, StandardScaler, StringIndexer,
                                    VectorAssembler)
    from pyspark.ml.regression import LinearRegression

    precipitating, typed = typed_features()
    prep = [SQLTransformer(statement=derived_features()), SQLTransformer(statement=precipitating),
            SQLTransformer(statement=typed)]
    prepared = PipelineModel(stages=prep).transform(train)

    # Numerics with a null in training get a 0/1 'was missing' slot before they are filled.
    nulls = prepared.agg(*[F.sum((F.col(f"n_{n}").isNull() | F.isnan(f"n_{n}")).cast("long")).alias(n)
                           for n in NUMERIC]).first().asDict()
    flagged = [n for n in NUMERIC if nulls[n]]
    flags = SQLTransformer(statement="SELECT *, " + ", ".join(
        f"CAST(n_{n} IS NULL OR isnan(n_{n}) AS DOUBLE) AS m_{n}" for n in flagged) + " FROM __THIS__")

    # Missing precipitation is filled with the training median of its weather family.
    def median(c):
        return F.percentile_approx(F.when(~F.isnan(c), F.col(c)), 0.5, 100_000)
    col = "n_weather_precipitation_mm"
    overall = prepared.agg(median(col)).first()[0]
    fill = f"CAST('{overall!r}' AS DOUBLE)"
    by_family = {r[0]: r[1] for r in prepared.groupBy("weather_family").agg(median(col)).collect()
                 if r[0] is not None and r[1] is not None}
    for family, value in sorted(by_family.items(), reverse=True):
        fill = f"CASE WHEN weather_family = '{family}' THEN CAST('{value!r}' AS DOUBLE) ELSE {fill} END"
    family_fill = SQLTransformer(statement=(
        f"SELECT *, CASE WHEN {col} IS NULL OR isnan({col}) THEN {fill} ELSE {col} END AS f_weather_precipitation_mm "
        "FROM __THIS__"))
    imputed = [("f_" if n == "weather_precipitation_mm" else "n_") + n for n in NUMERIC]

    stages = prep + [flags, family_fill,
                     Imputer(inputCols=imputed, outputCols=[f"i_{n}" for n in NUMERIC],
                             strategy="median"),
                     VectorAssembler(inputCols=[f"i_{n}" for n in NUMERIC], outputCol="numeric"),
                     StandardScaler(inputCol="numeric", outputCol="numeric_scaled", withMean=True, withStd=True),
                     StringIndexer(inputCols=[f"c_{n}" for n in CATEGORICAL],
                                   outputCols=[f"x_{n}" for n in CATEGORICAL],
                                   handleInvalid="keep", stringOrderType="frequencyDesc"),
                     OneHotEncoder(inputCols=[f"x_{n}" for n in CATEGORICAL],
                                   outputCols=[f"o_{n}" for n in CATEGORICAL], handleInvalid="keep", dropLast=True),
                     VectorAssembler(inputCols=["numeric_scaled", *[f"o_{n}" for n in CATEGORICAL],
                                                *[f"b_{n}" for n in BINARY], *[f"m_{n}" for n in flagged]],
                                     outputCol="features"),
                     LinearRegression(featuresCol="features", labelCol="label", predictionCol="prediction",
                                      regParam=0.01, elasticNetParam=0.0)]
    return Pipeline(stages=stages).fit(train), flagged


def evaluate(model, dataset) -> dict:
    from pyspark.ml.evaluation import RegressionEvaluator

    scored = model.transform(dataset).select("trip_id", "split", "label", "prediction").cache()
    ev = RegressionEvaluator(labelCol="label", predictionCol="prediction")
    mean = dataset.where("split = 'train'").agg(F.avg("label")).first()[0]
    out = {"rows": {r["split"]: r["count"] for r in scored.groupBy("split").count().collect()},
           "train_mean": mean}
    for s in ("train", "validation", "test"):
        part = scored.where(F.col("split") == s)
        out[s] = {m: ev.setMetricName(m).evaluate(part) for m in ("rmse", "mae", "r2")}
        if s != "train":
            base = part.withColumn("prediction", F.lit(mean))
            out[f"{s}_baseline"] = {m: ev.setMetricName(m).evaluate(base) for m in ("rmse", "mae", "r2")}
    scored.unpersist()
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--without-update", action="store_true", help="leave out the release v2 taxi file")
    parser.add_argument("--report", default=os.path.join(REPO, "reports", "comparison", "approach_a.json"))
    args = parser.parse_args()

    spark = session()
    timings = {}
    try:
        t0 = time.perf_counter()
        split = build_dataset(spark, not args.without_update)
        timings["dataset_sec"] = round(time.perf_counter() - t0, 1)

        dataset = spark.read.parquet(os.path.join(OUT, "trip_duration_dataset"))
        t1 = time.perf_counter()
        model, flagged = fit_model(dataset.where("split = 'train'"))
        timings["fit_sec"] = round(time.perf_counter() - t1, 1)
        model.write().overwrite().save(os.path.join(OUT, "model"))

        t2 = time.perf_counter()
        metrics = evaluate(model, dataset)
        timings["evaluate_sec"] = round(time.perf_counter() - t2, 1)
        timings["total_sec"] = round(time.perf_counter() - t0, 1)
    finally:
        spark.stop()

    result = {"release_v2": not args.without_update, "split": split, "missing_flags": flagged,
              "timings": timings, **metrics,
              "finished_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
    os.makedirs(os.path.dirname(args.report), exist_ok=True)
    with open(args.report, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=1, default=str)

    print(f"\nsplit      {split}")
    print(f"rows       {metrics['rows']}")
    for s in ("validation", "test"):
        m = metrics[s]
        print(f"{s:<10} rmse {m['rmse']:.4f}  mae {m['mae']:.4f}  r2 {m['r2']:.4f}  "
              f"(mean baseline rmse {metrics[s + '_baseline']['rmse']:.4f})")
    print(f"timings    {timings}")
    print(f"report     {os.path.relpath(args.report, REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
