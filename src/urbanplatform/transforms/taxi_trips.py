"""Dataset-specific rules for taxi_trips."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import DatasetConfig
from ..registry import pre_quality


@pre_quality("taxi_trips")
def derive_trip_columns(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    """Build the surrogate key and the derived columns the quality rules test.

    The feed publishes no trip identifier, so the key is a hash of the columns that
    together identify a trip. A hash rather than a generated id, because it makes
    re-ingesting the same file a no-op instead of a duplication: the same input row
    always produces the same key.

    xxhash64 rather than sha2-256, on measured grounds. sha2 returns 64 hex characters
    of high-entropy text: at 9.5M rows that one column cost 589 MB of a 888 MB table,
    two thirds of the total, and it is paid again by every scan. A 64-bit key is 8
    bytes. The trade is a collision probability of about n^2 / 2^65, which for 9.5M
    rows is 2.6e-6 -- far below the rate of genuine duplicates the rule already handles,
    and this is not a cryptographic use: nothing depends on the key being hard to
    forge, only on it being stable and unique enough.
    """
    df = df.withColumn(
        "trip_id",
        F.xxhash64(
            F.col("vendor_id").cast("string"),
            F.col("pickup_ts").cast("string"),
            F.col("dropoff_ts").cast("string"),
            F.col("pu_location_id").cast("string"),
            F.col("do_location_id").cast("string"),
            F.col("total_amount").cast("string"),
        ),
    )

    df = df.withColumn(
        "trip_duration_min",
        (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0,
    )

    # Join keys are UTC; calendar and reporting columns are New York local.
    #
    # These are different jobs and deriving both from UTC gets one of them wrong. The
    # hour is joined against weather and air quality, which are absolute instants, so it
    # must be UTC. The date and month are what an analyst means by "trips in March" or
    # "average duration per day" -- and March in New York ends at 04:00Z on 1 April, so
    # a UTC-derived month puts the last four hours of local March, about 16,700 trips,
    # into an April partition.
    pickup_local = F.from_utc_timestamp(F.col("pickup_ts"), "America/New_York")
    df = df.withColumn("pickup_date", F.to_date(pickup_local))
    df = df.withColumn("pickup_year_month", F.date_format(pickup_local, "yyyy-MM"))
    df = df.withColumn("pickup_hour", F.date_trunc("hour", F.col("pickup_ts")))

    # A source column that feeds a derived column is dropped by the transform that
    # derives it, not by the config's `drop` list, because `drop` runs during
    # standardization and would remove it before this code could read it.
    df = df.withColumn("is_store_and_fwd", F.col("store_and_fwd_flag") == F.lit("Y"))
    df = df.drop("store_and_fwd_flag")

    # Zones 264 and 265 are 'unknown' and 'outside NYC' rather than real places. Marked
    # here so borough-level reporting can exclude them instead of inventing a borough.
    unlocated = F.col("pu_location_id").isin(264, 265) | F.col("do_location_id").isin(264, 265)
    return df.withColumn("is_unlocated", unlocated)
