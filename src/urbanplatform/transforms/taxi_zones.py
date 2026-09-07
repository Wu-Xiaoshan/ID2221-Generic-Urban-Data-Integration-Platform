"""Dataset-specific rules for taxi_zones.

This dataset needs almost nothing: it arrives with a clean natural key, no timestamps
and no type problems. The only rule marks the two sentinel rows, and even that exists
for the benefit of downstream reporting rather than for the table itself.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..config import DatasetConfig
from ..registry import pre_quality


@pre_quality("taxi_zones")
def mark_sentinels(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    """Flag the rows that are not real zones.

    264 is 'Unknown' and 265 is 'Outside of NYC'. Both join successfully against trips,
    so without a marker they would silently become boroughs in a group-by. Note that
    'N/A' in this file is a literal value, not a missing one; the reader config sets the
    null token explicitly so it survives.
    """
    return df.withColumn("is_real_zone", ~F.col("location_id").isin(264, 265))
