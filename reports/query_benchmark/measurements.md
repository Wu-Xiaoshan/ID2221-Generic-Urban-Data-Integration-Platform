# Query optimisation measurements

Generated 2026-09-18 00:46. Median of 3 interleaved repeats after a discarded warm-up.

| Experiment | Technique | Baseline | Optimised | Speed-up | Change | Same result? |
|---|---|---:|---:|---:|---:|:--:|
| `partition_pruning_vs_skipping` | Partition pruning vs Delta data skipping | 3.35 s | 3.31 s | 1.01x | -1.2% | yes |
| `partition_pruning_vs_full_scan` | Partition pruning | 3.23 s | 3.24 s | 1.00x | +0.3% | yes |
| `broadcast_join` | Broadcast join | 11.50 s | 10.91 s | 1.05x | -5.2% | yes |
| `prejoined_vs_runtime_join` | Materialisation (Week 1's integrated table) | 11.05 s | 9.72 s | 1.14x | -12.1% | yes |
| `caching` | Caching | 7.08 s | 0.57 s | 12.43x | -92.0% | yes |
| `aqe` | Adaptive Query Execution | 4.06 s | 3.84 s | 1.06x | -5.5% | yes |
| `control_tiny_join` | Control (broadcast setting on a join of two tiny aggregates) | 4.31 s | 4.27 s | 1.01x | -0.9% | yes |
| `aqe_q1_zone_monthly_demand` | AQE sweep | 4.13 s | 3.86 s | 1.07x | -6.4% | yes |
| `aqe_q1_zone_demand_one_month` | AQE sweep | 3.27 s | 3.19 s | 1.03x | -2.6% | yes |
| `aqe_q2_weather_distance` | AQE sweep | 12.06 s | 11.86 s | 1.02x | -1.6% | yes |
| `aqe_q2_weather_distance_silver` | AQE sweep | 12.44 s | 12.67 s | 0.98x | +1.9% | yes |
| `aqe_q2_precip_distance` | AQE sweep | 12.34 s | 12.98 s | 0.95x | +5.2% | yes |
| `aqe_q3_aq_demand_buckets` | AQE sweep | 3.29 s | 3.15 s | 1.05x | -4.4% | yes |
| `aqe_q3_aq_demand_correlation` | AQE sweep | 2.54 s | 2.59 s | 0.98x | +2.0% | yes |
| `aqe_q3_aq_epa_categories` | AQE sweep | 1.80 s | 1.88 s | 0.96x | +4.5% | yes |
| `aqe_q4_weather_sensitive_zones` | AQE sweep | 4.95 s | 4.48 s | 1.10x | -9.5% | yes |
| `aqe_q5_peak_hours` | AQE sweep | 5.16 s | 4.65 s | 1.11x | -9.8% | yes |
| `aqe_q6_monthly_trend` | AQE sweep | 0.89 s | 0.83 s | 1.07x | -6.4% | yes |
| `aqe_q6_daily_trend` | AQE sweep | 0.44 s | 0.39 s | 1.14x | -12.2% | yes |
| `product_zone_monthly_statistics` | Materialised product vs recomputation | 1.37 s | 0.29 s | 4.65x | -78.5% | yes |
| `product_weather_impact_summary` | Materialised product vs recomputation | 1.49 s | 0.39 s | 3.81x | -73.7% | yes |
| `product_hourly_air_quality_demand` | Materialised product vs recomputation | 2.54 s | 0.12 s | 21.18x | -95.3% | yes |

## partition_pruning_vs_skipping

**Partition pruning vs Delta data skipping.** Does expressing the month as the partition key beat expressing it as a date range?

Identical result sets; the only difference is which column carries the restriction.

| | Baseline | Optimised |
|---|---|---|
| variant | date range on pickup_date | equality on pickup_year_month |
| median | 3.351 s | 3.310 s |
| samples | 3.375, 3.351, 3.298 | 3.424, 3.310, 3.294 |
| plan | 2 shuffles | PartitionFilters, 2 shuffles |
| PartitionFilters | `-` | `isnotnull(pickup_year_month#70), (pickup_year_month#70 = 2024-02)` |
| AQEShuffleRead (executed plan) | 2 nodes, coalesced | 2 nodes, coalesced |

Results verified identical.

## partition_pruning_vs_full_scan

**Partition pruning.** What does pruning save when nothing else can skip the data?

Wrapping the column in a function is what makes the baseline honest: Spark cannot rewrite MONTH(col) = 2 into a range, so neither partition elimination nor file statistics can eliminate anything.

| | Baseline | Optimised |
|---|---|---|
| variant | MONTH(pickup_date) = 2, unskippable | equality on pickup_year_month |
| median | 3.231 s | 3.240 s |
| samples | 3.231, 3.220, 3.241 | 3.261, 3.240, 3.220 |
| plan | 2 shuffles | PartitionFilters, 2 shuffles |
| PartitionFilters | `-` | `isnotnull(pickup_year_month#70), (pickup_year_month#70 = 2024-02)` |
| AQEShuffleRead (executed plan) | 2 nodes, coalesced | 2 nodes, coalesced |

Results verified identical.

## broadcast_join

**Broadcast join.** What does broadcasting the weather dimension save over shuffling both sides?

| | Baseline | Optimised |
|---|---|---|
| variant | shuffle join (threshold = -1) | broadcast join (threshold = 32 MB) |
| median | 11.504 s | 10.905 s |
| samples | 11.504, 11.550, 11.318 | 10.905, 10.990, 10.836 |
| plan | SortMergeJoin, 4 shuffles | BroadcastHashJoin, 3 shuffles, 1 broadcast |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | 4 nodes, coalesced | 3 nodes, coalesced |

Results verified identical.

## prejoined_vs_runtime_join

**Materialisation (Week 1's integrated table).** What did pre-joining weather into the gold table buy?

| | Baseline | Optimised |
|---|---|---|
| variant | join weather at query time, from silver | read the pre-joined gold table |
| median | 11.051 s | 9.715 s |
| samples | 11.051, 11.140, 10.929 | 9.533, 9.868, 9.715 |
| plan | BroadcastHashJoin, 3 shuffles, 1 broadcast | 3 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | 3 nodes, coalesced | 3 nodes, coalesced |

Results verified identical.

## caching

**Caching.** Does caching the hourly intermediate pay for itself across three consumers?

| | Baseline | Optimised |
|---|---|---|
| variant | uncached, three consumers | cached hourly_demand, three consumers |
| median | 7.083 s | 0.570 s |
| samples | 7.179, 7.083, 6.823 | 0.614, 0.570, 0.567 |
| setup | 0.000 s | 3.652 s |
| plan | 3 shuffles | InMemoryTableScan, 6 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | 3 nodes, coalesced | 4 nodes, coalesced |

Results verified identical.

## aqe

**Adaptive Query Execution.** Does AQE help Q4's shuffle-heavy aggregations?

AQE's skew handling only splits partitions of shuffle joins, so Q4's skewed aggregations cannot trigger it.

| | Baseline | Optimised |
|---|---|---|
| variant | AQE disabled | AQE enabled |
| median | 4.063 s | 3.839 s |
| samples | 4.099, 4.063, 4.054 | 3.865, 3.818, 3.839 |
| plan | SortMergeJoin, 6 shuffles | SortMergeJoin, 6 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 5 nodes, coalesced |

Results verified identical.

## control_tiny_join

**Control (broadcast setting on a join of two tiny aggregates).** Does a changed plan always mean a changed runtime?

The plan changes and the runtime does not. Judge noise from the samples of each row, not from this row alone.

| | Baseline | Optimised |
|---|---|---|
| variant | threshold = -1 | threshold = 32 MB |
| median | 4.308 s | 4.271 s |
| samples | 4.308, 4.323, 4.289 | 4.338, 4.252, 4.271 |
| plan | SortMergeJoin, 5 shuffles | BroadcastHashJoin, 5 shuffles, 1 broadcast |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | 5 nodes, coalesced | 5 nodes, coalesced |

Results verified identical.

## aqe_q1_zone_monthly_demand

**AQE sweep.** AQE on/off for q1_zone_monthly_demand

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 4.126 s | 3.860 s |
| samples | 4.121, 4.343, 4.126 | 3.721, 4.550, 3.860 |
| plan | SortMergeJoin, PartitionFilters, 4 shuffles | SortMergeJoin, PartitionFilters, 5 shuffles |
| PartitionFilters | `isnotnull(pickup_year_month#41900), dynamicpruningexpression(true)` | `isnotnull(pickup_year_month#43263), dynamicpruningexpression(pickup_year_month#43263 IN dynamicpruning#43457)` |
| AQEShuffleRead (executed plan) | none | 4 nodes, coalesced |

Results verified identical.

## aqe_q1_zone_demand_one_month

**AQE sweep.** AQE on/off for q1_zone_demand_one_month

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 3.273 s | 3.189 s |
| samples | 3.282, 3.267, 3.273 | 3.189, 3.186, 3.220 |
| plan | PartitionFilters, 2 shuffles | PartitionFilters, 2 shuffles |
| PartitionFilters | `isnotnull(pickup_year_month#70), (pickup_year_month#70 = 2024-02)` | `isnotnull(pickup_year_month#70), (pickup_year_month#70 = 2024-02)` |
| AQEShuffleRead (executed plan) | none | 2 nodes, coalesced |

Results verified identical.

## aqe_q2_weather_distance

**AQE sweep.** AQE on/off for q2_weather_distance

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 12.056 s | 11.863 s |
| samples | 9.789, 12.056, 12.510 | 10.521, 12.918, 11.863 |
| plan | 3 shuffles | 3 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 3 nodes, coalesced |

Results verified identical.

## aqe_q2_weather_distance_silver

**AQE sweep.** AQE on/off for q2_weather_distance_silver

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 12.440 s | 12.673 s |
| samples | 13.893, 12.440, 11.869 | 13.047, 12.108, 12.673 |
| plan | BroadcastHashJoin, 3 shuffles, 1 broadcast | BroadcastHashJoin, 3 shuffles, 1 broadcast |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 3 nodes, coalesced |

Results verified identical.

## aqe_q2_precip_distance

**AQE sweep.** AQE on/off for q2_precip_distance

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 12.337 s | 12.979 s |
| samples | 12.738, 12.245, 12.337 | 13.173, 12.979, 12.230 |
| plan | 3 shuffles | 3 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 3 nodes, coalesced |

Results verified identical.

## aqe_q3_aq_demand_buckets

**AQE sweep.** AQE on/off for q3_aq_demand_buckets

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 3.293 s | 3.149 s |
| samples | 3.240, 3.354, 3.293 | 3.149, 3.473, 3.126 |
| plan | 4 shuffles | 4 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 4 nodes, coalesced |

Results verified identical.

## aqe_q3_aq_demand_correlation

**AQE sweep.** AQE on/off for q3_aq_demand_correlation

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 2.544 s | 2.595 s |
| samples | 2.544, 2.632, 2.536 | 2.595, 2.519, 2.658 |
| plan | 3 shuffles | 3 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 2 nodes, coalesced |

Results verified identical.

## aqe_q3_aq_epa_categories

**AQE sweep.** AQE on/off for q3_aq_epa_categories

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 1.801 s | 1.883 s |
| samples | 1.801, 1.701, 1.829 | 1.751, 1.883, 1.983 |
| plan | 3 shuffles | 3 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 3 nodes, coalesced |

Results verified identical.

## aqe_q4_weather_sensitive_zones

**AQE sweep.** AQE on/off for q4_weather_sensitive_zones

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 4.948 s | 4.478 s |
| samples | 4.419, 4.948, 4.949 | 4.213, 4.592, 4.478 |
| plan | SortMergeJoin, 6 shuffles | SortMergeJoin, 6 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 5 nodes, coalesced |

Results verified identical.

## aqe_q5_peak_hours

**AQE sweep.** AQE on/off for q5_peak_hours

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 5.155 s | 4.650 s |
| samples | 5.288, 5.155, 5.118 | 5.381, 4.650, 4.596 |
| plan | BroadcastHashJoin, 5 shuffles, 1 broadcast | BroadcastHashJoin, 5 shuffles, 1 broadcast |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 5 nodes, coalesced |

Results verified identical.

## aqe_q6_monthly_trend

**AQE sweep.** AQE on/off for q6_monthly_trend

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 0.886 s | 0.829 s |
| samples | 0.886, 0.811, 0.914 | 0.925, 0.829, 0.753 |
| plan | 4 shuffles | 4 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 4 nodes, coalesced |

Results verified identical.

## aqe_q6_daily_trend

**AQE sweep.** AQE on/off for q6_daily_trend

| | Baseline | Optimised |
|---|---|---|
| variant | AQE off | AQE on |
| median | 0.443 s | 0.389 s |
| samples | 0.419, 0.470, 0.443 | 0.389, 0.387, 0.421 |
| plan | 2 shuffles | 2 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | none | 1 node, coalesced |

Results verified identical.

## product_zone_monthly_statistics

**Materialised product vs recomputation.** What does reading zone_monthly_statistics save over recomputing Q1?

| | Baseline | Optimised |
|---|---|---|
| variant | compute Q1 from the gold table | read zone_monthly_statistics |
| median | 1.370 s | 0.295 s |
| samples | 1.460, 1.356, 1.370 | 0.295, 0.300, 0.268 |
| plan | SortMergeJoin, PartitionFilters, 4 shuffles | 1 shuffle |
| PartitionFilters | `isnotnull(pickup_year_month#92010), dynamicpruningexpression(pickup_year_month#92010 IN dynamicpruning#92204)` | `-` |
| AQEShuffleRead (executed plan) | 3 nodes, coalesced | 1 node, coalesced |

Results verified identical.

## product_weather_impact_summary

**Materialised product vs recomputation.** What does weather_impact_summary save over recomputing Q4's ranking?

| | Baseline | Optimised |
|---|---|---|
| variant | compute Q4 from the gold table | recompute from weather_impact_summary |
| median | 1.490 s | 0.391 s |
| samples | 1.552, 1.458, 1.490 | 0.350, 0.391, 0.398 |
| plan | SortMergeJoin, 6 shuffles | 2 shuffles |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | 5 nodes, coalesced | 2 nodes, coalesced |

Results verified identical.

## product_hourly_air_quality_demand

**Materialised product vs recomputation.** What does hourly_air_quality_demand save over recomputing Q3's correlations?

| | Baseline | Optimised |
|---|---|---|
| variant | compute Q3 from the gold table | recompute from hourly_air_quality_demand |
| median | 2.541 s | 0.120 s |
| samples | 2.541, 1.327, 2.652 | 0.119, 0.120, 0.284 |
| plan | 3 shuffles | 1 shuffle |
| PartitionFilters | `-` | `-` |
| AQEShuffleRead (executed plan) | 2 nodes, coalesced | none |

Results verified identical.

