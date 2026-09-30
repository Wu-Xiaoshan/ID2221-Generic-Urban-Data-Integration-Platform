# Training dataset: trip_duration

Minutes from pickup to drop-off of a yellow taxi trip, predicted when the trip starts.

- source: `gold/integrated_taxi_trips` version 7
- output: `ml/trip_duration_dataset` version 5
- configuration fingerprint: `c3979729f2a1`
- label: `trip_duration_min`
- built 2026-09-30 15:33 UTC in 81.1 s (run `be541ec1-8b28-480b-8cb3-bced752095f2`)

## Filters

A row can fail several filters, so the failures do not add up to the rows removed.

| filter | condition | rows failing | % of source |
|---|---|---:|---:|
| located | `NOT is_unlocated` | 99,941 | 0.98% |
| plausible_duration | `trip_duration_min BETWEEN 1 AND 180` | 120,586 | 1.18% |
| positive_distance | `trip_distance_mi > 0 AND trip_distance_mi <= 100` | 222,283 | 2.17% |
| plausible_speed | `trip_distance_mi / (trip_duration_min / 60) BETWEEN 1 AND 80` | 238,913 | 2.34% |
| not_voided | `NOT dq_negative_total` | 124,520 | 1.22% |
| label_present | `(trip_duration_min) IS NOT NULL` | 0 | 0.00% |
| **kept** | | **9,753,369** | **95.43%** of 10,220,529 |

## Splits

Strategy `chronological`, validation from 2024-03-11, test from 2024-03-25 (counted back from 2024-04-07).

| split | rows | share | first | last | label mean | std | p5 | median | p95 |
|---|---:|---:|---|---|---:|---:|---:|---:|---:|
| train | 6,797,379 | 69.7% | 2024-01-01 | 2024-03-10 | 15.21 | 11.91 | 3.67 | 11.95 | 38.25 |
| validation | 1,551,915 | 15.9% | 2024-03-11 | 2024-03-24 | 16.12 | 12.85 | 3.77 | 12.60 | 41.32 |
| test | 1,404,075 | 14.4% | 2024-03-25 | 2024-04-07 | 16.36 | 13.09 | 3.82 | 12.80 | 41.82 |

## Columns

| column | from dataset | nulls (train) | nulls (validation) | nulls (test) |
|---|---|---:|---:|---:|
| `pickup_ts` | taxi_trips | 0 | 0 | 0 |
| `trip_distance_mi` | taxi_trips | 0 | 0 | 0 |
| `passenger_count` | taxi_trips | 372,651 (5.48%) | 167,251 (10.78%) | 197,536 (14.07%) |
| `vendor_id` | taxi_trips | 0 | 0 | 0 |
| `ratecode_id` | taxi_trips | 372,651 (5.48%) | 167,251 (10.78%) | 197,536 (14.07%) |
| `pu_location_id` | taxi_trips | 0 | 0 | 0 |
| `do_location_id` | taxi_trips | 0 | 0 | 0 |
| `pickup_borough` | taxi_zones | 0 | 0 | 0 |
| `dropoff_borough` | taxi_zones | 0 | 0 | 0 |
| `weather_temp_c` | weather | 0 | 0 | 0 |
| `weather_humidity_pct` | weather | 0 | 0 | 0 |
| `weather_precipitation_mm` | weather | 547,499 (8.05%) | 54,419 (3.51%) | 146,170 (10.41%) |
| `weather_wind_speed_kmh` | weather | 0 | 0 | 0 |
| `weather_cloud_cover_oktas` | weather | 0 | 0 | 0 |
| `weather_condition_code` | weather | 0 | 0 | 0 |
| `weather_is_missing` | weather | 0 | 0 | 0 |
| `aq_pm25_ugm3` | air_quality | 0 | 0 | 0 |
| `air_quality_is_missing` | air_quality | 0 | 0 | 0 |
