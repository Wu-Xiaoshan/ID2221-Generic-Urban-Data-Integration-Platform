# Feature pipeline: trip_duration

- input: `ml/trip_duration_dataset` version 5
- output: `ml/trip_duration_features` version 1, 9,753,369 rows, one `features` vector of 639 slots each
- fitted pipeline: `ml/trip_duration_feature_pipeline` (14 stages)
- configuration fingerprint: `cf45e97e28b7`
- fit on train 71.8 s, transform and write 189.3 s, run 288.2 s (run `33430893-2b9e-447b-b2b8-379f050de243`)

## Stages

| # | stage | does |
|---:|---|---|
| 1 | SQLTransformer | SELECT *, hour(from_utc_timestamp(pickup_ts, 'America/New_York')) AS `pickup_hour_of_day`, dayofweek(from_u... |
| 2 | SQLTransformer | SELECT *, pickup_borough = dropoff_borough AS `same_borough`, concat_ws(' -> ', pickup_borough, dropoff_bor... |
| 3 | SQLTransformer | SELECT *, CASE WHEN weather_condition_code IN (1, 2) THEN 'Clear' WHEN weather_condition_code IN (3, 4) THE... |
| 4 | SQLTransformer | SELECT *, coalesce(weather_precipitation_mm > 0, weather_family IN ('Rain', 'Snow/Ice')) AS `is_precipitati... |
| 5 | SQLTransformer | SELECT *, log1p(CAST(`trip_distance_mi` AS DOUBLE)) AS `_f_num__trip_distance_mi`, CAST(CASE WHEN `passenge... |
| 6 | SQLTransformer | SELECT *, CAST(`_f_num__passenger_count` IS NULL OR isnan(`_f_num__passenger_count`) AS DOUBLE) AS `_f_miss... |
| 7 | GroupImputerModel | fill `_f_num__weather_precipitation_mm` with the training median of each `weather_family` |
| 8 | ImputerModel | fill numeric nulls |
| 9 | VectorAssembler | 10 inputs -> `_f_num_vec` |
| 10 | StandardScalerModel | standardise numerics |
| 11 | StringIndexerModel | index categories |
| 12 | OneHotEncoderModel | one-hot encode |
| 13 | VectorAssembler | 22 inputs -> `features` |
| 14 | SelectColumns | keeps `trip_id`, `pickup_date`, `split`, `label`, `features` |

## Where the vector comes from

| source dataset | features | slots | share of slots |
|---|---:|---:|---:|
| taxi_trips | 13 | 572 | 89.5% |
| taxi_zones | 6 | 51 | 8.0% |
| weather | 8 | 14 | 2.2% |
| air_quality | 2 | 2 | 0.3% |

## Features

Nulls are counted after declared missing codes are turned into nulls: they are the values
the imputer fills (numeric) or the `__missing__` category (categorical). Unseen values are
categories that never occur in the training split.

| feature | kind | source | preprocessing | steps | slots | missing train / val / test | unseen val / test |
|---|---|---|---|---:|---:|---|---|
| `passenger_count` | numeric | taxi_trips | 0 -> missing; cast to double; missing flag; impute median (1); scale (standard) | 5 | 2 | 6.55% / 11.87% / 15.12% | - |
| `weather_precipitation_mm` | numeric | weather | cast to double; log1p; missing flag; impute median by weather_family (Clear 0, Cloudy 0, Fog 0, Rain 0.405, Snow/Ice 0.262); scale (standard) | 5 | 2 | 8.05% / 3.51% / 10.41% | - |
| `ratecode_id` | categorical | taxi_trips | 99 -> missing; null -> __missing__; index (7 values + unseen); one-hot | 4 | 8 | 6.44% / 11.87% / 15.15% | 0 / 0 |
| `pickup_hour_of_day` | categorical | taxi_trips | derived (temporal); null -> __missing__; index (24 values + unseen); one-hot | 4 | 25 | 0 / 0 / 0 | 0 / 0 |
| `pickup_day_of_week` | categorical | taxi_trips | derived (temporal); null -> __missing__; index (7 values + unseen); one-hot | 4 | 8 | 0 / 0 / 0 | 0 / 0 |
| `pickup_month` | categorical | taxi_trips | derived (temporal); null -> __missing__; index (3 values + unseen); one-hot | 4 | 4 | 0 / 0 / 0 | 0 / 45.82% |
| `borough_pair` | categorical | taxi_zones | derived (zone_pair); null -> __missing__; index (33 values + unseen); one-hot | 4 | 34 | 0 / 0 / 0 | 0.00% / 0 |
| `weather_family` | categorical | weather | derived (code_groups); null -> __missing__; index (5 values + unseen); one-hot | 4 | 6 | 0 / 0 / 0 | 0 / 0 |
| `trip_distance_mi` | numeric | taxi_trips | cast to double; log1p; scale (standard) | 3 | 1 | 0 / 0 / 0 | - |
| `pickup_hour_sin` | numeric | taxi_trips | derived (temporal); cast to double; scale (standard) | 3 | 1 | 0 / 0 / 0 | - |
| `pickup_hour_cos` | numeric | taxi_trips | derived (temporal); cast to double; scale (standard) | 3 | 1 | 0 / 0 / 0 | - |
| `vendor_id` | categorical | taxi_trips | null -> __missing__; index (2 values + unseen); one-hot | 3 | 3 | 0 / 0 / 0 | 0 / 0 |
| `pu_location_id` | categorical | taxi_trips | null -> __missing__; index (256 values + unseen); one-hot | 3 | 257 | 0 / 0 / 0 | 0.00% / 0.00% |
| `do_location_id` | categorical | taxi_trips | null -> __missing__; index (259 values + unseen); one-hot | 3 | 260 | 0 / 0 / 0 | 0 / 0 |
| `pickup_borough` | categorical | taxi_zones | null -> __missing__; index (6 values + unseen); one-hot | 3 | 7 | 0 / 0 / 0 | 0 / 0 |
| `dropoff_borough` | categorical | taxi_zones | null -> __missing__; index (6 values + unseen); one-hot | 3 | 7 | 0 / 0 / 0 | 0 / 0 |
| `weather_temp_c` | numeric | weather | cast to double; scale (standard) | 2 | 1 | 0 / 0 / 0 | - |
| `weather_humidity_pct` | numeric | weather | cast to double; scale (standard) | 2 | 1 | 0 / 0 / 0 | - |
| `weather_wind_speed_kmh` | numeric | weather | cast to double; scale (standard) | 2 | 1 | 0 / 0 / 0 | - |
| `weather_cloud_cover_oktas` | numeric | weather | cast to double; scale (standard) | 2 | 1 | 0 / 0 / 0 | - |
| `aq_pm25_ugm3` | numeric | air_quality | cast to double; scale (standard) | 2 | 1 | 0 / 0 / 0 | - |
| `pickup_is_weekend` | binary | taxi_trips | derived (temporal); to 0/1, null -> 0 | 2 | 1 | - | - |
| `pickup_is_holiday` | binary | taxi_trips | derived (temporal); to 0/1, null -> 0 | 2 | 1 | - | - |
| `same_borough` | binary | taxi_zones | derived (zone_pair); to 0/1, null -> 0 | 2 | 1 | - | - |
| `pickup_is_airport` | binary | taxi_zones | derived (zone_pair); to 0/1, null -> 0 | 2 | 1 | - | - |
| `dropoff_is_airport` | binary | taxi_zones | derived (zone_pair); to 0/1, null -> 0 | 2 | 1 | - | - |
| `is_precipitating` | binary | weather | derived (expression); to 0/1, null -> 0 | 2 | 1 | - | - |
| `weather_is_missing` | binary | weather | to 0/1, null -> 0 | 1 | 1 | - | - |
| `air_quality_is_missing` | binary | air_quality | to 0/1, null -> 0 | 1 | 1 | - | - |

Missing flags were added for: `passenger_count`, `weather_precipitation_mm`.
