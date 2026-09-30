# Model: trip_duration

Minutes from pickup to drop-off of a yellow taxi trip, predicted when the trip starts.

- model: `linear_regression` (regParam=0.01, elasticNetParam=0.0), version 2 of `ml/models/trip_duration_model`
- lineage: `gold/integrated_taxi_trips` v7 -> `ml/trip_duration_dataset` v5 -> `ml/models/trip_duration_model/v2`
- pipeline: 15 stages (14 from the feature pipeline, then `LinearRegressionModel`), 639 feature slots, saved at `ml/models/trip_duration_model/v2/pipeline`
- configuration fingerprint: `cb40964b99ce`
- fit on train 352.0 s, scoring and evaluation 316.5 s, run 680.1 s (run `cc0585fc-73c5-4cfa-b5c1-aa70be5a8df5`)

## Evaluation

Train holds the trips picked up before 2024-03-11, validation those from 2024-03-11 and test those from 2024-03-25 on.

| split | rows | RMSE | MAE | R² |
|---|---:|---:|---:|---:|
| train | 6,797,379 | 5.664 | 3.655 | 0.7740 |
| validation | 1,551,915 | 6.158 | 3.977 | 0.7702 |
| test | 1,404,075 | 6.586 | 4.150 | 0.7468 |
| validation, mean baseline | 1,551,915 | 12.877 | 8.719 | -0.0050 |
| test, mean baseline | 1,404,075 | 13.139 | 8.815 | -0.0078 |

The mean baseline predicts the training mean, 15.21 min, for every trip.

## Against the previous version

Version 1, trained on `ml/trip_duration_dataset` v3 (gold v3), scored on this version's test split:

| version | RMSE | MAE | R² |
|---|---:|---:|---:|
| v1 (previous) | 6.609 | 4.136 | 0.7450 |
| v2 (this run) | 6.586 | 4.150 | 0.7468 |

## Test error by `pickup_borough`

| pickup_borough | trips | mean duration | RMSE | MAE |
|---|---:|---:|---:|---:|
| Manhattan | 1,263,056 | 14.20 | 5.483 | 3.638 |
| Queens | 118,492 | 36.53 | 12.322 | 8.518 |
| Brooklyn | 17,950 | 30.62 | 14.467 | 9.654 |
| Bronx | 4,533 | 34.64 | 16.086 | 10.858 |
| Staten Island | 40 | 25.36 | 8.740 | 6.782 |
| EWR | 4 | 19.09 | 4.944 | 3.167 |

## Coefficients by feature

Intercept 18.83 min; 626 of 639 coefficients are non-zero. A numeric feature is standardised, so its coefficient is minutes per standard
deviation (of `log1p` of it, where configured). A binary feature's is minutes when it is 1.

| feature | source | coefficient |
|---|---|---:|
| `trip_distance_mi` | taxi_trips | +10.101 |
| `passenger_count (missing flag)` | taxi_trips | -5.958 |
| `pickup_hour_cos` | taxi_trips | -1.928 |
| `pickup_is_holiday` | taxi_trips | -1.825 |
| `dropoff_is_airport` | taxi_zones | -1.462 |
| `pickup_is_airport` | taxi_zones | -1.060 |
| `pickup_hour_sin` | taxi_trips | -0.958 |
| `same_borough` | taxi_zones | -0.705 |
| `pickup_is_weekend` | taxi_trips | -0.705 |
| `is_precipitating` | weather | +0.579 |
| `weather_humidity_pct` | weather | -0.364 |
| `weather_cloud_cover_oktas` | weather | +0.245 |
| `weather_precipitation_mm (missing flag)` | weather | -0.233 |
| `aq_pm25_ugm3` | air_quality | +0.112 |
| `passenger_count` | taxi_trips | +0.091 |
| `weather_temp_c` | weather | -0.058 |
| `weather_precipitation_mm` | weather | +0.049 |
| `weather_wind_speed_kmh` | weather | -0.046 |
| `weather_is_missing` | weather | +0.000 |
| `air_quality_is_missing` | air_quality | +0.000 |

A one-hot feature's coefficients are minutes added when that category is present. The categories
of one feature share their level with the intercept, so only their spread within the feature means
something; the table orders the features by it.

| feature | source | slots | lowest | highest |
|---|---|---:|---|---|
| `pu_location_id` | taxi_trips | 257 | -20.85 (204) | +22.01 (117) |
| `do_location_id` | taxi_trips | 260 | -15.54 (154) | +9.84 (109) |
| `borough_pair` | taxi_zones | 34 | -5.85 (Staten Island -> Brooklyn) | +18.73 (Brooklyn -> Bronx) |
| `ratecode_id` | taxi_trips | 8 | -4.11 (3) | +4.01 (__missing__) |
| `pickup_hour_of_day` | taxi_trips | 25 | -3.66 (5) | +0.99 (1) |
| `pickup_borough` | taxi_zones | 7 | -0.98 (Staten Island) | +2.07 (Bronx) |
| `pickup_day_of_week` | taxi_trips | 8 | -1.48 (1) | +0.64 (5) |
| `dropoff_borough` | taxi_zones | 7 | -1.03 (EWR) | +0.48 (Brooklyn) |
| `weather_family` | weather | 6 | -0.42 (Snow/Ice) | +0.70 (Fog) |
| `vendor_id` | taxi_trips | 3 | -0.53 (2) | +0.53 (1) |
| `pickup_month` | taxi_trips | 4 | -0.37 (1) | +0.53 (3) |

## Versions

Every run saves a new version; the latest is the one `load_model` returns. RMSE of each version is on its own test split, so versions trained on different data are
compared in the section above instead.

| version | trained (UTC) | gold version | dataset version | train rows | test from | validation RMSE | test RMSE | fingerprint |
|---:|---|---:|---:|---:|---|---:|---:|---|
| v1 | 2026-09-30 15:25 | 3 | 3 | 6,109,158 | 2024-03-19 | 6.189 | 6.286 | `cb40964b99ce` |
| v2 | 2026-09-30 15:35 | 7 | 5 | 6,797,379 | 2024-03-25 | 6.158 | 6.586 | `cb40964b99ce` |
