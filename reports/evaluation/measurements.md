# Platform evaluation measurements

Generated 2026-09-27 04:33. Wall-clock seconds per process, including JVM start-up; median of 3 interleaved repeats, order reversed on every other repeat. Every run starts from a restored snapshot.

## Runs

| Run | Snapshot | Command | Overrides | Median | Samples | Exit codes |
|---|---|---|---|---:|---|---|
| `update` | A | `jobs/run_update.py v2` | `-` | 100.52 s | 213.4, 100.1, 100.5 | 0,0,0 |
| `update_no_metrics` | A | `jobs/run_update.py v2` | `{"validation": {"metrics": false}}` | 102.22 s | 186.3, 102.2, 99.3 | 0,0,0 |
| `update_no_validation` | A | `jobs/run_update.py v2` | `{"validation": {"enabled": false}}` | 79.24 s | 146.7, 79.2, 77.2 | 0,0,0 |
| `update_no_monitoring` | A | `jobs/run_update.py v2` | `{"monitoring": {"enabled": false}}` | 87.31 s | 186.9, 87.3, 83.7 | 0,0,0 |
| `ingest` | A | `jobs/run_ingest.py` | `-` | 213.46 s | 303.9, 213.5, 207.3 | 0,0,0 |
| `ingest_no_metrics` | A | `jobs/run_ingest.py` | `{"validation": {"metrics": false}}` | 198.04 s | 198.0, 194.8, 214.3 | 0,0,0 |
| `ingest_no_validation` | A | `jobs/run_ingest.py` | `{"validation": {"enabled": false}}` | 137.24 s | 140.0, 137.2, 136.6 | 0,0,0 |
| `refresh` | B | `jobs/run_refresh.py` | `-` | 148.43 s | 148.4, 144.9, 155.8 | 0,0,0 |
| `refresh_full` | B | `jobs/run_refresh.py --full` | `-` | 248.64 s | 248.6, 237.2, 251.1 | 0,0,0 |
| `refresh_no_monitoring` | B | `jobs/run_refresh.py` | `{"monitoring": {"enabled": false}}` | 130.87 s | 130.9, 130.2, 137.6 | 0,0,0 |
| `update_repeat` | B | `jobs/run_update.py v2` | `-` | 79.58 s | 79.6, 77.0, 85.1 | 0,0,0 |
| `refresh_noop` | C | `jobs/run_refresh.py` | `-` | 58.06 s | 55.6, 58.1, 61.9 | 0,0,0 |

## Comparisons

### Incremental update against a full re-ingest

| Run | Median | Against the first | Share of the first |
|---|---:|---:|---:|
| `ingest` | 213.46 s | +0.00 s | 100.0% |
| `update` | 100.52 s | -112.94 s | 47.1% |

### Analytical refresh: incremental against full

| Run | Median | Against the first | Share of the first |
|---|---:|---:|---:|
| `refresh_full` | 248.64 s | +0.00 s | 100.0% |
| `refresh` | 148.43 s | -100.21 s | 59.7% |

### Validation overhead on the v2 update

| Run | Median | Against the first | Share of the first |
|---|---:|---:|---:|
| `update` | 100.52 s | +0.00 s | 100.0% |
| `update_no_metrics` | 102.22 s | +1.69 s | 101.7% |
| `update_no_validation` | 79.24 s | -21.28 s | 78.8% |

### Validation overhead on the full load

| Run | Median | Against the first | Share of the first |
|---|---:|---:|---:|
| `ingest` | 213.46 s | +0.00 s | 100.0% |
| `ingest_no_metrics` | 198.04 s | -15.43 s | 92.8% |
| `ingest_no_validation` | 137.24 s | -76.23 s | 64.3% |

### Monitoring overhead on the v2 update

| Run | Median | Against the first | Share of the first |
|---|---:|---:|---:|
| `update` | 100.52 s | +0.00 s | 100.0% |
| `update_no_monitoring` | 87.31 s | -13.21 s | 86.9% |

### Monitoring overhead on the refresh

| Run | Median | Against the first | Share of the first |
|---|---:|---:|---:|
| `refresh` | 148.43 s | +0.00 s | 100.0% |
| `refresh_no_monitoring` | 130.87 s | -17.56 s | 88.2% |

### An update with new data against one with none

| Run | Median | Against the first | Share of the first |
|---|---:|---:|---:|
| `update` | 100.52 s | +0.00 s | 100.0% |
| `update_repeat` | 79.58 s | -20.94 s | 79.2% |

### A refresh with changes against one with none

| Run | Median | Against the first | Share of the first |
|---|---:|---:|---:|
| `refresh` | 148.43 s | +0.00 s | 100.0% |
| `refresh_noop` | 58.06 s | -90.37 s | 39.1% |

## Storage

Live is what the current version of a table holds. Disk adds the superseded versions and the change data feed, which Delta keeps until VACUUM.

### A: initial load

| Table | Versions | Live | Change data | Log | On disk |
|---|---:|---:|---:|---:|---:|
| `gold/integrated_taxi_trips` | 4 | 452.92 MB | 0.00 MB | 0.04 MB | 913.18 MB |
| `products/daily_mobility_summary` | 2 | 0.04 MB | 0.00 MB | 0.01 MB | 0.05 MB |
| `products/hourly_air_quality_demand` | 2 | 0.06 MB | 0.00 MB | 0.01 MB | 0.07 MB |
| `products/weather_impact_summary` | 2 | 0.05 MB | 0.00 MB | 0.01 MB | 0.06 MB |
| `products/zone_monthly_statistics` | 2 | 0.06 MB | 0.00 MB | 0.01 MB | 0.07 MB |
| `silver/air_quality_hourly` | 2 | 0.11 MB | 0.00 MB | 0.00 MB | 0.12 MB |
| `silver/taxi_trips` | 3 | 359.22 MB | 0.00 MB | 0.02 MB | 724.03 MB |
| `silver/taxi_zones` | 2 | 0.01 MB | 0.00 MB | 0.00 MB | 0.01 MB |
| `silver/weather_hourly` | 2 | 0.16 MB | 0.00 MB | 0.01 MB | 0.16 MB |
| **total** | | 812.64 MB | 0.00 MB | 0.11 MB | 1,637.75 MB |

### B: after the v2 update

| Table | Versions | Live | Change data | Log | On disk |
|---|---:|---:|---:|---:|---:|
| `gold/integrated_taxi_trips` | 4 | 452.92 MB | 0.00 MB | 0.04 MB | 913.18 MB |
| `products/daily_mobility_summary` | 2 | 0.04 MB | 0.00 MB | 0.01 MB | 0.05 MB |
| `products/hourly_air_quality_demand` | 2 | 0.06 MB | 0.00 MB | 0.01 MB | 0.07 MB |
| `products/weather_impact_summary` | 2 | 0.05 MB | 0.00 MB | 0.01 MB | 0.06 MB |
| `products/zone_monthly_statistics` | 2 | 0.06 MB | 0.00 MB | 0.01 MB | 0.07 MB |
| `silver/air_quality_hourly` | 4 | 0.12 MB | 0.00 MB | 0.01 MB | 0.26 MB |
| `silver/taxi_trips` | 6 | 383.79 MB | 0.00 MB | 0.03 MB | 798.33 MB |
| `silver/taxi_zones` | 2 | 0.01 MB | 0.00 MB | 0.00 MB | 0.01 MB |
| `silver/weather_hourly` | 4 | 0.18 MB | 0.00 MB | 0.01 MB | 0.37 MB |
| **total** | | 837.24 MB | 0.00 MB | 0.13 MB | 1,712.40 MB |

### C: after the refresh

| Table | Versions | Live | Change data | Log | On disk |
|---|---:|---:|---:|---:|---:|
| `gold/integrated_taxi_trips` | 8 | 483.47 MB | 0.00 MB | 0.05 MB | 1,005.60 MB |
| `products/daily_mobility_summary` | 4 | 0.04 MB | 0.00 MB | 0.02 MB | 0.06 MB |
| `products/hourly_air_quality_demand` | 4 | 0.07 MB | 0.00 MB | 0.02 MB | 0.15 MB |
| `products/weather_impact_summary` | 4 | 0.05 MB | 0.00 MB | 0.01 MB | 0.12 MB |
| `products/zone_monthly_statistics` | 4 | 0.08 MB | 0.00 MB | 0.01 MB | 0.10 MB |
| `silver/air_quality_hourly` | 4 | 0.12 MB | 0.00 MB | 0.01 MB | 0.26 MB |
| `silver/taxi_trips` | 6 | 383.79 MB | 0.00 MB | 0.03 MB | 798.33 MB |
| `silver/taxi_zones` | 2 | 0.01 MB | 0.00 MB | 0.00 MB | 0.01 MB |
| `silver/weather_hourly` | 4 | 0.18 MB | 0.00 MB | 0.01 MB | 0.37 MB |
| **total** | | 867.81 MB | 0.00 MB | 0.16 MB | 1,804.99 MB |

### _meta: monitoring, validation metrics and quarantine

| Table | Versions | Live | Change data | Log | On disk |
|---|---:|---:|---:|---:|---:|
| `data_product_metadata` | 31 | 0.01 MB | 0.00 MB | 0.17 MB | 0.40 MB |
| `ingestion_metadata` | 71 | 0.01 MB | 0.00 MB | 0.42 MB | 1.06 MB |
| `integration_metadata` | 31 | 0.01 MB | 0.00 MB | 0.15 MB | 0.34 MB |
| `quarantine` | 65 | 3.31 MB | 0.00 MB | 0.23 MB | 56.77 MB |
| `validation_metrics` | 53 | 0.01 MB | 0.00 MB | 0.21 MB | 0.44 MB |
| **total** | | 3.35 MB | 0.00 MB | 1.18 MB | 59.02 MB |

