# Architecture Diagram

```mermaid
flowchart TD
    subgraph raw["data/raw/ · immutable landing zone"]
        R1["taxi_trips/*.parquet"]
        R2["weather/weather.csv"]
        R3["air_quality/hourly_88101_2024.csv"]
        R4["taxi_zones/taxi_zone_lookup.csv"]
    end

    subgraph fw["Ingestion framework · src/urbanplatform/, config-driven"]
        direction TB
        S1["read_source\nformat, options, push-down filter"]
        S2["validate_schema\nfingerprint + drift report"]
        S3["standardize\nsnake_case, UTC timestamps, casts, drops"]
        S4["pre_quality transforms\nsurrogate keys, derived columns"]
        S5["quality rules\nreject or flag"]
        S6["post_quality transforms\nair_quality to city-wide hourly"]
        S1 --> S2 --> S3 --> S4 --> S5 --> S6
    end

    R1 & R2 & R3 & R4 --> S1

    subgraph silver["data/lakehouse/silver/ · Delta"]
        T1["taxi_trips\npart pickup_year_month, ZORDER pu_location_id"]
        T2["weather_hourly"]
        T3["air_quality_hourly"]
        T4["taxi_zones lookup"]
    end

    S6 --> T1 & T2 & T3 & T4
    S5 -. rejected rows .-> Q["_meta/quarantine"]
    fw -. one row per dataset per run .-> M["_meta/ingestion_metadata"]

    subgraph integ["Integration pipeline · integrate.py, conf/integration.yaml"]
        direction TB
        I1["densify context to gap-free hourly spine\nfill gaps up to 3h from nearest observation"]
        I2["equi-join on pickup_hour, UTC\nweather + air quality"]
        I3["left-join taxi_zones twice\npickup and dropoff to zone, borough"]
        I1 --> I2 --> I3
    end

    T1 --> I2
    T2 & T3 --> I1
    T4 --> I3

    subgraph gold["data/lakehouse/gold/ · Delta"]
        G["integrated_taxi_trips\none row per trip + pickup-hour context + zones\npart pickup_year_month, ZORDER pu_location_id"]
    end

    I3 --> G
    integ -. coverage: exact / filled / missing .-> MI["_meta/integration_metadata"]

    subgraph bench["Benchmark · jobs/run_benchmark.py"]
        B1["bench_taxi_daily\nstrategy A: partitionBy(pickup_date)"]
        B2["bench_taxi_zordered\nstrategy B: unpartitioned + ZORDER pu_location_id"]
    end
    T1 -. rewritten two ways .-> B1 & B2
```

## Notes

- No bronze table. `data/raw/` is the immutable landing zone and transforms replay from the files.
  See [Storage Architecture §1](storage_architecture.md).
- The ingestion framework carries no dataset name. Everything is driven by
  `conf/datasets/<name>.yaml` except the `pre_quality` and `post_quality` transforms, reached
  through a registry.
- Silver tables share their join keys, the UTC `pickup_hour` and `obs_hour` and the `location_id`,
  so integration is one broadcast join per context table. Gap-filling runs on the context tables
  before the join.
- The `air_quality` filter is applied during the raw scan, not after landing.

Sizes, row counts and timings are in the [Design Report](design_report.md).
