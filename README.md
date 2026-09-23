# Urban Data Integration Platform

A Spark + Delta Lake platform that ingests NYC taxi trips, weather, air quality and taxi zone data,
standardises them into Delta tables that share one set of join keys, and integrates them into a
single analytical table with one row per trip. New releases of the data are merged into the
existing tables without rebuilding them.

## Setup

Tested on Windows 11 with Anaconda Python 3.11, JDK 17, Spark 3.5.3 and Delta Lake 3.2.1.

```bash
conda install -c conda-forge openjdk=17     # Spark 3.5 needs Java 11 or 17
pip install -r requirements.txt
```

Set `JAVA_HOME` to the JDK root. For Anaconda that is `<anaconda>\Library\lib\jvm`.

On Windows, also download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x from
[cdarlint/winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin) into
`<dir>\bin`, then set `HADOOP_HOME=<dir>` and add `%HADOOP_HOME%\bin` to `PATH`. PySpark does not
ship these and Spark will not start without them. Linux and macOS do not need this step.

Then download the datasets into `data/raw/`:

```
data/raw/taxi_trips/yellow_tripdata_2024-{01,02,03}.parquet
data/raw/weather/weather.csv
data/raw/air_quality/hourly_88101_2024.csv
data/raw/taxi_zones/taxi_zone_lookup.csv
```

Finally, check that everything works:

```bash
python scripts/check_env.py
```

It verifies Java, the Hadoop helpers, the Python packages, the raw files and a real Delta write, and
prints a fix for the first thing that fails.

On Windows, Spark prints `ERROR ShutdownHookManager: Exception while deleting Spark temp dir` when
it exits. The run has already finished at that point, so ignore it.

## Initial load

```bash
python jobs/run_ingest.py                      # the four datasets into data/lakehouse/silver/
python jobs/run_ingest.py weather taxi_zones   #   or just these
python jobs/run_integrate.py                   # the silver tables joined into gold/integrated_taxi_trips
python jobs/run_products.py                    # the four analytical data products
```

Ingestion reads `data/raw/` and takes a few minutes. Re-running it is safe: the silver tables are
overwritten and the surrogate keys are content hashes, so the same input gives the same result.
`run_integrate.py` produces one row per trip with the weather and air quality at the pickup hour and
the zone and borough of both endpoints. Its config is `conf/integration.yaml`.

Every table loads the same way:

```python
spark.read.format("delta").load("data/lakehouse/gold/integrated_taxi_trips")
```

## Processing incremental updates

An incremental release is a folder of files holding only new or re-delivered records, one file per
dataset, under `data/updates/<release>/<dataset>/`. The municipality has published only the initial
release, so release `v2` is generated from it:

```bash
python scripts/generate_updates.py            # writes data/updates/v2/ and reports/updates/v2.md
python jobs/run_update.py v2                  # merges it into the silver tables
python jobs/run_update.py v2 weather          #   or only some datasets
python scripts/verify_incremental.py v2       # checks the result, writes reports/updates/v2_verification.md
python jobs/run_refresh.py                    # brings gold and the data products up to date
python scripts/verify_refresh.py              # checks them against a rebuild from scratch
```

| Dataset | Update file | New records | Duplicates | Schema change |
|---|---|---:|---:|---|
| taxi_trips | 1-7 April 2024, parquet | 668,834 trips | 10,185 copied trips | none |
| weather | January 2025, CSV | 744 hours | 0 | `humidity` (double) added |
| air_quality | January 2025, CSV | 3,968 readings, 744 hours | 0 | `aqi` (integer) added |

`reports/updates/v2.md` describes how each file was generated and what the pipeline should do with
it. The generator is seeded, so the files are identical on every run and are not committed.
`--new-frac`, `--dup-frac` and `--dup-base` change the proportions. By default the duplicates are
1.5% of the update file; `--dup-base original` makes them 1.5% of the original trips instead.

`run_update.py` runs each dataset through the same pipeline as the initial load (schema check,
standardisation, transforms, quality rules) and then merges the result into the existing table:

- A row whose key is not in the table is inserted. The key is the table's grain: `trip_id` for
  taxi trips, `obs_hour` for weather and air quality.
- A row whose key is already there is ignored and counted as a duplicate. Stored rows are never
  rewritten, so applying the same release twice inserts nothing the second time.
- Rows that fail a quality rule go to `_meta/quarantine`, as in the initial load.
- Only the partitions that received rows are re-optimised. The v2 taxi update rewrites the new
  2024-04 partition and leaves January to March alone.

The first update also turns on Delta's Change Data Feed for the silver tables, so later steps can
read which rows an update inserted.

`run_update.py` changes the silver tables only. `run_refresh.py` then updates what is built from
them. The gold table records the version of each silver table it was built from, and the refresh
reads the silver change feeds from there:

- New and changed trips are enriched and merged into gold, and removed trips are deleted from it.
- A changed weather or air-quality hour affects the trips up to `max_gap_hours` away, because gaps
  are filled from the nearest observed hour. Those trips are recomputed.
- Each data product declares `refresh` in `conf/products.yaml`. `daily_mobility_summary` and
  `zone_monthly_statistics` compute every value within one month, so only the months whose trips
  changed are rewritten. `weather_impact_summary` and `hourly_air_quality_demand` use values taken
  over the whole period, so any change to the trips rebuilds them. A product whose trips did not
  change is skipped.

For v2 the refresh inserted 668,620 trips into gold in 113 s, against 241 s for a full
integration, and rewrote only the 2024-04 month of the two monthly products. Running it again
changes nothing.

Gold is rebuilt in full when the change feed cannot say what changed: gold records no source
versions, `conf/integration.yaml` changed, `taxi_zones` changed, a source table was overwritten,
or its change feed does not reach back far enough. The reason is recorded in
`_meta/integration_metadata`. `python jobs/run_refresh.py --full` forces the rebuild.

`run_ingest.py` rebuilds silver from `data/raw/` alone, so a full rebuild is `run_ingest.py`,
`run_integrate.py` and `run_products.py`, followed by `run_update.py` and `run_refresh.py` for each
release in order.

## Handling schema evolution

Every run compares the source's columns with `expected_columns` in `conf/datasets/<name>.yaml`. A
missing column listed under `required_columns` stops the job. Any other missing column, a new
column or a changed type is recorded in the run's `schema_drift`, and `schema_version` holds a
fingerprint of the source's column names and types.

During an update, only additive changes are applied. Delta's schema auto-merge is switched on for
the merge statement alone, so a new source column is added to the table and rows loaded before it
read it as null. A column that changes type fails the run.

A new column needs no configuration when its dataset keeps every column, as weather does: the
snake_case naming rule names it and the merge adds it. Two kinds of new column need a manual change:

- A column in a file read without schema inference arrives as a string and needs a `cast` entry.
  `aqi` has one in `conf/datasets/air_quality.yaml`.
- `air_quality` changes grain in `transforms/air_quality.py`, aggregating six monitors into one
  row per hour, so a new column there needs an aggregate written for it. `aqi` takes the worst
  monitor per hour.

The quality rules' time windows (`timestamp_window` in `conf/datasets/*.yaml`) also have to be moved
to cover each release's period, or its records are rejected as out of range. They are set for v2.

A new silver column does not reach gold or the data products on its own, so the queries built on
them keep working. `humidity` and `aqi` stay in silver for now: they only have values for January
2025, when there are no trips. To bring a column into gold, map it under `context` in
`conf/integration.yaml`. That changes the configuration's fingerprint, and the next refresh rebuilds
gold in full, so rows loaded earlier get the column too. A product whose columns change needs a new
`schema_version` in `conf/products.yaml`; the refresh refuses to overwrite it otherwise.

To see what changed in each run:

```python
(spark.read.format("delta").load("data/lakehouse/_meta/ingestion_metadata")
    .select("started_at", "dataset_name", "release", "schema_version", "schema_drift")
    .orderBy("started_at").show(truncate=False))
```

## Monitoring

Every job appends a record of each run to a Delta table under `data/lakehouse/_meta/`:

| Table | One row per | What it records |
|---|---|---|
| `ingestion_metadata` | dataset per ingestion or update run | status, duration, source files, records read, written, rejected, inserted and ignored as duplicates, schema version and drift, error |
| `integration_metadata` | integration or refresh run | status, duration, mode (full, incremental or skipped) and the reason for a full rebuild, rows inserted, updated and deleted, the silver versions read, trips in and out, how many trips got exact, filled or missing weather and air quality |
| `data_product_metadata` | product per refresh | status, duration, mode (full, incremental or skipped), the months rewritten, source table and version, schema version, rows, files, size |

```python
spark.read.format("delta").load("data/lakehouse/_meta/ingestion_metadata").show()
```

## Validation reports

Each dataset's quality rules are declared in its `conf/datasets/<name>.yaml` (`not_null`, `unique`,
`range`, `timestamp_window`, `allowed_values`, `expression`). A rule either rejects a row or flags
it. A rejected row goes to `_meta/quarantine` with the names of the rules it failed and the whole
row as JSON. A flagged row stays in the table with a `dq_<rule>` column set to true.

Rejections per dataset and rule:

```python
(spark.read.format("delta").load("data/lakehouse/_meta/quarantine")
    .groupBy("dataset_name", "failed_rules").count().show(truncate=False))
```

Four scripts check the results end to end and exit non-zero on failure:

```bash
python scripts/verify_incremental.py v2   # an applied release; writes reports/updates/v2_verification.md
python scripts/verify_refresh.py          # gold and the products against a rebuild from scratch
python scripts/verify_analytics.py        # the analytical queries' invariants
python scripts/verify_products.py         # the data products against their sources
```

## Analytical layer

Twelve Spark SQL queries and four materialised data products read `gold/integrated_taxi_trips`.
Their design is in `DesignReportWeek2.pdf`, and the optimisation experiments behind them in
`BenchmarkReportWeek2.pdf`.

```bash
python jobs/run_queries.py --list        # the queries
python jobs/run_queries.py               # run all twelve
python jobs/run_products.py              # rebuild all four products
python jobs/run_products.py --if-stale   # skip products already built from the current gold table
```

Trips per day in Q1 and Q6 divide by the days with trips in the month. For a month that is still
being loaded, like the first week of April in v2, that leaves out the days whose data has not
arrived. For a complete month it equals the calendar length, so earlier results are unchanged.

| Product | One row per | Partitioned by |
|---|---|---|
| `daily_mobility_summary` | New York local date and pickup borough | `pickup_year_month` |
| `zone_monthly_statistics` | month and pickup zone | `pickup_year_month` |
| `weather_impact_summary` | pickup zone and weather family | nothing |
| `hourly_air_quality_demand` | UTC pickup hour | `pickup_year_month` |

Bucket edges, weather families and thresholds are in `conf/analytics.yaml`, and each product's
grain, schema version and partitioning in `conf/products.yaml`.

## Adding a dataset

Write `conf/datasets/<name>.yaml` declaring the source, expected schema, renames, timestamps, casts
and quality rules. Most datasets need no Python. If one needs a derived column that cannot be
declared, add a function to `src/urbanplatform/transforms/` and register it with `@pre_quality` or
`@post_quality`. To take incremental releases, add an `incremental` section with the path pattern
of its update files and the merge key.

## Project layout

```
DesignReportWeek2.pdf         the Week 2 design report
BenchmarkReportWeek2.pdf      the Week 2 benchmark report
README.md                     this file
conf/
  platform.yaml               Spark settings and the lakehouse root
  datasets/*.yaml             one file per dataset: source, schema, renames, timestamps, quality rules, updates
  integration.yaml            how the integration joins the four tables
  analytics.yaml              bucket edges, weather families and thresholds for the analytical queries
  products.yaml               the analytical data products: grain, schema version, partitioning
src/urbanplatform/            the ingestion framework, the integration pipeline and the analytical layer
  ingest.py                   one pipeline that runs every dataset, for the initial load and for updates
  writer.py                   Delta writes: overwrite for a full load, merge for an update
  integrate.py                builds gold/integrated_taxi_trips and refreshes it from the silver change feeds
  transforms/                 the only dataset-specific code
  analytics/                  the analytical queries, the data products and the benchmark harness
jobs/                         one entry point per step (see Scripts)
scripts/                      the environment check, the update generator and the correctness checks
docs/common_data_model.md     the common data model, with every column transformation listed
reports/query_benchmark/      written by run_query_benchmark.py
reports/updates/              written by generate_updates.py and verify_incremental.py
data/raw/                     source files, not included (see Setup)
data/updates/                 incremental releases, written by generate_updates.py, not included
data/lakehouse/               created by the jobs
  silver/                     one Delta table per dataset
  gold/                       integrated_taxi_trips
  benchmark/                  the two storage variants the Week 1 benchmark compares
  products/                   the analytical data products
  _meta/                      ingestion_metadata, integration_metadata, data_product_metadata, quarantine
```

## Scripts

| Script | What it does | Needs Spark |
|---|---|---|
| `scripts/check_env.py` | verifies the whole stack end to end | yes |
| `jobs/run_ingest.py` | loads the raw files into the silver tables | yes |
| `jobs/run_integrate.py` | joins the silver tables into `gold/integrated_taxi_trips` | yes |
| `jobs/run_products.py` | rebuilds the analytical data products and records their metadata | yes |
| `jobs/run_queries.py` | runs the analytical query library | yes |
| `scripts/generate_updates.py` | generates an incremental release from the raw files, with its manifest | no |
| `jobs/run_update.py` | merges an incremental release into the silver tables | yes |
| `jobs/run_refresh.py` | brings gold and the data products up to date after an update | yes |
| `scripts/verify_refresh.py` | checks gold and the products against a rebuild from scratch | yes |
| `scripts/verify_incremental.py` | checks an applied release against the table before it and the manifest | yes |
| `scripts/verify_analytics.py` | checks the analytical queries' correctness invariants | yes |
| `scripts/verify_products.py` | checks the data products against their sources, and their metadata | yes |
| `jobs/run_benchmark.py` | Week 1: rewrites the fact table two ways and measures both | yes |
| `jobs/run_query_benchmark.py` | Week 2: runs the query optimisation experiments | yes |
| `scripts/profile_raw.py` | profiles the raw files and produces every number quoted in the data catalog | no |
