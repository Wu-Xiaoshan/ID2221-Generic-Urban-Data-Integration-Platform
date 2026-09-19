# Urban Data Integration Platform

A Spark + Delta Lake platform that ingests NYC taxi trips, weather, air quality and taxi zone data,
standardises them into Delta tables that share one set of join keys, and integrates them into a
single analytical table with one row per trip.

## Running the platform

```bash
python jobs/run_ingest.py                      # 1. ingest all four datasets
python jobs/run_ingest.py weather taxi_zones   #    or just these
python jobs/run_integrate.py                   # 2. join them into gold/integrated_taxi_trips
python jobs/run_benchmark.py                   # 3. compare the two storage strategies
python jobs/run_queries.py                     # 4. run the analytical queries (Week 2)
python jobs/run_query_benchmark.py             # 5. run the optimisation experiments (Week 2)
python jobs/run_products.py                    # 6. build the analytical data products (Week 2)
```

Ingestion reads `data/raw/`, writes Delta tables to `data/lakehouse/silver/`, and appends one row
per dataset to `_meta/ingestion_metadata`. Rejected rows go to `_meta/quarantine` with the rule that
rejected them. A full run takes a few minutes and prints:

```
dataset              status           read     written  rejected     sec
taxi_trips           success     9,554,778   9,551,907     2,871  183.47
weather              success         8,784       8,784         0    6.75
air_quality          success        51,885       8,784         0   36.97
taxi_zones           success           265         265         0    5.62
```

Re-running is safe. The silver tables are overwritten and the surrogate keys are content hashes, so
the same input always produces the same result.

`run_integrate.py` joins the four silver tables into `gold/integrated_taxi_trips`, one row per trip
with the weather and air quality at the pickup hour and the zone and borough of both endpoints. It
takes three to four minutes, with some variation between runs, and appends a coverage row to
`_meta/integration_metadata`. Its config is `conf/integration.yaml`.

To read the integrated table:

```python
spark.read.format("delta").load("data/lakehouse/gold/integrated_taxi_trips")
```

The four silver tables under `data/lakehouse/silver/` load the same way.

On Windows, Spark prints `ERROR ShutdownHookManager: Exception while deleting Spark temp dir` when
it exits. The run has already finished at that point, so ignore it.

## Analytical queries (Week 2)

Twelve named queries cover the six analyses the Week 2 assignment asks for. They run as Spark SQL
against `gold/integrated_taxi_trips` and, for the join experiments, against the silver tables behind
it.

```bash
python jobs/run_queries.py --list                        # what is available
python jobs/run_queries.py                               # run all twelve
python jobs/run_queries.py q4_weather_sensitive_zones    # or just one
python jobs/run_queries.py --explain q2_weather_distance_silver
```

The query design (grain, filters, normalisation, and the measurements behind each threshold) is in
`DesignReportWeek2.pdf`. Bucket edges, the weather-code families and the support thresholds are
configuration, in `conf/analytics.yaml`; the SQL hard-codes none of them.

The timings `run_queries.py` prints come from a single cold run with no warm-up and no repeats. They
show that the queries work; for measurements, use the benchmark harness below.

To check that the results are correct:

```bash
python scripts/verify_analytics.py
```

Its invariants catch a filter that silently drops valid rows or a timezone conversion that never
happened, and they confirm that the two implementations of Q2 still agree. It exits non-zero on
failure.

## Optimisation experiments (Week 2)

```bash
python jobs/run_query_benchmark.py                 # the seven experiments
python jobs/run_query_benchmark.py --sweep         # plus AQE on/off across every query
python jobs/run_query_benchmark.py --products      # plus each product against recomputing it
python jobs/run_query_benchmark.py --only caching --repeats 5
```

`--products` needs `jobs/run_products.py` to have run. The numbers in the Week 2 benchmark report
come from one run of `--sweep --products`, so that is the command to reproduce them.

Each experiment is a pair of variants that differ in exactly one thing (a predicate that can prune, a
join hint, a cached intermediate, a session setting) and that must return the same answer. The
harness times both with interleaved repeats after a discarded warm-up, takes the median, verifies the
two result sets agree, and captures both physical plans.

Exit codes: 1 means the run crashed, 2 means an optimisation changed the answer.

Output goes to `reports/query_benchmark/`: `measurements.md` with the timings and plan features, and
`plans/<experiment>.<side>.txt` with the plans. Two plans are captured per variant, because
`EXPLAIN FORMATTED` reports the plan *before* execution: under AQE that is `isFinalPlan=false` and
shows none of AQE's rewrites, so the executed plan is captured afterwards as `.final.txt`.

The analysis is in `BenchmarkReportWeek2.pdf`.

## Analytical data products (Week 2)

Four Delta tables materialise common reports from `gold/integrated_taxi_trips`, so analysts can read
the results without running the queries.

```bash
python jobs/run_products.py --list                     # what is available
python jobs/run_products.py                            # build or refresh all four
python jobs/run_products.py weather_impact_summary     # or just one
python jobs/run_products.py --if-stale                 # skip products already built from the current gold table
python scripts/verify_products.py                      # check them
```

| Product | One row per | Partitioned by |
|---|---|---|
| `daily_mobility_summary` | New York local date and pickup borough | `pickup_year_month` |
| `zone_monthly_statistics` | month and pickup zone | `pickup_year_month` |
| `weather_impact_summary` | pickup zone and weather family | nothing |
| `hourly_air_quality_demand` | UTC pickup hour | `pickup_year_month` |

Products are written to `data/lakehouse/products/<name>`. Each one is a single Spark SQL statement in
`src/urbanplatform/analytics/products.py` over the same views as the analytical queries, and
`zone_monthly_statistics` embeds Q1 unchanged. Each product's grain, schema version and partitioning
are set in `conf/products.yaml`.

Metadata is recorded in two places. Every product table carries `product.*` Delta table properties:
its source table and the Delta version of that table it was built from, creation and refresh time
(UTC), schema version and a fingerprint of its columns. `_meta/data_product_metadata` gets one row
per product per run with the same fields, plus row count, file count, size and duration. If a
product's columns change and its `schema_version` does not, the run fails.

```python
spark.sql("DESCRIBE DETAIL delta.`data/lakehouse/products/daily_mobility_summary`").select("properties").show(truncate=False)
spark.read.format("delta").load("data/lakehouse/_meta/data_product_metadata").show()
```

`verify_products.py` checks each product's key, metadata and freshness, and compares it with its
source. The daily trips must add up to the gold table, and `zone_monthly_statistics` must equal Q1.
Q4's ranking and Q3's correlations must come out the same when recomputed from
`weather_impact_summary` and `hourly_air_quality_demand`. It exits non-zero on failure.

## Project layout

```
DesignReportWeek2.pdf         the design report
BenchmarkReportWeek2.pdf      the benchmark report
README.md                     this file
conf/
  platform.yaml               Spark settings and the lakehouse root
  datasets/*.yaml             one file per dataset: source, schema, renames, timestamps, quality rules
  integration.yaml            how the integration joins the four tables
  analytics.yaml              bucket edges, weather families and thresholds for the analytical queries
  products.yaml               the analytical data products: grain, schema version, partitioning
src/urbanplatform/            the ingestion framework, the integration pipeline and the analytical layer
  ingest.py                   one pipeline that runs every dataset
  integrate.py                builds gold/integrated_taxi_trips
  transforms/                 the only dataset-specific code
  analytics/                  the Week 2 analytical layer
    config.py                 loads conf/analytics.yaml and turns it into SQL CASE expressions
    views.py                  the views that queries and products read: trips, hourly_demand
    queries.py                the analytical query library
    benchmark.py              the benchmark harness: timing, result comparison, plan capture
    experiments.py            the optimisation experiments
    products.py               what each data product computes
    materialize.py            writes the data products and their metadata
jobs/                         one entry point per step (see Scripts)
scripts/                      the environment check, raw-file profiling and correctness checks (see Scripts)
docs/common_data_model.md     the common data model, with every column transformation listed
reports/query_benchmark/      written by run_query_benchmark.py: measurements.md and plans/
data/raw/                     source files, not included (see Setup)
data/lakehouse/               created by the jobs
  silver/                     one Delta table per dataset
  gold/                       integrated_taxi_trips
  benchmark/                  the two storage variants the benchmark compares
  products/                   the analytical data products
  _meta/                      ingestion_metadata, integration_metadata, data_product_metadata, quarantine
```

## Adding a dataset

Write `conf/datasets/<name>.yaml` declaring the source, expected schema, renames, timestamps, casts
and quality rules. Most datasets need no Python. If one needs a derived column that cannot be
declared, add a function to `src/urbanplatform/transforms/` and register it with `@pre_quality` or
`@post_quality`.

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

## Scripts

| Script | What it does | Needs Spark |
|---|---|---|
| `jobs/run_ingest.py` | runs the ingestion framework | yes |
| `jobs/run_integrate.py` | joins the silver tables into `gold/integrated_taxi_trips` | yes |
| `scripts/check_env.py` | verifies the whole stack end to end | yes |
| `jobs/run_benchmark.py` | rewrites the fact table two ways and measures both | yes |
| `jobs/run_queries.py` | runs the Week 2 analytical query library | yes |
| `jobs/run_query_benchmark.py` | runs the optimisation experiments, writes the measurements | yes |
| `scripts/verify_analytics.py` | checks the analytical layer's correctness invariants | yes |
| `jobs/run_products.py` | builds or refreshes the analytical data products and records their metadata | yes |
| `scripts/verify_products.py` | checks the data products against their sources, and their metadata | yes |
| `scripts/profile_raw.py` | profiles the raw files and produces every number quoted in the data catalog | no |
