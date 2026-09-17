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
takes three to four minutes (also subject to the same run-to-run variance as ingestion) and appends
a coverage row to `_meta/integration_metadata`. Config is
`conf/integration.yaml`.

On Windows, Spark prints `ERROR ShutdownHookManager: Exception while deleting Spark temp dir` when
it exits. The run has already finished at that point, so ignore it.

## Analytical queries (Week 2)

Twelve named queries covering the six analyses the Week 2 assignment asks for. They run as Spark SQL
against `gold/integrated_taxi_trips` and, for the join experiments, the silver tables behind it.

```bash
python jobs/run_queries.py --list                        # what is available
python jobs/run_queries.py                               # run all twelve
python jobs/run_queries.py q4_weather_sensitive_zones    # or just one
python jobs/run_queries.py --explain q2_weather_distance_silver
```

The query design (grain, filters, normalisation, and the measurements behind each threshold) is in
`DesignReportWeek2.docx`. Bucket edges, the weather-code families and the support thresholds are
configuration, in `conf/analytics.yaml`; the SQL hard-codes none of them.

The timings `run_queries.py` prints are a single cold run with no warm-up and no repeats. They are
there to show the queries work, not to be quoted: the benchmark harness is what produces measurements.

Check the results are right, not just fast:

```bash
python scripts/verify_analytics.py
```

It asserts the invariants that would catch a filter silently dropping valid rows, a timezone
conversion that never happened, or the two implementations of Q2 drifting apart. Exits non-zero on
failure.

## Optimisation experiments (Week 2)

```bash
python jobs/run_query_benchmark.py                 # the seven experiments
python jobs/run_query_benchmark.py --sweep         # plus AQE on/off across every query
python jobs/run_query_benchmark.py --only caching --repeats 5
```

Each experiment is a pair of variants that differ in exactly one thing (a predicate that can prune, a
join hint, a cached intermediate, a session setting) and that must return the same answer. The
harness times both with interleaved repeats after a discarded warm-up, takes the median, verifies the
two result sets agree, and captures both physical plans.

Exit codes: 1 means the run crashed, 2 means an optimisation changed the answer.

Output goes to `reports/query_benchmark/`: `measurements.md` with the timings and plan features, and
`plans/<experiment>.<side>.txt` with the plans. Two plans are captured per variant, because
`EXPLAIN FORMATTED` reports the plan *before* execution: under AQE that is `isFinalPlan=false` and
shows none of AQE's rewrites, so the executed plan is captured afterwards as `.final.txt`.

The analysis is in `BenchmarkReportWeek2.docx`.

Reading the result:

```python
spark.read.format("delta").load("data/lakehouse/gold/integrated_taxi_trips")
```

The four silver tables under `data/lakehouse/silver/` load the same way.

## Project layout

```
DesignReportWeek1.pdf         the design report
ArchitectureDiagramWeek1.pdf  the architecture diagram
BenchmarkReportWeek1.pdf      the benchmark report
README.md                     this file
conf/
  platform.yaml               Spark settings and the lakehouse root
  datasets/*.yaml             one file per dataset: source, schema, renames, timestamps, quality rules
  integration.yaml            how the integration joins the four tables
src/urbanplatform/            the ingestion framework and the integration pipeline
  ingest.py                   one pipeline that runs every dataset
  integrate.py                builds gold/integrated_taxi_trips
  transforms/                 the only dataset-specific code
jobs/                         entry points: run_ingest.py, run_integrate.py, run_benchmark.py
scripts/                      check_env.py and profile_raw.py
docs/common_data_model.md     the common data model, with every column transformation listed
data/raw/                     source files, not included (see Setup)
data/lakehouse/               created by the jobs
  silver/                     one Delta table per dataset
  gold/                       integrated_taxi_trips
  benchmark/                  the two storage variants the benchmark compares
  _meta/                      ingestion_metadata, integration_metadata, quarantine
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

**On Windows**, also download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x from
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

Finally, check everything works:

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
| `scripts/profile_raw.py` | profiles the raw files and produces every number quoted in the data catalog | no |
