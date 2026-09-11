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
```

Ingestion reads `data/raw/`, writes Delta tables to `data/lakehouse/silver/`, and appends one row
per dataset to `_meta/ingestion_metadata`. Rejected rows go to `_meta/quarantine` with the rule that
rejected them. A full run takes about four minutes and prints:

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
takes about four minutes and appends a coverage row to `_meta/integration_metadata`. Config is
`conf/integration.yaml`.

On Windows, Spark prints `ERROR ShutdownHookManager: Exception while deleting Spark temp dir` when
it exits. The run has already finished at that point, so ignore it.

Reading the result:

```python
spark.read.format("delta").load("data/lakehouse/gold/integrated_taxi_trips")
```

The four silver tables under `data/lakehouse/silver/` load the same way.

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
| `scripts/profile_raw.py` | profiles the raw files and produces every number quoted in the data catalog | no |
