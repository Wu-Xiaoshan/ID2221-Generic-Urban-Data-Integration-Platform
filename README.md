# Urban Data Integration Platform

A Spark + Delta Lake platform that ingests NYC taxi trips, weather, air quality and taxi zone data,
standardises them, and integrates them into a single analytical table.

Documentation: [Design Report](docs/design_report.md) ·
[Data Catalog](docs/data_catalog.md) (Task 1) ·
[Storage Architecture](docs/storage_architecture.md) (Task 2) ·
[Common Data Model](docs/common_data_model.md) (Task 4)

## Setup

Verified on Windows 11 with Anaconda Python 3.11. Spark 3.5.3, Delta Lake 3.2.1, JDK 17.

**1. Java 17.** Spark 3.5 needs Java 11 or 17. Installing through conda keeps it self-contained and
needs no admin rights:

```
conda install -c conda-forge openjdk=17
```

Then set `JAVA_HOME` to the JDK root — for Anaconda that is `<anaconda>\Library\lib\jvm`.

**2. Python packages.**

```
pip install -r requirements.txt
```

**3. Windows only: Hadoop native helpers.** Hadoop's `Shell` class needs `winutils.exe` and
`hadoop.dll` on Windows, and PySpark does not ship them. Without these, Spark fails at startup with
`HADOOP_HOME and hadoop.home.dir are unset`.

Download both files for Hadoop 3.3.x — Spark 3.5.3 bundles Hadoop 3.3.4 — from
[cdarlint/winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin), put them in
`<dir>\bin`, and set `HADOOP_HOME=<dir>` and add `%HADOOP_HOME%\bin` to `PATH`.

On Linux and macOS this step is not needed.

**4. Data.** Download the four datasets into `data/raw/`:

```
data/raw/taxi_trips/yellow_tripdata_2024-{01,02,03}.parquet
data/raw/weather/weather.csv
data/raw/air_quality/hourly_88101_2024.csv
data/raw/taxi_zones/taxi_zone_lookup.csv
```

**5. Verify.**

```
python scripts/check_env.py
```

It checks Java, the Hadoop helpers, the Python packages, the raw files, and a real Delta write and
read. It prints a fix for the first thing that fails.

## Notes

- The first Spark start downloads the Delta jars from Maven into `~/.ivy2`. This is cached, so only
  the first run is slow. Sessions set a Maven mirror to speed this up on slow connections.
- On shutdown, Spark on Windows logs `ERROR ShutdownHookManager: Exception while deleting Spark temp
  dir`. This is a known Windows file-locking issue, happens after all work has completed, and can be
  ignored.

## Scripts

| Script | What it does | Needs Spark |
|---|---|---|
| `scripts/check_env.py` | verifies the whole stack end to end | yes |
| `scripts/profile_raw.py` | profiles the raw files; produces every number quoted in the data catalog | no |
