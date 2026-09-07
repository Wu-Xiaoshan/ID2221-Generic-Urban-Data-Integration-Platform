# Task 2 — Storage Architecture

Two measurements from the [Data Catalog](data_catalog.md) drive the design: `taxi_trips` is 160 MB
over 91 days (**1.8 MB per day**), and `air_quality` is 2.2 GB raw but 12 MB relevant (**the first
filter drops 99.36%**). So fine-grained partitioning costs more than it saves here, and the binding
constraint on the largest file is read efficiency rather than storage.

## 1. Layers

Task 3 asks for each dataset stored as a Delta table and Task 5 for one integrated Delta table, so
there are two Delta layers, named with the usual medallion vocabulary:

| Layer | Tables | Written by | Contents |
|---|---|---|---|
| `silver/` | 4, one per source | `ingest.py` | canonical names and types, UTC timestamps, surrogate keys, quality flags. No joins |
| `gold/` | 1 | `integrate.py` | `integrated_taxi_trips` — one row per trip with weather and air quality at the pickup hour, plus pickup and drop-off zone and borough |

`_meta/` holds `ingestion_metadata` (required by Task 3) and `quarantine` (rejected rows stored with
the rule that rejected them). `benchmark/` holds two more copies of `taxi_trips` under different
storage strategies, used only in Task 6.

**No bronze table.** Bronze exists so transformations can be re-run without re-fetching the source,
and `data/raw/` already does that — the files are on disk, immutable, and never written to. A Delta
copy would duplicate 160 MB for recoverability we have, and inflate the Task 6 storage measurement
with data no query reads. The shape is `data/raw/` (files) → `silver/` → `gold/`, with bronze served
by the filesystem. Because there is no landing table, the `air_quality` filter is applied at read
time and pushed into the scan; the predicate is recorded in `ingestion_metadata` so a missing row can
be traced to the config that excluded it.

**Why silver is materialised** instead of read straight into the join: ingestion is dataset-agnostic
(it processes whatever a YAML describes) while integration is taxi-centric (trips drive the join,
context attaches at the pickup hour), so merging them would push "pickup hour" into the generic
framework. It also means the 2.2 GB scan happens once rather than per integration run, and keeps
`quarantine` and `ingestion_metadata` per dataset, which is the only grain at which a rejected row
means anything. For `weather_hourly` and `taxi_zones` alone the layer would be hard to justify at
8,784 and 265 rows; they are materialised anyway because special-casing them would add a branch to
the framework to save under a megabyte.

## 2. Directory structure

```
lab1/
├─ conf/
│  ├─ platform.yaml                 # Spark + lakehouse root
│  └─ datasets/                     # one YAML per dataset — the extension point
│     ├─ taxi_trips.yaml  weather.yaml  air_quality.yaml  taxi_zones.yaml
├─ scripts/profile_raw.py           # pandas-only profiler behind the data catalog
├─ data/
│  ├─ raw/                          # source files, read-only to the platform
│  │  ├─ taxi_trips/yellow_tripdata_2024-{01,02,03}.parquet   160 MB
│  │  ├─ weather/weather.csv                                  1.0 MB
│  │  ├─ air_quality/hourly_88101_2024.csv                    2.2 GB
│  │  └─ taxi_zones/taxi_zone_lookup.csv                       12 KB
│  └─ lakehouse/
│     ├─ silver/{taxi_trips, weather_hourly, air_quality_hourly, taxi_zones}/
│     ├─ gold/integrated_taxi_trips/
│     ├─ benchmark/{bench_taxi_daily, bench_taxi_zordered}/
│     └─ _meta/{ingestion_metadata, quarantine}/
└─ docs/
```

The platform never writes to `data/raw/`, so `rm -rf data/lakehouse` and a re-run must reproduce the
same result — which is also why the surrogate key is a hash. The lakehouse root is one config value,
so the same code runs against a local filesystem or object storage.

## 3. Delta table organisation

| Table | Write mode | Partition by | Why |
|---|---|---|---|
| `taxi_trips` | overwrite by partition (`replaceWhere`) | `pickup_year_month` | 3 partitions of ~50 MB — see §5 |
| `weather_hourly` | overwrite | none | 8,784 rows, under 1 MB |
| `air_quality_hourly` | overwrite | none | 8,784 rows after hourly aggregation |
| `taxi_zones` | overwrite | none | 265 rows, SCD Type 1 |
| `integrated_taxi_trips` | overwrite by partition | `pickup_year_month` | same access pattern as the fact table |
| `ingestion_metadata` | append | none | one row per dataset per run |
| `quarantine` | append | `dataset_name` | keeps each source's rejects separate for debugging |

Both context tables end at 8,784 rows, one per hour of 2024. `weather` arrives that way;
`air_quality` gets there by averaging six monitors and three POCs into a city-wide hourly value.
Giving them the same grain and join key is the point of the silver layer.

Delta settings in `conf/platform.yaml`: `targetFileSize = 128MB` with `optimizeWrite` and
`autoCompact` on the fact tables, `OPTIMIZE` after ingesting a partitioned table, and `VACUUM` at
the default 7-day retention. Schema evolution is declared rather than automatic — a new upstream
column (the 2025 files add `cbd_congestion_fee`) reaches a table only once it is in the dataset
YAML, though the observed schema fingerprint is versioned in `ingestion_metadata` either way.

## 4. Naming conventions

Consistent naming is what lets the framework be generic: "the primary key must not be null" can be
one configurable rule only because every dataset spells its keys the same way. Tables are
`snake_case`, with the grain as a suffix where it differs from the source (`weather_hourly`).

| Convention | Example | Applies to |
|---|---|---|
| `snake_case`, lower case | `passenger_count` | everything |
| `*_id` | `trip_id`, `pu_location_id` | identifiers and foreign keys |
| `*_ts` / `*_date` / `*_hour` | `pickup_ts`, `pickup_date`, `obs_hour` | timestamps (always UTC), dates, hour-truncated join keys |
| `*_amount` | `fare_amount` | money, USD |
| unit suffix | `trip_distance_mi`, `trip_duration_min`, `temp_c`, `wind_speed_kmh`, `pm25_ugm3` | every physical measure |
| `is_*` / `*_is_modelled` | `is_store_and_fwd`, `cloud_cover_is_modelled` | booleans and provenance flags |
| `_*` prefix | `_ingest_run_id`, `_source_file` | platform-generated columns |

Three come from the data rather than taste. `Airport_fee` is the only capitalised column of 19, so
lower-casing must be mechanical. Unit suffixes are required because distance is in miles, wind in
km/h, temperature in Celsius and PM2.5 in µg/m³, and only the last states its unit in the source.
`*_ts` means UTC, which is a claim the normalisation step has to make true given three sources on
three time bases.

## 5. Partitioning strategy

Partition when a partition is a useful unit of I/O (roughly 128 MB to 1 GB) and queries filter on
the key.

| Table | Rows | Size | Decision |
|---|---:|---:|---|
| `taxi_trips` | 9,554,778 | ~160 MB | `pickup_year_month`, 3 partitions |
| `weather_hourly` | 8,784 | < 1 MB | none |
| `air_quality_hourly` | 8,784 | < 1 MB | none |
| `taxi_zones` | 265 | 12 KB | none, one file, broadcast |

`pickup_date` looks obvious and at production volume would be right, but here 160 MB over 91 days is
1.8 MB per partition — about a seventieth of the target — so it would pay 91 file opens and 91 log
entries to skip data that reads in milliseconds. The dirty timestamps make it worse: 21
out-of-window rows create 96 distinct dates instead of 91, so five extra partitions would hold about
four records. `pickup_year_month` gives 3 partitions of ~50 MB — still under target but the right
order of magnitude, and a month can still be re-ingested with `replaceWhere`. The range check runs
before the partition column is derived, so bad rows go to `quarantine` and never create a partition.

High-cardinality filtering uses Z-ORDER instead: the fact table is Z-ordered on `pu_location_id`
(265 values), which is filtered often but would be a bad partition key. Z-ordering groups values
within files and lets Delta skip files by statistics, giving most of the pruning without the
directory explosion.

Daily partitioning is kept as Strategy A in Task 6 so its cost is measured rather than asserted;
Strategy B is unpartitioned plus `OPTIMIZE ... ZORDER BY (pu_location_id)`. We expect A to lose on
all four metrics, since none of the three benchmark queries filters on a date and so none can prune,
while all pay the file-open overhead. If the numbers disagree, `benchmark_report.md` will say so.

## 6. Design discussion

**Which datasets are conceptually lookup tables?** `taxi_zones`: a single-column natural key, no
time dimension, only descriptive columns that exist to be joined to. It is broadcast on every join
and overwritten each run. The two context tables are a closer call — both are 8,784 rows, joined to
the fact table and never analysed alone, so in practice they behave like dimensions and are
broadcast too. But they are time series with unbounded growth, small only because this extract
covers one year at one location. Treating them as dimensions is a physical decision valid at this
size, not a claim about what they are, and naming that tells us which assumption breaks first.

**Which datasets should not be partitioned, and why?** All three non-fact tables. `taxi_zones` is
12 KB and both context tables are under a megabyte. Partitioning trades metadata cost for less
scanning, and there is nothing to save on a table that fits in one file: one 1 MB file costs one
file open, while 366 daily files of 3 KB cost 366 opens, 366 Parquet footers and a bigger
transaction log — to skip data that reads in milliseconds. Below the target file size the right
partition count is one. Note this applies to `air_quality` as a table, not as a file: the raw 2.2 GB
could sensibly be partitioned, but the filter and hourly aggregation shrink it 900× before it is
queryable. Partitioning is decided where the data is read, not where it arrived.

**Which datasets require different partitioning strategies?** Three, chosen by access pattern rather
than size. (1) `taxi_trips` and `integrated_taxi_trips`: monthly partitions for the time filter plus
Z-ORDER on `pu_location_id` for the location filter. (2) Context and lookup tables: no partitioning,
one compacted file, broadcast. (3) `quarantine`: partitioned by `dataset_name`, four values — this
breaks the size rule on purpose, because the table is read while debugging and keeping each source's
rejects separate is what makes it useful.

**When does partitioning become harmful?**

- Partitions smaller than a target file. This dataset is the example, at 1.8 MB a day against
  128 MB; overhead exceeds payload.
- High-cardinality keys. `(pickup_date, pu_location_id)` would give up to 24,000 directories for
  three months, each with a few hundred rows, and the transaction log grows larger than the data.
- Queries that don't filter on the key. A full scan of a partitioned table is slower than of an
  unpartitioned one, and all three Task 6 queries are like this.
- Skew. Partitioning by borough puts Manhattan next to Staten Island — 69 zones against 20, and much
  worse by trip volume — so parallelism drops to the slowest partition.
- Keys from unvalidated or changeable data. The 21 bad timestamps are the mild case; the serious one
  is partitioning on something that can be corrected later, which forces a rewrite.

In short, partitioning helps when a partition is large, immutable, evenly sized and named by the
query. Here "large" fails outright, and "named by the query" fails for every benchmark query.

**If the data volume increased 20×?** `taxi_trips` would be ~3.2 GB, roughly five years.

- *Partitioning:* daily still holds only 1.8 MB, so it stays wrong; monthly gives ~54 MB across 60
  partitions, finally in range. So keep `pickup_year_month` — the same key, now correct for the
  right reason. Daily becomes reasonable around 100×.
- *File layout:* `targetFileSize` to 256 MB, `optimizeWrite` and `autoCompact` mandatory, `OPTIMIZE
  ZORDER` moved to a scheduled job. With liquid clustering available both fact tables would move to
  it, since it lets the clustering key change later without rewriting the table.
- *Ingestion:* full overwrite stops working; incremental `replaceWhere` over the affected months,
  with deletion vectors so corrections don't rewrite whole files.
- *Context tables:* nothing changes. They grow with time, not trip count — twenty years of hourly
  weather is 175,000 rows, still broadcast. What breaks them is more spatial detail, not more
  volume: a per-zone weather grid is 265× larger, crosses the broadcast threshold, and forces a
  sort-merge join on a co-partitioned `(zone, hour)` key. That pressure exists already, since
  raising spatial resolution is the obvious next improvement.
- *Already handled:* the `air_quality` raw file is larger than the 20× projection for `taxi_trips`,
  and the platform copes by filtering at read time rather than storing it well.
- *Unchanged:* layers, naming, the config-driven framework and the key scheme are all
  volume-independent. At 20× only table properties in `conf/` change — testably so, since the switch
  from `pickup_date` to `pickup_year_month` in §5 was itself a one-line config edit.
