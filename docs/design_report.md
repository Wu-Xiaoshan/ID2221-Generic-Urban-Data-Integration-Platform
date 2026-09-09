# Design Report: Urban Data Integration Platform

All figures are measured: `scripts/profile_raw.py` reproduces the source statistics,
`jobs/run_ingest.py` the ingestion results and `jobs/run_integrate.py` the integration coverage.
Fuller detail is in [Data Catalog](data_catalog.md),
[Storage Architecture](storage_architecture.md) and [Common Data Model](common_data_model.md).

## 1. Data catalog

| | `taxi_trips` (fact) | `weather` | `air_quality` | `taxi_zones` (lookup) |
|---|---|---|---|---|
| **1. Primary entity** | one completed metered trip | one hour of weather at a single implicit station | one PM2.5 reading from one instrument at one site in one hour | one TLC taxi zone |
| **2. Primary key** | none natural, so a hashed surrogate `trip_id` | `(year, month, day, hour)` | `(site_id, parameter_code, poc, obs_ts)` | `LocationID` |
| **3. Join attributes** | `pu_location_id`, `do_location_id`, `pickup_hour` | `obs_hour` | `obs_hour` (GMT) | `LocationID`, used twice |
| **4. Temporal attributes** | pickup and dropoff, New York local with DST | one timestamp, split across four integer columns | local and GMT, each split across two columns | none |
| **5. Categorical attributes** | vendor, ratecode, payment type, and both location ids | `coco` plus ten `*_source` columns | parameter, units, qualifier, method, site | every column |
| **6. Attributes that grow** | rows, about 38M per year, **and the schema itself** | rows only, 8,760 per year (one per hour) | rows, 8,760 per year per monitor, **and the number of monitors** | nothing since 2015 |
| Rows in this extract | 9,554,778 | 8,784 | 8,139,551 | 265 |

Row 6 decides the storage design: `taxi_trips` grows with events at about 38 million rows a year,
while the context tables grow with time at one row per hour, so a century of weather is 16 MB and
never needs partitioning. Four findings decided the rest.

**Three time bases, none of them stated in the data.** We worked two out from the data. Taxi pickups
bottom out at 04:00 and peak at 18:00, a local wall clock. Weather temperature bottoms out at hour 10
instead of the physical 05:00, a steady +5 offset, so that file is UTC. For the EPA file
`GMT - Local` is exactly 5.0 hours on every row with no seasonal change, so its "local" is Eastern
Standard Time all year while New York's is not. Getting this wrong puts every context join four or
five hours out and raises no error.

**The largest file is not the largest table.** `air_quality` is the EPA's national extract, 2.2 GB,
and only 51,885 rows (0.64%) are in New York City.

**POC has to be in the air-quality key.** Some sites run two instruments at once, so the key without
`POC` has 4,587 duplicates and with it none. Leaving it out would delete 4,587 real readings.

**Some weather values are model output.** The `*_source` columns show `condition_code` is 98.1%
`dwd_mosmix` and `cloud_cover_oktas` 43.8%, and `dwd_mosmix` is a forecast model. Nothing in the
values shows this, so it survives any schema check.

The fact table has no published trip identifier. The candidate key
`(VendorID, pickup, dropoff, PULocationID, DOLocationID, total_amount)` has no duplicates across the
9.55M rows, and we hash it into a surrogate `trip_id`.

## 2. Storage architecture

Two Delta layers, matching the two stages the assignment asks for. `silver/` holds four tables, one
per source, with no joins; `gold/` holds the integrated table. `_meta/` holds ingestion metadata and
quarantine, `benchmark/` the Task 6 variants. We skipped a bronze table because `data/raw/` already
lets us re-run transformations without fetching the source again, so a Delta copy would duplicate
160 MB and pad the Task 6 storage measurement with data no query reads. Only `taxi_trips` is
partitioned, by `pickup_year_month`, giving three partitions of about 125 MB against a 128 MB
target.

### Which datasets should be treated as lookup tables?

`taxi_zones`: single-column natural key, no time dimension, only descriptive columns. The two
context tables act like dimensions at this size and are broadcast too, but they are time series that
grow without bound, so that is a decision about the current size rather than about what they are.

### Which datasets should not be partitioned, and why?

All three non-fact tables. They are 265 rows and two tables of 8,784 rows. Partitioning trades
metadata cost for less scanning, and there is nothing to save on a table that fits in one file.

### Which datasets require different partitioning strategies?

Three strategies. `taxi_trips` gets monthly partitions plus `ZORDER BY (pu_location_id)`, since
partitioning on a 265-value column would create 265 directories. The context and lookup tables get
no partitioning and are broadcast. `quarantine` is partitioned by `dataset_name`, which breaks the
size rule on purpose because we read it while debugging and want each source's rejects separate.

### When does partitioning become harmful?

When partitions fall below one target file size, which is our case: `pickup_date` would give 4.1 MB
per partition over 91 days, a thirtieth of the target, paying 91 file opens to skip data that reads
in milliseconds. Also when the key has high cardinality (`(pickup_date, pu_location_id)` would give
up to 24,000 directories for three months), when queries do not filter on the key, which is true of
all three Task 6 queries, when partitions are badly skewed, and when the key comes from unvalidated
data.

### What would change at 20 times the volume?

It depends which axis grows. A month currently holds 125 MB and a day 4.1 MB.

If the extra volume is more time, the layout does not change: months stay at 125 MB and there are
simply 60 of them, while days stay at 4.1 MB, so daily never becomes right through time alone. What
changes is operational. Delta keeps superseded files until `VACUUM` runs, and we measured 1,772 MB
on disk against 887 MB live after one `OPTIMIZE`, so retention and capacity become deliberate
settings rather than defaults.

If the extra volume is more trips per day, from adding the other fleets, a day grows to 83 MB and a
month to 2.5 GB, so the key moves to `pickup_date`. The context tables change in neither case: a
century of hourly weather is 16 MB.

## 3. Common data model

**Standard timestamp format.** Every timestamp is `TimestampType` in UTC, named `*_ts`, and the Spark
session timezone is pinned to UTC. The three source encodings (one column, four integers, a date plus
a time) are declared in YAML. We convert with `America/New_York` rather than a fixed -5 offset
because March 2024 crosses the DST boundary. Join keys stay UTC and calendar columns are local:
`pickup_hour` joins against absolute instants, while `pickup_date` is what someone means by trips per
day, which is a New York day.

**Naming conventions.** Lower `snake_case`, produced mechanically, so `PULocationID` becomes
`pu_location_id`. We avoided a hand-written rename list because `Airport_fee` is the only capitalised
column of 19, so the list would be one line shorter today and stale later. The rename map handles
only semantic changes, mostly units: distance is in miles, wind in km/h, temperature in Celsius and
PM2.5 in µg/m³, and only the last states its unit in the source.

**Missing values.** The platform never imputes; there is no `fillna` in `src/`, so a null means the
value was missing at source. Null tokens are declared per dataset instead of inherited, because
`"N/A"` is a real value in `taxi_zones` and pandas' defaults invent four nulls that are not in the
file. Rows missing a key go to `_meta/quarantine` with the rule that rejected them, entirely-null
columns are dropped, and optional columns keep their nulls. The rule that matters most separates
invalid from implausible: a trip with negative duration cannot exist and is rejected, while a trip
with zero recorded distance is a real transaction with a bad odometer, so we keep it and set
`dq_zero_distance`. Rejecting the second group would remove 2.2% of the fact table.

**Common data types.** Timestamps `timestamp` (UTC), calendar dates `date` (local), surrogate keys
`bigint`, natural and foreign keys `int`, numeric codes `int` but documented as categorical, money
`double`, physical measures `double`, counts `bigint`, flags `boolean`. Every column transformation
is listed in [Common Data Model §5](common_data_model.md).

## 4. Ingestion framework

One pipeline runs every dataset through eight steps and contains no dataset name:

```
read -> validate schema -> standardize -> pre-quality transform
     -> quality rules -> post-quality transform -> write Delta -> record metadata
```

### Which components are generic and reusable?

Everything except one directory: the reader (format dispatch and read-time source filter), schema
validation and fingerprinting, name standardisation, timestamp normalisation, casting, all quality
rule types, the Delta writer with partitioning and `OPTIMIZE`, the metadata recorder and the
quarantine writer.

### Which components stay dataset-specific, and why?

Only `transforms/`, about 120 lines across four files. It holds business meaning we could not find a
way to declare: how the taxi surrogate key is built, what `dwd_mosmix` means in a provenance column,
and the fact that a site identifier is three columns stuck together. Declaring those would need
something close to a programming language.

### How are transformation rules defined and maintained?

Schema, renames, timestamps, casts, drops, quality rules and target layout all live in
`conf/datasets/<name>.yaml`. The rest is registered with a decorator at one of two stages, because
dataset-specific work sits on both sides of the quality checks. `pre_quality` builds the columns the
rules test; `post_quality` reshapes data already known to be clean. The air-quality aggregation has
to be `post_quality`, because the rules are written against per-reading columns that the aggregation
removes: a negative measurement can only be caught while the individual readings still exist.

### How is metadata managed?

One row per dataset per run in `_meta/ingestion_metadata`, same schema for every dataset: run id,
status, timings, source files and filter, records read, written and rejected, target path and
partitioning. It also stores a schema fingerprint, a hash of the source's column names and types,
and a `schema_drift` field listing missing, unexpected and type-changed columns. On the first run
the fingerprint caught that `tpep_pickup_datetime` is `timestamp_ntz` rather than `timestamp`,
confirming at type level the no-timezone property we inferred from the demand curve in §1.

Rejected rows go to `_meta/quarantine` with the rule that rejected them, serialised as JSON so one
table serves every dataset. `taxi_trips` read 9,554,778 rows, wrote 9,551,907 and rejected 2,871 in
183 seconds: 2,849 implausible durations, 21 timestamps dated 2002, 2008 and 2009, and one duplicate
from the DST gap where local 02:30 and 03:30 on 10 March both map to 07:30Z.

### How does the design reduce duplication and simplify maintenance?

A rule like "the primary key must not be null" is written once and configured four times, and adding
a rule type makes it available everywhere. Consistent naming is what allows it, since generic rules
only work because every dataset spells its keys the same way.

### What would 20 new datasets require?

Each one in a supported format with declarable transformations costs one YAML file and no Python. We
would also add a few rule types and format handlers, written once and usable everywhere. The part
that would not scale is orchestration: running 24 datasets sequentially in one process needs a
scheduler with per-dataset retry and dependency ordering.

## 5. Integration pipeline

`jobs/run_integrate.py` reads the four silver tables and writes `gold/integrated_taxi_trips`: one
row per trip, plus weather and air quality at the pickup hour and the zone and borough of both
endpoints. Config is `conf/integration.yaml`, code is `src/urbanplatform/integrate.py`.

Trips drive the join. Every context table broadcasts and every join is a left join on a unique key,
so the output has the same 9,551,907 rows as silver `taxi_trips`, with columns added but no rows. It
writes three files, one per monthly partition, in about 183 seconds. 93,507 trips, or 1.0%, have a
sentinel pickup or drop-off zone and are flagged `is_unlocated`.

### Associating an hourly observation with a trip

`taxi_trips` carries a UTC `pickup_hour` and both context tables a UTC `obs_hour`; the join is
equality on those. A trip at 18:42Z gets the 18:00Z observation. This is the nearest hour, always
rounding down, so the reading is at most 59 minutes stale. Rounding to the closer of two
observations would halve that at the cost of a second join, and neither weather nor PM2.5 moves
enough within an hour to be worth it. Weather and air quality use the same rule. The hourly grain
that makes it work is established in §3.

Both matched exactly for every trip this run. `weather_hourly` covers all 8,784 hours of 2024. The
pickup hours span 2024-01-01 05:00Z to 2024-04-01 03:00Z, and `air_quality_hourly` runs from
2024-01-01 05:00Z to 2025-01-01 04:00Z with no gaps, so it covers every one of them. All 9,551,907
trips carry a same-hour weather and air-quality reading.

### Handling missing observations

Each context table is expanded to a gap-free hourly spine before the join. The spine spans the
first observation to the last, plus `max_gap_hours`, currently 3, at each end. An empty hour,
whether an interior gap or the padded edge, takes the nearest observation within three hours, with
the closer side winning and ties rounding down. Wider gaps stay null. Spine bounds are computed in
Spark rather than on the driver, because a timestamp collected to Python is re-read in the driver's
local zone and shifts the whole spine by the UTC offset. This was a bug we hit and fixed.

Each row records `<name>_offset_hours`, which is 0 for an exact hour and otherwise the fill
distance, and `<name>_is_missing` for rows still null. Filter `offset_hours = 0` for observations
only. Both tables were gap-free for 2024, so this run wrote 0 filled and 0 missing on both.
`_meta/integration_metadata` keeps the three counts per run, so a monitor outage in a later month
shows up without recomputation.

Zones join twice, `pu_location_id` and `do_location_id` against `location_id`, broadcast. Zones 264
and 265 resolve to `Unknown` or `Outside of NYC` from the lookup, and `is_unlocated` carries through
for borough reporting that needs to drop them.

### Limitations

| Limitation | Consequence |
|---|---|
| One city-wide weather and air-quality value per hour | A Staten Island trip carries Manhattan's readings. Six PM2.5 monitors serve the city and none is in Manhattan. See §6 |
| Hour truncation | Context up to 59 minutes old |
| Pickup only, not drop-off | A long trip into changing weather keeps its pickup conditions |
| Gap fill up to 3 hours | A filled value can be a 3-hour-old reading, recorded in `offset_hours` |
| Weather is one implied station | No spatial weather resolution, a limit of the source data |

When `air_quality_is_missing` is true the row stays, with null PM2.5; dropping such rows would move
every borough count in Task 6. No Q1 2024 row is in that state, but a quarter with a monitor outage
will be.

## 6. Engineering decisions and trade-offs

| Decision | Alternative | Why, and what it costs |
|---|---|---|
| Surrogate key is `xxhash64` (bigint) | `sha2(...,256)` hex | The sha2 column alone was 589 MB of an 888 MB table. xxhash64 brought it to 377 MB, 58% smaller. Costs a collision probability of about 2.6e-6, and this is not a cryptographic use |
| Key is a hash, not a generated id | `monotonically_increasing_id()` | Re-ingestion becomes idempotent |
| No bronze layer | Full medallion | Saves a redundant 160 MB copy. Costs replaying from Delta rather than from raw files |
| Air-quality filter at read time | Land first, filter after | Avoids moving 2.2 GB to keep 12 MB. Costs the only exception to "raw files are the landing zone" |
| Money as `double` | `decimal(10,2)` | Matches the source and nothing here accumulates money precisely. A simplification we know about, since `double` is not really right for currency |
| City-wide hourly air quality | Nearest monitor per zone | Six monitors cover 265 zones and Manhattan has none, where most pickups happen. Per-zone values would look more precise than the data is |
| `taxi_zones` as SCD Type 1 | Type 2 with history | Zones stable since 2015. A later renaming would retroactively change historical reports |
| Anomalies flagged, not rejected | Reject on any failure | Keeps 2.2% of trips that are implausible but real. Costs consumers having to read the `dq_*` flags |

One bug is worth recording because it changed the data model. We wrote the temporal range rule as
`max: "2024-04-01"`, a local calendar date, but `pickup_ts` was already UTC, and local
2024-03-31T20:00 is 2024-04-01T00:00Z. The rule threw out 16,721 legitimate rush-hour trips. Our
first fix was to pad the bounds by a day, which let one trip at 00:34 on 1 April through and created
a fourth partition holding a single row, which is the failure the rule exists to prevent. Writing the
bounds as UTC instants fixed it properly, and that is why §3 keeps UTC join keys and local calendar
columns separate.
