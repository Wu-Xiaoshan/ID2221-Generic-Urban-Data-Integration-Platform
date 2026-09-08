# Task 4 — Common Data Model

The four sources disagree on almost everything: three time bases, four naming styles, units
stated in one dataset and implied in the others. This document defines the single
representation they are converted into, and records every transformation applied.

The model is enforced by generic, config-driven code
([`standardize.py`](../src/urbanplatform/standardize.py)), not by per-dataset scripts. A dataset
declares *what* its columns mean in `conf/datasets/<name>.yaml`; the platform decides *how* they
are represented.

---

## 1. Standard timestamp format

**Every timestamp in the platform is `TimestampType`, in UTC, named `*_ts`.** The Spark session
timezone is pinned to UTC in `conf/platform.yaml`, so no downstream code depends on the driver's
locale.

This matters because none of the three sources declares its time base, and two of them are not
UTC:

| Source | Encoding | Time base | Conversion |
|---|---|---|---|
| `taxi_trips` | one `timestamp_ntz` column | New York local, observes DST | `to_utc_timestamp(ts, "America/New_York")` |
| `weather` | four integer columns | UTC | assemble with `make_timestamp`, no conversion |
| `air_quality` | a date column plus a time column | UTC (the GMT pair) | concatenate, parse, no conversion |

The three shapes are declared, not coded:

```yaml
timestamps:
  pickup_ts: {from: tpep_pickup_datetime, tz: America/New_York}   # taxi
  obs_ts:    {from_parts: {year: year, month: month, day: day, hour: hour}, tz: UTC}   # weather
  obs_ts:    {from_concat: [date_gmt, time_gmt], format: "yyyy-MM-dd HH:mm", tz: UTC}  # air quality
```

`tz` names the timezone the *source values* are written in. `UTC` means they are already absolute.

**Why `America/New_York` rather than a fixed −5 offset.** March 2024 crosses the DST boundary on
10 March. A fixed offset would shift every trip after that date by an hour. The named zone also
makes the spring-forward gap explicit: local 02:30 and 03:30 on 10 March both map to 07:30Z, which
is the sole cause of the one duplicate `trip_id` the platform rejects per run.

**Why the EPA GMT columns rather than its local ones.** EPA "Local" means Eastern Standard Time all
year — measured at exactly 5.0 hours from GMT on all 51,885 rows, with no seasonal variation. From
10 March, New York's actual local time is EDT. Both datasets would have had a column called "local"
that differed by an hour.

### 1.1 Derived time columns: UTC for joining, local for reporting

| Suffix | Type | Time base | Purpose |
|---|---|---|---|
| `*_ts` | timestamp | **UTC** | the canonical instant |
| `*_hour` | timestamp | **UTC** | hour-truncated join key between facts and context |
| `*_date` | date | **New York local** | calendar reporting and partitioning |
| `*_year_month` | string | **New York local** | partition key |

These are different jobs, and deriving both from UTC gets one wrong. `pickup_hour` joins against
weather and air quality, which are absolute instants, so it must be UTC. `pickup_date` is what an
analyst means by "trips per day" — a New York day. March in New York ends at 04:00Z on 1 April, so
a UTC-derived month put the last four hours of local March, about 16,700 trips, into an April
partition.

---

## 2. Naming conventions

Uniform naming is what allows the ingestion framework to be generic: "the primary key must not be
null" is one configurable rule only because every dataset spells its keys the same way.

**Columns are lower `snake_case`, produced mechanically** by
[`to_snake_case`](../src/urbanplatform/standardize.py), not by a per-column list:
`PULocationID` → `pu_location_id`, `Date GMT` → `date_gmt`, `Airport_fee` → `airport_fee`.

A rename map in the dataset YAML is used only where a name needs a *semantic* change — adding a
unit (`wspd` → `wind_speed_kmh`) or a meaning (`Sample Measurement` → `pm25_ugm3_raw`). Everything
else is handled by the mechanical rule, so a new upstream column arrives correctly named with no
config change.

| Convention | Example | Applies to |
|---|---|---|
| `*_id` | `trip_id`, `pu_location_id`, `site_id` | identifiers and foreign keys |
| `*_ts` | `pickup_ts`, `obs_ts` | timestamps, always UTC |
| `*_date` / `*_hour` | `pickup_date`, `obs_hour` | calendar date (local), hour join key (UTC) |
| `*_amount` | `fare_amount`, `total_amount` | money, USD |
| unit suffix | `trip_distance_mi`, `trip_duration_min`, `temp_c`, `wind_speed_kmh`, `pm25_ugm3` | every physical measure |
| `is_*` | `is_store_and_fwd`, `is_unlocated`, `is_real_zone` | booleans |
| `*_is_modelled` | `cloud_cover_oktas_is_modelled` | provenance flags |
| `dq_*` | `dq_zero_distance` | data-quality flags on kept rows |
| `_*` | `_ingest_run_id` | platform-generated technical columns |

Tables are `snake_case`, with the grain as a suffix where it differs from the source
(`weather_hourly`), so the name states what one row is.

**Two of these are forced by the data.** `Airport_fee` is the only capitalised column of 19 in the
taxi feed, which is why lower-casing must be mechanical rather than a rename list that someone has
to maintain. Unit suffixes are mandatory because distance is in miles, wind in km/h, temperature in
Celsius and PM2.5 in µg/m³ — and only the last states its unit anywhere in the source data.

---

## 3. Rules for handling missing values

**The platform never imputes.** There is no `fillna` anywhere in `src/`. A null in a silver table
means the value was missing at the source, and nothing else. Gap-filling, where it is needed at
all, belongs to the integration step (Task 5) where the strategy can be stated and its effect
measured — not hidden inside ingestion.

The rules, in the order they apply:

| # | Rule | Applied by | Example |
|---|---|---|---|
| 1 | **Null tokens are declared per dataset, never inherited from a library default** | `source.options.nullValue` | `taxi_zones` sets `nullValue: ""`, because `"N/A"` is a real value there — zone 264 is `Unknown`/`N/A` and 265 is `N/A`/`Outside of NYC`. pandas' defaults convert it and invent four nulls that are not in the file |
| 2 | **A row missing a key or a mandatory field is rejected**, not repaired | `not_null` rules | `taxi_trips` requires `trip_id`, both timestamps and both location ids |
| 3 | **Rejected rows go to `_meta/quarantine` with the rule that rejected them** | `quality.to_quarantine` | a rejection can be explained later, not just counted |
| 4 | **A column that is entirely null is dropped, not carried** | `drop` in YAML | `weather.snwd` and `weather.wpgt` are null on all 8,784 rows |
| 5 | **Optional columns keep their nulls** | (no rule) | five taxi columns are null on the same 751,962 rows (7.87%). The cause is unknown and not vendor-specific, so they are documented as optional rather than filtered on |
| 6 | **Anomalies that are suspicious but not invalid are flagged, not removed** | `on_fail: flag` | `dq_zero_distance` marks 213k trips reporting zero distance; `dq_negative_total` marks refunds and disputes. Discarding them would silently change the trip count |
| 7 | **Values that are absent rather than wrong pass every rule except `not_null`** | `quality._passes` | otherwise one missing value would trip several rules at once and the quarantine reason would not say which problem mattered |
| 8 | **Missing provenance is treated as "observed"** | `weather` transform | `coalesce(source == 'dwd_mosmix', false)` — a null source column does not silently become a model flag |

Rule 6 is the one that carries the most weight. It draws the line between *invalid* (a trip with
negative duration cannot exist — rejected) and *implausible* (a trip with zero recorded distance
is a real transaction with a bad odometer reading — kept and flagged). Rejecting the second class
would remove 2.2% of the fact table and change every count in the benchmark.

---

## 4. Common data types

| Concept | Type | Rationale |
|---|---|---|
| Timestamp | `timestamp` | UTC, see §1 |
| Calendar date | `date` | New York local |
| Surrogate key | `bigint` | a 64-bit content hash; see the note below |
| Natural / foreign key | `int` | `location_id` 1–265, `pu_location_id`, `do_location_id` |
| Code or category stored as a number | `int` | `vendor_id`, `payment_type`, `ratecode_id`, `condition_code`. Integer-typed for join efficiency, but **categorical**: summing or averaging them is meaningless |
| Category stored as text | `string` | `borough`, `zone`, `service_zone` |
| Money | `double` | USD. Fixed-point `decimal(10,2)` would be more correct for currency; `double` is kept because the source is `double` and no accumulation in this platform is precision-sensitive. Recorded as a known simplification |
| Physical measure | `double` | `trip_distance_mi`, `temp_c`, `pm25_ugm3`, `trip_duration_min` |
| Bounded integer measure | `int` | `passenger_count`, `humidity_pct`, `cloud_cover_oktas`, `wind_direction_deg` |
| Count / cardinality | `bigint` | `site_count`, `reading_count` — Spark aggregate output |
| Flag | `boolean` | `is_*`, `dq_*`, `*_is_modelled` |

**Why the surrogate key is a `bigint` and not a hash string.** `sha2(..., 256)` returns 64
high-entropy hex characters. Measured on this dataset, that one column cost 589 MB of an 888 MB
table — two thirds of it — and is paid again by every scan. `xxhash64` is 8 bytes and brought the
table to 377 MB, a 58% reduction. The trade is a collision probability of about n²/2⁶⁵, which at
9.55M rows is 2.6 × 10⁻⁶. This is not a cryptographic use: nothing depends on the key being hard
to forge, only on it being stable and unique enough.

**Why the key is a hash at all, rather than a generated id.** `monotonically_increasing_id()` would
give a different value on every run, so re-ingesting a file would duplicate its rows. A content
hash makes re-ingestion idempotent: the same input row always produces the same key.

---

## 5. Transformation catalogue

Every transformation applied to every column. Type changes not listed are identity.

### 5.1 Applied to all datasets

| Step | Transformation | Implemented in |
|---|---|---|
| 1 | Read with the declared format, options and null token | `reader.read_source` |
| 2 | Apply the source filter during the scan | `reader.read_source` |
| 3 | Record a schema fingerprint and report drift | `reader.validate_schema` |
| 4 | Semantic renames from the YAML `rename` map | `standardize.standardize_names` |
| 5 | Mechanical `snake_case` on every remaining column | `standardize.standardize_names` |
| 6 | Build declared timestamps, converting to UTC | `standardize.normalize_timestamps` |
| 7 | Cast declared columns | `standardize.apply_casts` |
| 8 | Drop declared columns | `standardize.drop_columns` |

### 5.2 `taxi_trips` — 19 source columns → 28

| Source | Target | Transformation | Why |
|---|---|---|---|
| `VendorID` | `vendor_id` | snake_case | |
| `tpep_pickup_datetime` | `pickup_ts` | `to_utc_timestamp(·, America/New_York)`; source dropped | local wall clock with DST |
| `tpep_dropoff_datetime` | `dropoff_ts` | same | |
| `trip_distance` | `trip_distance_mi` | rename | unit is miles and unstated |
| `PULocationID` / `DOLocationID` | `pu_location_id` / `do_location_id` | snake_case | |
| `Airport_fee` | `airport_fee` | snake_case | the one capitalised column |
| `passenger_count`, `RatecodeID`, `payment_type` | same, `int` | cast from `bigint` | bounded values |
| `store_and_fwd_flag` | `is_store_and_fwd` | `== 'Y'`; source dropped | Y/N string to boolean |
| — | `trip_id` | `xxhash64(vendor_id, pickup_ts, dropoff_ts, pu_location_id, do_location_id, total_amount)` | no natural key exists; §4 |
| — | `trip_duration_min` | `(dropoff_ts − pickup_ts) / 60` | needed by the duration rule |
| — | `pickup_date`, `pickup_year_month` | derived from `pickup_ts` **in New York local time** | calendar reporting; §1.1 |
| — | `pickup_hour` | `date_trunc('hour', pickup_ts)` in **UTC** | join key to context |
| — | `is_unlocated` | `pu` or `do` location in (264, 265) | sentinels are not boroughs |
| — | `dq_zero_distance`, `dq_negative_total`, `dq_unknown_vendor` | quality flags | §3 rule 6 |

### 5.3 `weather` — 24 source columns → 18

| Source | Target | Transformation | Why |
|---|---|---|---|
| `year`, `month`, `day`, `hour` | `obs_ts` | `make_timestamp(...)`; sources dropped | the timestamp is not a column |
| `temp` | `temp_c` | rename | Celsius |
| `rhum` | `humidity_pct` | rename | percent |
| `prcp` | `precipitation_mm` | rename | millimetres |
| `wdir` | `wind_direction_deg` | rename | degrees |
| `wspd` | `wind_speed_kmh` | rename | km/h, not m/s |
| `pres` | `pressure_hpa` | rename | hPa |
| `cldc` | `cloud_cover_oktas` | rename, cast `int` | oktas |
| `coco` | `condition_code` | rename, cast `int` | categorical code |
| `snwd`, `wpgt` (+ their sources) | — | dropped | 100% null on all 8,784 rows |
| `*_source` (8 remaining) | `*_is_modelled` | `source == 'dwd_mosmix'`; sources dropped | `condition_code` is 98.1% and `cloud_cover_oktas` 43.8% forecast-model output, invisible in the values |
| — | `obs_hour`, `obs_date` | truncation from `obs_ts` | join key and calendar |

### 5.4 `air_quality` — 24 source columns → 7

| Source | Target | Transformation | Why |
|---|---|---|---|
| all rows | — | filter `State Code = '36' AND County Code IN (5 NYC boroughs)` **during the scan** | 99.36% of the 2.2 GB file is out of scope |
| `State Code`+`County Code`+`Site Num` | `site_id` | concatenation, kept as **strings** | zero-padded; parsing as integers merges distinct sites |
| `Date GMT` + `Time GMT` | `obs_ts` | concatenate, parse `yyyy-MM-dd HH:mm`; sources dropped | GMT, not the EST-all-year "local" pair |
| `Sample Measurement` | `pm25_ugm3_raw` | rename, cast `double` | unit stated only in a sibling column |
| `POC` | `poc` | cast `int` | part of the key |
| `Date Local`, `Time Local`, `Date of Last Change`, `Uncertainty`, `Datum` | — | dropped | superseded, or metadata about the record rather than the measurement |
| — | `obs_hour` | `date_trunc('hour', obs_ts)` | join key |
| *(after quality checks)* | `pm25_ugm3`, `pm25_min_ugm3`, `pm25_max_ugm3`, `site_count`, `reading_count` | `groupBy(obs_hour)` aggregation | six monitors → one city-wide hourly value |

The aggregation runs **after** the quality checks, and the order is forced: the rules are written
against per-reading columns (`pm25_ugm3_raw`, `site_id`, `poc`) that the aggregation removes. A null
or negative measurement can only be checked while the individual readings still exist.

The mean is taken over readings rather than over sites, which is safe here because every NYC site
runs one instrument: across all 8,784 hours `reading_count` equals `site_count`. Sites with two
instruments exist in the wider file, but they fall outside the NYC filter. Both counts are kept in
the table so the assumption stays visible, and an extract that broke it would show the two columns
diverging.

### 5.5 `taxi_zones` — 4 source columns → 5

| Source | Target | Transformation | Why |
|---|---|---|---|
| `LocationID` | `location_id` | snake_case, cast `int` | |
| `Borough`, `Zone`, `service_zone` | `borough`, `zone`, `service_zone` | snake_case | |
| — | `is_real_zone` | `location_id NOT IN (264, 265)` | sentinels join successfully and would otherwise become boroughs |

The reader sets `nullValue: ""` for this dataset so the literal `"N/A"` survives; see §3 rule 1.

---

## 6. Where the model is enforced

| Concern | Enforced by | Configured in |
|---|---|---|
| Naming | `standardize.standardize_names` | `rename` (semantic changes only) |
| Timestamps | `standardize.normalize_timestamps` | `timestamps` |
| Types | `standardize.apply_casts` | `cast` |
| Missing values | `quality.apply_rules`, `source.options` | `quality.rules`, `nullValue` |
| Derived columns | `transforms/<dataset>.py` | registered via `registry` |

Only the last row is dataset-specific code — about 120 lines across four files. Everything else in
this document is one implementation applied uniformly, which is why adding a fifth dataset is
normally a YAML file and no Python at all.
