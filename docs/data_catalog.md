# Task 1 — Data Catalog

All figures measured from `data/raw/` by [`scripts/profile_raw.py`](../scripts/profile_raw.py).

| Dataset | File | Size | Rows | One row = | Role |
|---|---|---:|---:|---|---|
| `taxi_trips` | `yellow_tripdata_2024-{01,02,03}.parquet` | 160 MB | 9,554,778 | one taxi trip | Fact |
| `weather` | `weather.csv` | 1.0 MB | 8,784 | one hour at one station | Context |
| `air_quality` | `hourly_88101_2024.csv` | 2.2 GB | 8,139,551 | one PM2.5 reading, one instrument, one hour | Context |
| `taxi_zones` | `taxi_zone_lookup.csv` | 12 KB | 265 | one taxi zone | Lookup |

Trips cover 91 days (2024-01-01 to 03-31); both context datasets cover all of 2024, so about 75% of
their rows have no trip to join to. `air_quality` is the largest file but is the EPA's national
extract — only 51,885 rows (0.64%) are in New York City.

## A. `taxi_trips`

| Question | Answer |
|---|---|
| Primary entity | one completed metered trip |
| Primary key | none natural. Candidate key `(VendorID, pickup, dropoff, PULocationID, DOLocationID, total_amount)` has 0 duplicates in 9.55M rows; hashed into a surrogate `trip_id` |
| Join attributes | `PULocationID`, `DOLocationID` → `taxi_zones`; `pickup_hour` → both context datasets |
| Temporal | `tpep_pickup_datetime`, `tpep_dropoff_datetime`, local New York time (observes DST) |
| Categorical | `VendorID` {1,2,6}, `RatecodeID` {1–6,99}, `payment_type` {0–4}, `store_and_fwd_flag`, and both location IDs — these are codes, not measures |
| Grows over time | rows, and the schema: fee columns were added in 2015, 2019, 2022 and 2025 |

- The timezone is not stated. Pickups bottom out at 04:00 and peak at 18:00, which is a local clock.
- `Airport_fee` is the only capitalised column of 19, so lower-casing must be a mechanical rule.
- Timestamps run from 2002-12-31 to 2024-04-01. Only 21 rows are out of window, but they give 96
  distinct pickup dates instead of 91 — five near-empty partitions if we partition by day. Range
  checks must run before the partition column is derived.
- The key is hashed rather than generated so re-ingesting a file is a no-op, not a duplication.
- 751,962 rows (7.87%) are null across the same five columns at once, and it is not one vendor's
  feed (vendor 1 is 9.0%, vendor 2 is 7.5%). Cause unknown; those columns are treated as optional.
- Dirt for the quality rules: 2,801 trips of non-positive duration, 48 over 24 h, 215,764 with zero
  distance, 105,931 with zero passengers, 36,760 pickups / 84,879 drop-offs in sentinel zones
  264–265.

## B. `weather`

A Meteostat export, not the NOAA format the assignment's source link suggests, and already reduced
to one row per hour.

| Question | Answer |
|---|---|
| Primary entity | one hour of weather at a single station (there is no station column) |
| Primary key | `(year, month, day, hour)`, 0 duplicates. 8,784 rows = 366 × 24, no gaps |
| Join attributes | `obs_hour` only. No spatial key, so every trip in an hour gets the same weather |
| Temporal | the timestamp is split across four integer columns and must be assembled |
| Categorical | `coco` (condition code) and ten `*_source` columns |
| Grows over time | ~8,760 rows a year, negligible here |

Measures: `temp` °C, `rhum` %, `prcp` mm, `wdir` °, `wspd` km/h, `pres` hPa, `cldc` oktas, `coco`
code. `snwd` and `wpgt` are 100% null and dropped.

- Timezone is UTC: mean temperature is lowest at hour 10 and highest at hour 20, but the real
  minimum is ~05:00 local and maximum ~15:00 — a consistent +5 offset.
- Not every value is an observation. `temp`, `rhum`, `wspd` and `pres` are ~100% measured, but
  `coco` is 98.1% and `cldc` is 43.8% `dwd_mosmix`, i.e. forecast-model output. Nothing in the
  values shows this, so each measure carries a `*_is_modelled` flag downstream.

## C. `air_quality`

Standard EPA AQS hourly format; column names match the published schema.

| Question | Answer |
|---|---|
| Primary entity | one PM2.5 reading from one instrument at one site in one hour |
| Primary key | `(site_id, parameter_code, poc, obs_ts_gmt)`, `site_id` = state ‖ county ‖ site |
| Join attributes | `obs_hour` (GMT) only. No spatial key to `taxi_zones` |
| Temporal | `Date/Time Local` and `Date/Time GMT`, each split across two columns |
| Categorical | `Parameter Name`, `Units of Measure` (both single-valued), `Qualifier`, `Method Type/Code/Name`, `County Name`, `site_id` |
| Grows over time | rows and monitors; a new pollutant would change the schema |

- `POC` must be in the key: without it there are 4,587 duplicates, with it 0. Sites run more than
  one instrument (Erie site `360290005`: 13,339 rows over 8,752 hours). Omitting it and then
  de-duplicating would delete 4,587 real readings.
- One pollutant only — `88101` (PM2.5) on all 8.1M rows, one unit — so no pivot to wide is needed.
- EPA "Local" is EST all year: `GMT − Local` is exactly 5.0 h on all 51,885 NYC rows, while New York
  clocks shift on 10 March 2024. In March both datasets have a "local" column and they differ by an
  hour, so we use the GMT pair.
- Six monitors: two each in the Bronx, Brooklyn and Queens. Manhattan and Staten Island have none,
  and Manhattan is where most pickups happen. Nearest-monitor assignment would look precise without
  being so, so we take a city-wide hourly mean. This is the main limitation in Task 5.
- The first filter drops 99.36% of the file (8,139,551 → 51,885), so it must be pushed into the read.
- Otherwise clean: no nulls, no negatives, 0.0–239.3 µg/m³, and all 8,784 hours have a city reading,
  so no gap-filling is needed.

## D. `taxi_zones`

| Question | Answer |
|---|---|
| Primary entity | one TLC taxi zone |
| Primary key | `LocationID` — unique, 1–265, no nulls. The only natural key in the platform |
| Join attributes | `LocationID`, joined twice (pickup and drop-off), aliased per side |
| Temporal | none, which is why this is the lookup table |
| Categorical | `Borough` (Queens 69, Manhattan 69, Brooklyn 61, Bronx 43, Staten Island 20, EWR 1, Unknown 1, `N/A` 1), `service_zone` (Boro Zone 205, Yellow Zone 55, Airports 2, `N/A` 2, EWR 1), `Zone` (262 names) |
| Grows over time | nothing. Stable since 2015, so kept as SCD Type 1 (overwrite) |

- Sentinels: `264` = Unknown / N/A / N/A, `265` = N/A / Outside of NYC / N/A.
- `"N/A"` is a real value, not a missing one. pandas converts it by default and invents four nulls,
  so the config states the null token explicitly instead of trusting a library default.

## Cross-cutting findings

1. Three time bases, none declared: taxi is local New York with DST, weather is UTC, EPA local is
   EST all year. Two had to be inferred from the data. Everything is converted to UTC — getting
   this wrong shifts every context join by 4–5 hours and raises no error.
2. The largest file is not the largest table. `air_quality` is 2.2 GB raw and 12 MB relevant, so
   partitioning matters for `taxi_trips` while read efficiency matters for `air_quality`.
3. Neither context dataset has a spatial key, and coverage is thin — one weather point, six air
   monitors, none in Manhattan. This limits how precise the integrated table can be.
4. De-duplication needs differ per dataset and cannot be read off the schema, so key-uniqueness
   checking is configured per dataset rather than fixed in code.
5. Validation must cover meaning, not just structure: `coco` passes every schema check and is 98%
   model output.
