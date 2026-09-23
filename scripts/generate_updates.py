"""Generate an incremental release of the three time-series datasets (Week 3, Task 1).

The municipality has published only the initial release, so the second one is
simulated from it. Every generated record starts from a real record of the same
dataset, shifted forward in time and perturbed, rather than being drawn from invented
distributions. That keeps the joint structure the analytics depend on: zone pairs
against fares, the weekday and hour-of-day cycle of demand, humidity against
precipitation, the diurnal PM2.5 cycle.

    python scripts/generate_updates.py
    python scripts/generate_updates.py --release v2 --seed 20240401 \\
        --new-frac 0.07 --dup-frac 0.015 --dup-base update

Reads data/raw/. Writes one file per dataset under data/updates/<release>/<dataset>/,
the machine-readable manifest data/updates/<release>/manifest.json, and the summary
reports/updates/<release>.md. Runs on pandas + pyarrow; no Spark needed.

The same seed produces the same files, which is why the files themselves are not
committed.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import functools
import glob
import json
import os

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "data", "raw")
UPDATES = os.path.join(ROOT, "data", "updates")
REPORTS = os.path.join(ROOT, "reports", "updates")

# --- taxi trips --------------------------------------------------------------------
# All taxi times here are New York wall clock, as in the source files.

# The template is the last full week of the original release, Monday 25 to Sunday 31
# March 2024. Replayed exactly one week later, every trip keeps its weekday and hour.
TAXI_TEMPLATE = ("2024-03-25", "2024-04-01")
TAXI_SHIFT = dt.timedelta(days=7)
# Pickup and dropoff move by the same random offset, so durations are unchanged while
# no new trip keeps the timestamps (and so the trip_id) of the trip it came from.
TAXI_JITTER_SEC = 600
TAXI_RELEASE_END = "2024-04-08"  # exclusive

# The labelled coverage of the original files. Duplicates are copied only from rows
# the platform accepted, so that each one is certain to be in silver already and the
# pipeline's duplicate count can be checked exactly against this manifest.
TAXI_ORIGINAL_WINDOW = ("2024-01-01", "2024-04-01")
# A trip spanning the spring-forward gap loses an hour when converted to UTC and can
# come out with a negative duration, so none is used as a duplicate.
TAXI_DST_GAP = ("2024-03-10 02:00", "2024-03-10 03:00")
# Mirrors the implausible_duration rule in conf/datasets/taxi_trips.yaml.
TAXI_MAX_DURATION_MIN = 1440

# The columns the platform hashes into trip_id (transforms/taxi_trips.py).
TAXI_KEY = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "total_amount",
]

# --- weather and air quality -------------------------------------------------------

# January 2024 replayed one year later. 2024 is a leap year, so one year is 366 days.
YEAR_SHIFT = dt.timedelta(days=366)
TEMPLATE_MONTH = "2024-01"

NYC_COUNTY_CODES = ["005", "047", "061", "081", "085"]
# Stamped on every new reading, as the EPA stamps the date a record last changed.
AQ_RELEASE_DATE = "2025-02-10"

# EPA PM2.5 AQI breakpoints, as revised in February 2024:
# (concentration low, concentration high, index low, index high), in ug/m3.
PM25_AQI_BREAKPOINTS = [
    (0.0, 9.0, 0, 50),
    (9.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 125.4, 151, 200),
    (125.5, 225.4, 201, 300),
    (225.5, 325.4, 301, 500),
]

# The ranges the assignment gives for the two new columns.
HUMIDITY_RANGE = (20.0, 100.0)
AQI_RANGE = (0, 500)


# --- helpers -----------------------------------------------------------------------


def rel(path: str) -> str:
    return os.path.relpath(path, ROOT).replace(os.sep, "/")


def ar1(rng: np.random.Generator, n: int, phi: float, sigma: float) -> np.ndarray:
    """A stationary AR(1) series: noise that drifts smoothly from hour to hour.

    Independent noise per hour would make temperature jump by a degree between
    consecutive observations, which real weather never does.
    """
    shocks = rng.normal(0.0, sigma, n)
    out = np.empty(n)
    out[0] = shocks[0] / np.sqrt(1.0 - phi**2)
    for i in range(1, n):
        out[i] = phi * out[i - 1] + shocks[i]
    return out


def ts(value: str) -> pa.Scalar:
    return pa.scalar(pd.Timestamp(value).to_pydatetime(), type=pa.timestamp("us"))


def shift_timestamps(column: pa.ChunkedArray, offset_us: np.ndarray) -> pa.Array:
    shifted = pc.add(column.combine_chunks().cast(pa.int64()), pa.array(offset_us))
    return shifted.cast(pa.timestamp("us"))


def all_of(conditions: list) -> np.ndarray:
    combined = pc.fill_null(functools.reduce(pc.and_, conditions), False)
    if isinstance(combined, pa.ChunkedArray):
        combined = combined.combine_chunks()
    return combined.to_numpy(zero_copy_only=False)


# --- taxi trips --------------------------------------------------------------------


def taxi_accepted(t: pa.Table, window: tuple[str, str], dst_gap: tuple[str, str] | None) -> np.ndarray:
    """Which rows the platform's reject rules keep, mirrored from taxi_trips.yaml.

    The platform checks these after converting to UTC; here the same bounds are
    applied in New York local time, which is what they were derived from.
    """
    pu, do = t["tpep_pickup_datetime"], t["tpep_dropoff_datetime"]
    distance = t["trip_distance"]
    duration_us = pc.subtract(do.cast(pa.int64()), pu.cast(pa.int64()))
    conditions = [
        pc.is_valid(pu), pc.is_valid(do),
        pc.is_valid(t["PULocationID"]), pc.is_valid(t["DOLocationID"]),
        pc.greater_equal(pu, ts(window[0])), pc.less(pu, ts(window[1])),
        pc.greater(duration_us, 0),
        pc.less_equal(duration_us, TAXI_MAX_DURATION_MIN * 60 * 1_000_000),
        pc.or_(pc.is_null(distance), pc.greater_equal(distance, 0.0)),
    ]
    if dst_gap:
        spans_gap = pc.and_(pc.less(pu, ts(dst_gap[1])), pc.greater_equal(do, ts(dst_gap[0])))
        conditions.append(pc.invert(spans_gap))
    return all_of(conditions)


def unique_on_key(t: pa.Table) -> np.ndarray:
    keys = t.select(TAXI_KEY).to_pandas()
    return ~keys.duplicated().to_numpy()


def generate_taxi(rng: np.random.Generator, out_dir: str, args: argparse.Namespace) -> dict:
    files = sorted(glob.glob(os.path.join(RAW, "taxi_trips", "*.parquet")))
    if not files:
        raise SystemExit("no taxi files in data/raw/taxi_trips/")
    schema = pq.read_schema(files[0]).remove_metadata()

    # Pass 1: counts, the latest pickup, and which rows may be copied as duplicates.
    needed = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
              "PULocationID", "DOLocationID"]
    original_rows, latest, eligible = 0, None, []
    for f in files:
        t = pq.read_table(f, columns=needed)
        original_rows += t.num_rows
        file_max = pc.max(t["tpep_pickup_datetime"]).as_py()
        latest = file_max if latest is None else max(latest, file_max)
        eligible.append(taxi_accepted(t, TAXI_ORIGINAL_WINDOW, TAXI_DST_GAP))

    n_new = round(args.new_frac * original_rows)
    if args.dup_base == "update":
        # dup_frac is the duplicates' share of the update file: d / (n + d) = f.
        n_dup = round(n_new * args.dup_frac / (1.0 - args.dup_frac))
    else:
        n_dup = round(args.dup_frac * original_rows)

    # Pass 2: copy the duplicates and collect the template week.
    per_file = rng.multivariate_hypergeometric([int(m.sum()) for m in eligible], n_dup)
    duplicates, template = [], []
    for f, mask, k in zip(files, eligible, per_file):
        t = pq.read_table(f).cast(schema)
        picks = np.sort(rng.choice(np.flatnonzero(mask), size=int(k), replace=False))
        duplicates.append(t.take(pa.array(picks)))
        pu = t["tpep_pickup_datetime"]
        in_week = all_of([pc.greater_equal(pu, ts(TAXI_TEMPLATE[0])), pc.less(pu, ts(TAXI_TEMPLATE[1]))])
        template.append(t.filter(pa.array(in_week)))
    duplicates = pa.concat_tables(duplicates)
    template = pa.concat_tables(template).combine_chunks()

    # Replay the template week one week later, with the per-trip jitter.
    jitter = rng.integers(-TAXI_JITTER_SEC, TAXI_JITTER_SEC + 1, size=template.num_rows)
    offset_us = (jitter + int(TAXI_SHIFT.total_seconds())) * 1_000_000
    for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        idx = template.schema.get_field_index(name)
        template = template.set_column(idx, name, shift_timestamps(template[name], offset_us))

    # Strictly after the latest trip of the original release, and inside the new week.
    pu = template["tpep_pickup_datetime"]
    after_latest = all_of([
        pc.greater(pu, pa.scalar(latest, type=pa.timestamp("us"))),
        pc.less(pu, ts(TAXI_RELEASE_END)),
    ])
    candidates = np.flatnonzero(after_latest)
    if len(candidates) < n_new:
        raise SystemExit(f"template week has {len(candidates):,} usable trips, {n_new:,} requested")
    new = template.take(pa.array(np.sort(rng.choice(candidates, size=n_new, replace=False))))

    # One key per row across the whole file, so every duplicate the pipeline finds is
    # one of the copies below and nothing else.
    update = pa.concat_tables([new, duplicates])
    is_duplicate = np.r_[np.zeros(new.num_rows, bool), np.ones(duplicates.num_rows, bool)]
    keep = unique_on_key(update)
    update, is_duplicate = update.filter(pa.array(keep)), is_duplicate[keep]
    order = rng.permutation(update.num_rows)
    update, is_duplicate = update.take(pa.array(order)), is_duplicate[order]

    path = os.path.join(out_dir, "taxi_trips", "yellow_tripdata_2024-04_v2.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(update, path, compression="snappy")

    new_part = update.filter(pa.array(~is_duplicate))
    new_accepted = taxi_accepted(new_part, (TAXI_TEMPLATE[0], TAXI_RELEASE_END), None)
    new_pu = new_part["tpep_pickup_datetime"]
    n_new, n_dup = int((~is_duplicate).sum()), int(is_duplicate.sum())
    return {
        "file": rel(path),
        "format": "parquet",
        "rows": update.num_rows,
        "new_records": n_new,
        "duplicate_records": n_dup,
        "duplicate_share_of_file": round(n_dup / update.num_rows, 5),
        "new_share_of_original": round(n_new / original_rows, 5),
        "duplicate_base": args.dup_base,
        "original_records": original_rows,
        "latest_original_record": str(latest),
        "coverage": {
            "from": str(pc.min(new_pu).as_py()),
            "to": str(pc.max(new_pu).as_py()),
            "time_basis": "tpep_pickup_datetime, New York local",
        },
        "schema_changes": [],
        "method": (
            f"New trips: trips picked up {TAXI_TEMPLATE[0]} to {TAXI_TEMPLATE[1]} (exclusive) "
            f"sampled without replacement, moved forward {TAXI_SHIFT.days} days with one random "
            f"offset of up to +/-{TAXI_JITTER_SEC // 60} min per trip applied to pickup and "
            f"dropoff, and kept only if picked up after {latest}. Duplicates: exact copies of "
            f"original rows that pass every reject rule, sampled uniformly across the "
            f"{len(files)} original files."
        ),
        "expected": {
            "target_grain": "one row per trip (trip_id)",
            "rejected_by_quality_rules": int(n_new - new_accepted.sum()),
            "inserted": int(new_accepted.sum()),
            "duplicates_ignored": n_dup,
        },
    }


# --- weather -----------------------------------------------------------------------


def generate_weather(rng: np.random.Generator, out_dir: str) -> dict:
    src = os.path.join(RAW, "weather", "weather.csv")
    df = pd.read_csv(src)
    columns = list(df.columns)

    when = pd.to_datetime(df[["year", "month", "day", "hour"]])
    tpl = df[when.dt.strftime("%Y-%m") == TEMPLATE_MONTH].reset_index(drop=True)
    new_when = pd.to_datetime(tpl[["year", "month", "day", "hour"]]) + YEAR_SHIFT
    if new_when.min() <= when.max():
        raise SystemExit("weather: the template does not start after the original data")
    n = len(tpl)

    out = tpl.copy()
    out["year"], out["month"] = new_when.dt.year, new_when.dt.month
    out["day"], out["hour"] = new_when.dt.day, new_when.dt.hour

    out["temp"] = (tpl["temp"] + ar1(rng, n, phi=0.9, sigma=0.4)).round(1)
    out["pres"] = (tpl["pres"] + ar1(rng, n, phi=0.95, sigma=0.3)).round(1)

    # Relative humidity is generated once as a continuous value. The new column carries
    # it to one decimal; the legacy integer column rhum is the same value rounded, so
    # the two never disagree.
    humidity = np.clip(tpl["rhum"] + ar1(rng, n, phi=0.9, sigma=1.5), *HUMIDITY_RANGE).round(1)
    out["humidity"] = humidity
    out["rhum"] = humidity.round().astype(int)

    # Calm hours stay calm and dry hours stay dry; only non-zero values are scaled.
    wet = tpl["prcp"] > 0
    out.loc[wet, "prcp"] = np.maximum(
        0.1, (tpl.loc[wet, "prcp"] * rng.lognormal(0.0, 0.25, wet.sum())).round(1)
    )
    windy = tpl["wspd"] > 0
    out.loc[windy, "wspd"] = (tpl.loc[windy, "wspd"] * rng.lognormal(0.0, 0.15, windy.sum())).round(1)
    turned = (tpl["wdir"] + rng.normal(0.0, 15.0, n)) / 10.0
    out["wdir"] = np.where(windy, (turned.round() * 10).astype(int) % 360, tpl["wdir"])

    # Integers must not be written as 6.0, or Spark's inferSchema reads a double.
    for column in ("year", "month", "day", "hour", "rhum", "wdir", "cldc"):
        out[column] = out[column].astype(int)
    out["coco"] = out["coco"].astype("Int64")

    out = out[columns + ["humidity"]]
    path = os.path.join(out_dir, "weather", "weather_2025-01_v2.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    out.to_csv(path, index=False)

    # Mirrors the reject rules in conf/datasets/weather.yaml.
    failing = (
        ~out["temp"].between(-40, 55)
        | ~out["rhum"].between(0, 100)
        | ~out["wdir"].between(0, 360)
        | (out["prcp"] < 0)
    )
    return {
        "file": rel(path),
        "format": "csv",
        "rows": n,
        "new_records": n,
        "duplicate_records": 0,
        "coverage": {
            "from": str(new_when.min()),
            "to": str(new_when.max()),
            "time_basis": "year/month/day/hour, UTC",
        },
        "schema_changes": [{
            "change": "add_column",
            "column": "humidity",
            "type": "double",
            "position": "last",
            "meaning": "relative humidity in percent, one decimal; rhum is the same value rounded",
            "valid_range": list(HUMIDITY_RANGE),
            "observed_range": [float(humidity.min()), float(humidity.max())],
        }],
        "method": (
            f"{TEMPLATE_MONTH} replayed {YEAR_SHIFT.days} days later. temp and pres get "
            "AR(1) noise, humidity is rhum plus AR(1) noise clipped to 20-100, non-zero "
            "wind speed and precipitation are scaled by log-normal factors, wind direction "
            "is turned by N(0, 15) degrees. Cloud cover, condition code and provenance "
            "columns are kept from the template hour."
        ),
        "expected": {
            "target_grain": "one row per hour (obs_hour)",
            "rejected_by_quality_rules": int(failing.sum()),
            "inserted": int((~failing).sum()),
            "duplicates_ignored": 0,
        },
    }


# --- air quality -------------------------------------------------------------------


def read_nyc_air_quality() -> tuple[pd.DataFrame, list[str]]:
    """Stream the 2.2 GB national extract and keep the New York City rows.

    Every column is read as a string: the site identifiers are zero-padded, and the
    output must reproduce them exactly.
    """
    files = sorted(glob.glob(os.path.join(RAW, "air_quality", "*.csv")))
    if not files:
        raise SystemExit("no air quality file in data/raw/air_quality/")
    with open(files[0], newline="", encoding="utf-8") as fh:
        columns = next(csv.reader(fh))

    convert = pcsv.ConvertOptions(
        column_types={c: pa.string() for c in columns}, strings_can_be_null=False
    )
    counties = pa.array(NYC_COUNTY_CODES)
    kept = []
    for f in files:
        reader = pcsv.open_csv(
            f, read_options=pcsv.ReadOptions(block_size=64 << 20), convert_options=convert
        )
        for batch in reader:
            in_nyc = pc.and_(
                pc.equal(batch.column("State Code"), "36"),
                pc.is_in(batch.column("County Code"), value_set=counties),
            )
            kept.append(batch.filter(in_nyc))
    return pa.Table.from_batches(kept).to_pandas(), columns


def pm25_aqi(concentration: np.ndarray) -> np.ndarray:
    """EPA's piecewise-linear AQI for PM2.5.

    The concentration is truncated to one decimal first, as the EPA method requires.
    Negative readings, which the platform rejects anyway, map to 0.
    """
    c = np.floor(np.clip(concentration, 0.0, None) * 10 + 1e-6) / 10
    c_lo, c_hi, i_lo, i_hi = (np.array(v, dtype=float) for v in zip(*PM25_AQI_BREAKPOINTS))
    band = np.minimum(np.searchsorted(c_hi, c, side="left"), len(c_hi) - 1)
    index = (i_hi[band] - i_lo[band]) / (c_hi[band] - c_lo[band]) * (c - c_lo[band]) + i_lo[band]
    return np.clip(np.rint(index), *AQI_RANGE).astype(int)


def format_measurement(value: float) -> str:
    """Write a reading the way the EPA extract does: 14 rather than 14.0."""
    text = f"{value:.1f}"
    text = text[:-2] if text.endswith(".0") else text
    return "0" if text == "-0" else text


def generate_air_quality(rng: np.random.Generator, out_dir: str) -> dict:
    df, columns = read_nyc_air_quality()
    gmt = pd.to_datetime(df["Date GMT"] + " " + df["Time GMT"])

    # The extract is organised by local date, so the template month is too.
    tpl = df[df["Date Local"].str.startswith(TEMPLATE_MONTH)].reset_index(drop=True)
    tpl_gmt = pd.to_datetime(tpl["Date GMT"] + " " + tpl["Time GMT"])
    new_gmt = tpl_gmt + YEAR_SHIFT
    if new_gmt.min() <= gmt.max():
        raise SystemExit("air_quality: the template does not start after the original data")

    out = tpl.copy()
    for column in ("Date Local", "Date GMT"):
        out[column] = (pd.to_datetime(tpl[column]) + YEAR_SHIFT).dt.strftime("%Y-%m-%d")
    out["Date of Last Change"] = AQ_RELEASE_DATE

    # One smooth multiplicative factor per instrument, so each monitor's series keeps
    # its diurnal shape and moves the way pollution does, hour to hour.
    measured = tpl["Sample Measurement"].astype(float).to_numpy()
    factor = np.ones(len(tpl))
    instrument = ["State Code", "County Code", "Site Num", "POC"]
    for rows in tpl.groupby(instrument).indices.values():
        rows = rows[np.argsort(tpl_gmt.to_numpy()[rows])]
        factor[rows] = np.exp(ar1(rng, len(rows), phi=0.9, sigma=0.08))
    value = np.round(measured * factor, 1)
    out["Sample Measurement"] = [format_measurement(v) for v in value]
    out["aqi"] = pm25_aqi(value)

    out = out[columns + ["aqi"]]
    path = os.path.join(out_dir, "air_quality", "hourly_88101_2025-01_v2.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    out.to_csv(path, index=False)

    # Mirrors the implausible_pm25 rule; the silver table holds one row per hour with at
    # least one accepted reading.
    accepted = (value >= 0) & (value <= 1000)
    hours = new_gmt.dt.floor("h")
    return {
        "file": rel(path),
        "format": "csv",
        "rows": len(out),
        "new_records": len(out),
        "new_hours": int(hours.nunique()),
        "duplicate_records": 0,
        "monitors": int(tpl.groupby(instrument).ngroups),
        "coverage": {
            "from": str(new_gmt.min()),
            "to": str(new_gmt.max()),
            "time_basis": "Date GMT + Time GMT, UTC",
        },
        "schema_changes": [{
            "change": "add_column",
            "column": "aqi",
            "type": "integer",
            "position": "last",
            "meaning": "EPA PM2.5 Air Quality Index of the reading (2024 breakpoints)",
            "valid_range": list(AQI_RANGE),
            "observed_range": [int(out["aqi"].min()), int(out["aqi"].max())],
        }],
        "method": (
            f"New York City readings with a local date in {TEMPLATE_MONTH}, replayed "
            f"{YEAR_SHIFT.days} days later. Each instrument's series is multiplied by a "
            "smooth log-normal AR(1) factor. aqi is computed from the new concentration."
        ),
        "expected": {
            "target_grain": "one row per UTC hour (obs_hour), all monitors aggregated",
            "rejected_by_quality_rules": int((~accepted).sum()),
            "inserted": int(hours[accepted].nunique()),
            "duplicates_ignored": 0,
        },
    }


# --- manifest and report -----------------------------------------------------------


def write_report(manifest: dict, path: str) -> None:
    ds = manifest["datasets"]
    lines = [
        f"# Incremental release `{manifest['release']}`",
        "",
        f"Generated by `{manifest['generator']}` with seed {manifest['seed']}. "
        f"The machine-readable version is `data/updates/{manifest['release']}/manifest.json`.",
        "",
        "## What each update file contains",
        "",
        "| Dataset | File | Rows | New records | Duplicates | Schema change |",
        "|---|---|---:|---:|---:|---|",
    ]
    for name, info in ds.items():
        changes = ", ".join(
            f"`{c['column']}` ({c['type']}) added" for c in info["schema_changes"]
        ) or "none"
        lines.append(
            f"| {name} | `{info['file']}` | {info['rows']:,} | {info['new_records']:,} "
            f"| {info['duplicate_records']:,} | {changes} |"
        )

    taxi = ds.get("taxi_trips")
    if taxi:
        lines += [
            "",
            f"The taxi file holds {taxi['new_share_of_original']:.2%} new trips relative to "
            f"the {taxi['original_records']:,} original trips. Its duplicates make up "
            f"{taxi['duplicate_share_of_file']:.2%} of the file.",
        ]

    lines += ["", "## Per dataset", ""]
    for name, info in ds.items():
        cov = info["coverage"]
        lines += [
            f"### {name}",
            "",
            f"- Coverage: {cov['from']} to {cov['to']} ({cov['time_basis']})",
        ]
        if "latest_original_record" in info:
            lines.append(f"- Latest record of the original release: {info['latest_original_record']}")
        if "monitors" in info:
            lines.append(f"- Monitors: {info['monitors']}, hours covered: {info['new_hours']:,}")
        for change in info["schema_changes"]:
            lo, hi = change["observed_range"]
            lines.append(
                f"- Schema change: new column `{change['column']}` ({change['type']}, "
                f"{change['position']}): {change['meaning']}. Generated values {lo} to {hi}."
            )
        lines.append(f"- Method: {info['method']}")
        exp = info["expected"]
        lines += [
            f"- Expected pipeline outcome: {exp['inserted']:,} inserted, "
            f"{exp['duplicates_ignored']:,} duplicates ignored, "
            f"{exp['rejected_by_quality_rules']:,} source rows rejected by quality rules. "
            f"Counted at the grain of the silver table, {exp['target_grain']}.",
            "",
        ]

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--release", default="v2")
    parser.add_argument("--seed", type=int, default=20240401)
    parser.add_argument("--new-frac", type=float, default=0.07,
                        help="new taxi trips as a share of the original trips (0.05-0.10)")
    parser.add_argument("--dup-frac", type=float, default=0.015,
                        help="duplicate taxi trips as a share of --dup-base (0.01-0.02)")
    parser.add_argument("--dup-base", choices=["update", "original"], default="update",
                        help="what --dup-frac is a share of: the update file or the original data")
    args = parser.parse_args()
    if not 0.05 <= args.new_frac <= 0.10:
        parser.error("--new-frac must be between 0.05 and 0.10")
    if not 0.01 <= args.dup_frac <= 0.02:
        parser.error("--dup-frac must be between 0.01 and 0.02")

    out_dir = os.path.join(UPDATES, args.release)
    generators = [
        ("taxi_trips", lambda rng: generate_taxi(rng, out_dir, args)),
        ("weather", lambda rng: generate_weather(rng, out_dir)),
        ("air_quality", lambda rng: generate_air_quality(rng, out_dir)),
    ]

    datasets = {}
    for i, (name, generate) in enumerate(generators):
        # One stream per dataset, so changing one generator leaves the others' output
        # unchanged.
        rng = np.random.default_rng([args.seed, i])
        print(f"--- {name}")
        info = datasets[name] = generate(rng)
        print(f"    {info['file']}: {info['rows']:,} rows "
              f"({info['new_records']:,} new, {info['duplicate_records']:,} duplicates)")
        for change in info["schema_changes"]:
            print(f"    schema change: + {change['column']} {change['type']} "
                  f"{change['observed_range']}")

    manifest = {
        "release": args.release,
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "generator": "scripts/generate_updates.py",
        "seed": args.seed,
        "parameters": {"new_frac": args.new_frac, "dup_frac": args.dup_frac,
                       "dup_base": args.dup_base},
        "datasets": datasets,
    }
    manifest_path = os.path.join(out_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)
    report_path = os.path.join(REPORTS, f"{args.release}.md")
    write_report(manifest, report_path)
    print(f"\nwrote {rel(manifest_path)} and {rel(report_path)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
