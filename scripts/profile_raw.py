"""Profile the raw source files and print the measurements quoted in docs/data_catalog.md.

Runs on plain pandas + pyarrow (no Spark, no JVM) so the data catalog can be verified
before the platform itself is stood up.

    python scripts/profile_raw.py

Reads only from data/raw/ and writes nothing.
"""

import glob
import os

import pandas as pd
import pyarrow.parquet as pq

RAW = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")

# NYC's five boroughs as EPA county codes, within state 36 (New York).
NYC_COUNTY_CODES = ["005", "047", "061", "081", "085"]

# The window the taxi files are labelled with; anything outside is a data error.
TRIP_WINDOW = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-04-01"))


def header(title):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def profile_taxi_trips():
    header("A. TAXI TRIPS (parquet)")
    files = sorted(glob.glob(os.path.join(RAW, "taxi_trips", "*.parquet")))
    if not files:
        print("  no files found")
        return

    total = 0
    for f in files:
        n = pq.ParquetFile(f).metadata.num_rows
        total += n
        print(f"  {os.path.basename(f)}: {n:>10,} rows, {os.path.getsize(f) / 1e6:6.1f} MB")
    raw_bytes = sum(os.path.getsize(f) for f in files)
    print(f"  TOTAL: {total:,} rows, {raw_bytes / 1e6:.0f} MB")

    print("\n  schema:")
    for fld in pq.ParquetFile(files[0]).schema_arrow:
        print(f"    {fld.name:<26} {fld.type}")

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    print("\n  null counts (co-occurring block):")
    nn = df.isna().sum()
    for c in df.columns:
        if nn[c]:
            print(f"    {c:<26} {nn[c]:>9,}  ({nn[c] / len(df) * 100:5.2f}%)")

    print("\n  null share by vendor (is the null block vendor-specific?):")
    by_vendor = df.assign(is_null=df["Airport_fee"].isna()).groupby("VendorID")["is_null"]
    for vendor, grp in by_vendor:
        print(f"    vendor {vendor}: {grp.sum():>8,} / {grp.size:>9,}  ({grp.mean() * 100:5.1f}%)")

    pu = df["tpep_pickup_datetime"]
    lo, hi = TRIP_WINDOW
    oob = (pu < lo) | (pu >= hi)
    print(f"\n  pickup range: {pu.min()}  ->  {pu.max()}")
    print(f"  out-of-window pickups: {oob.sum():,} ({oob.sum() / len(df) * 100:.4f}%)")
    print(f"  out-of-window years: {sorted(pu[oob].dt.year.unique().tolist())}")
    print(f"  distinct pickup dates: {pu.dt.date.nunique()} total, "
          f"{pu[~oob].dt.date.nunique()} inside the window")

    per_day = df[~oob].groupby(pu[~oob].dt.date).size()
    print(f"  rows/day: min={per_day.min():,} median={int(per_day.median()):,} "
          f"max={per_day.max():,}")
    print(f"  => daily partition would average {raw_bytes / 1e6 / len(per_day):.1f} MB")

    cand = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "PULocationID", "DOLocationID", "total_amount"]
    print(f"\n  candidate-key duplicates: {df.duplicated(subset=cand).sum():,} / {len(df):,}")
    print(f"  full-row duplicates:      {df.duplicated().sum():,}")

    dur = (df["tpep_dropoff_datetime"] - pu).dt.total_seconds() / 60
    print(f"\n  duration <= 0 min: {(dur <= 0).sum():,}   > 24 h: {(dur > 1440).sum():,}   "
          f"max: {dur.max():.0f} min")
    print(f"  trip_distance == 0:   {(df.trip_distance == 0).sum():,}")
    print(f"  passenger_count == 0: {(df.passenger_count == 0).sum():,}")
    print(f"  negative total_amount: {(df.total_amount < 0).sum():,}")
    print(f"  sentinel zones 264/265 — PU: {df.PULocationID.isin([264, 265]).sum():,}  "
          f"DO: {df.DOLocationID.isin([264, 265]).sum():,}")

    for c in ["VendorID", "RatecodeID", "payment_type", "store_and_fwd_flag"]:
        print(f"  {c} domain: {sorted(df[c].dropna().unique().tolist())}")

    # The columns carry no timezone. Infer it from the demand curve: taxi demand
    # bottoms out around 04:00 and peaks in the evening rush, in LOCAL time.
    by_hour = pu[~oob].dt.hour.value_counts().sort_index()
    print(f"\n  [timezone] trips per hour: trough at {by_hour.idxmin():02d}:00 "
          f"({by_hour.min():,}), peak at {by_hour.idxmax():02d}:00 ({by_hour.max():,})")
    print("  => matches a local wall clock; taxi timestamps are America/New_York")


def profile_weather():
    header("B. WEATHER (csv)")
    w = pd.read_csv(os.path.join(RAW, "weather", "weather.csv"))
    print(f"  {len(w):,} rows x {len(w.columns)} cols   (366 * 24 = {366 * 24})")

    print("\n  columns:")
    for c in w.columns:
        n = w[c].isna().sum()
        print(f"    {c:<14} {str(w[c].dtype):<9} nulls={n:>5} ({n / len(w) * 100:5.1f}%)")

    key = ["year", "month", "day", "hour"]
    print(f"\n  key {tuple(key)} duplicates: {w.duplicated(subset=key).sum()}")
    print(f"  years present: {sorted(w.year.unique())}")

    # The *_source columns say whether each value was observed or model-generated.
    print("\n  provenance (dwd_mosmix = numerical forecast model, not an observation):")
    for c in [c for c in w.columns if c.endswith("_source")]:
        counts = w[c].value_counts(dropna=False).to_dict()
        print(f"    {c:<14} {counts}")

    # Temperature bottoms out ~05:00 and peaks ~15:00 LOCAL; the offset reveals the base.
    t = w.groupby("hour").temp.mean()
    print(f"\n  [timezone] mean temp: trough at {t.idxmin():02d}:00 ({t.min():.2f} C), "
          f"peak at {t.idxmax():02d}:00 ({t.max():.2f} C)")
    print("  => shifted +5 h from local; weather timestamps are UTC")


def profile_air_quality():
    header("C. AIR QUALITY (csv)")
    path = os.path.join(RAW, "air_quality", "hourly_88101_2024.csv")
    print(f"  raw file: {os.path.getsize(path) / 1e9:.2f} GB")

    cols = ["State Code", "County Code", "Site Num", "Parameter Code", "POC",
            "Date GMT", "Time GMT", "Date Local", "Time Local", "Sample Measurement",
            "Units of Measure", "Qualifier", "County Name"]
    str_cols = {"State Code": str, "County Code": str, "Site Num": str, "Parameter Code": str}

    total = 0
    ny_chunks, nyc_chunks = [], []
    for chunk in pd.read_csv(path, usecols=cols, dtype=str_cols, chunksize=500_000):
        total += len(chunk)
        ny = chunk[chunk["State Code"] == "36"]
        ny_chunks.append(ny)
        nyc_chunks.append(ny[ny["County Code"].isin(NYC_COUNTY_CODES)])
    ny = pd.concat(ny_chunks, ignore_index=True)
    nyc = pd.concat(nyc_chunks, ignore_index=True)

    print(f"  national rows: {total:,}")
    print(f"  New York State: {len(ny):,} ({len(ny) / total * 100:.2f}%)")
    print(f"  New York City:  {len(nyc):,} ({len(nyc) / total * 100:.2f}%)")
    print(f"  => the first filter discards {100 - len(nyc) / total * 100:.2f}% of the largest file")

    print(f"\n  distinct Parameter Code: {ny['Parameter Code'].unique().tolist()}")
    print(f"  distinct units:          {ny['Units of Measure'].unique().tolist()}")

    ny["site_id"] = ny["State Code"] + ny["County Code"] + ny["Site Num"]
    print(f"\n  NYC monitors by borough ({nyc['County Name'].nunique()} counties, "
          f"{len(nyc.groupby(['County Name', 'Site Num']))} sites):")
    print(nyc.groupby(["County Name", "Site Num"]).size().to_string(header=False))
    missing = {"Bronx", "Kings", "New York", "Queens", "Richmond"} - set(nyc["County Name"])
    print(f"  boroughs with NO monitor: {sorted(missing)}")

    # POC distinguishes co-located instruments; omitting it turns real readings into
    # apparent duplicates.
    without_poc = ["site_id", "Parameter Code", "Date GMT", "Time GMT"]
    print(f"\n  POC values: {sorted(ny.POC.unique().tolist())}")
    print(f"  duplicates WITHOUT poc in key: {ny.duplicated(subset=without_poc).sum():,}")
    print(f"  duplicates WITH    poc in key: "
          f"{ny.duplicated(subset=without_poc + ['POC']).sum():,}")

    gmt = pd.to_datetime(nyc["Date GMT"] + " " + nyc["Time GMT"])
    local = pd.to_datetime(nyc["Date Local"] + " " + nyc["Time Local"])
    offsets = ((gmt - local).dt.total_seconds() / 3600).value_counts().to_dict()
    print(f"\n  [timezone] GMT - Local offset hours: {offsets}")
    print("  => constant across the year; EPA 'Local' is EST year-round, it ignores DST")

    hourly = nyc.groupby(gmt)["Sample Measurement"].mean()
    print(f"\n  city-wide hourly coverage: {len(hourly):,} / 8784 "
          f"({len(hourly) / 8784 * 100:.1f}%)")
    print(f"  city-wide PM2.5 ug/m3: min={hourly.min():.1f} median={hourly.median():.1f} "
          f"max={hourly.max():.1f}")
    print(f"  null measurements: {nyc['Sample Measurement'].isna().sum():,}   "
          f"negative: {(nyc['Sample Measurement'] < 0).sum():,}")
    print(f"  non-null Qualifier: {nyc['Qualifier'].notna().sum():,}")


def profile_taxi_zones():
    header("D. TAXI ZONES (csv)")
    path = os.path.join(RAW, "taxi_zones", "taxi_zone_lookup.csv")

    # keep_default_na=False: "N/A" is a literal value in this file, not a missing marker.
    z = pd.read_csv(path, keep_default_na=False)
    print(f"  {len(z)} rows; LocationID unique: {z.LocationID.is_unique}; "
          f"range {z.LocationID.min()}-{z.LocationID.max()}")
    print(f"  Borough:      {z.Borough.value_counts().to_dict()}")
    print(f"  service_zone: {z.service_zone.value_counts().to_dict()}")
    print(f"  distinct Zone names: {z.Zone.nunique()}")
    print("\n  sentinel rows:")
    print(z[z.LocationID.isin([264, 265])].to_string(index=False))

    naive = pd.read_csv(path)
    print(f"\n  nulls with pandas defaults: {naive.isna().sum().to_dict()}")
    print(f"  nulls with keep_default_na=False: {z.isna().sum().to_dict()}")
    print("  => pandas coerces the literal 'N/A' to NaN; null tokens must be declared per dataset")


if __name__ == "__main__":
    profile_taxi_trips()
    profile_weather()
    profile_air_quality()
    profile_taxi_zones()
    print()
