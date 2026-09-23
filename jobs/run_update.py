"""Apply one incremental release to the silver tables.

    python jobs/run_update.py v2                  # every dataset the release has a file for
    python jobs/run_update.py v2 weather          # only these

Each dataset goes through the same pipeline as the initial load, reading the release's
files from data/updates/<release>/<dataset>/ and merging into the existing table:
new rows are inserted, rows already present are ignored, new columns are added.
Re-applying a release inserts nothing.

Generate a release with scripts/generate_updates.py.
"""

from __future__ import annotations

import glob
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.config import PlatformConfig, load_datasets  # noqa: E402
from urbanplatform.ingest import ingest_all  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402


def main(argv: list[str]) -> int:
    if not argv or argv[0].startswith("-"):
        print(__doc__)
        return 2
    release, names = argv[0], argv[1:]

    platform = PlatformConfig.load()
    configs = []
    for cfg in load_datasets(names or None):
        if cfg.incremental is None:
            print(f"{cfg.name}: no incremental section in its config, skipped")
            continue
        update = cfg.for_release(release)
        if not glob.glob(update.source.abs_path):
            print(f"{cfg.name}: release {release} has no file at {update.source.path}, skipped")
            continue
        configs.append(update)
    if not configs:
        print(f"nothing to apply for release {release}")
        return 1
    print()

    spark = build_session(platform)
    try:
        runs = ingest_all(spark, platform, configs, release=release)
    finally:
        spark.stop()

    print("\n" + "=" * 80)
    print(f"{'dataset':<14} {'status':<9} {'read':>9} {'rejected':>9} {'inserted':>10} "
          f"{'duplicate':>10} {'sec':>7}  new columns")
    print("-" * 80)
    for run in runs:
        new_columns = ""
        if run.schema_drift and "unexpected_columns" in run.schema_drift:
            new_columns = ", ".join(json.loads(run.schema_drift)["unexpected_columns"])
        print(
            f"{run.dataset_name:<14} {run.status:<9} {run.records_read or 0:>9,} "
            f"{run.records_rejected or 0:>9,} {run.records_inserted or 0:>10,} "
            f"{run.records_duplicate or 0:>10,} {run.duration_sec or '':>7}  {new_columns}"
        )
    print("=" * 80)

    return 1 if any(run.status != "success" for run in runs) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
