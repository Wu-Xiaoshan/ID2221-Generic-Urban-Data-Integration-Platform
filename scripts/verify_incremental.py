"""Correctness checks for an applied incremental release (Week 3, Task 1).

    python scripts/verify_incremental.py v2

For every dataset in the release manifest, the table as it was just before the
release's first merge (read with Delta time travel) is compared with the table now:

  counts       rows now = rows before + rows inserted by the release's merges
  unique key   the merge key is still unique
  preserved    every row that existed before is still there, unchanged
  new columns  each added column is null on the rows that existed before, and set and
               within the manifest's declared range on the rows the release added
  expected     inserted, ignored and rejected counts equal the manifest's expectation
  idempotent   any later run of the same release inserted nothing

Run jobs/run_update.py <release> first. Exits non-zero on failure, and writes the
results to reports/updates/<release>_verification.md.
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from pyspark.sql import functions as F  # noqa: E402

from urbanplatform.config import REPO_ROOT, PlatformConfig, load_datasets  # noqa: E402
from urbanplatform.session import build_session  # noqa: E402


class Checks:
    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.results: list[tuple[str, str, bool, str]] = []
        self.dataset = ""

    def __call__(self, name: str, condition: bool, detail: str) -> None:
        condition = bool(condition)
        self.results.append((self.dataset, name, condition, detail))
        if condition:
            self.passed += 1
            print(f"  PASS  {name}: {detail}")
        else:
            self.failed += 1
            print(f"  FAIL  {name}: {detail}")


# Commits that change no rows, which a release's merges can be interleaved with.
_MAINTENANCE = {"OPTIMIZE", "SET TBLPROPERTIES", "VACUUM START", "VACUUM END"}


def release_merges(spark, path: str):
    """The merge commits since the table was last loaded or restored, oldest first.

    Walking back from the latest commit, merges are collected and maintenance commits
    skipped. Anything else (the initial WRITE, a RESTORE, a full re-ingest) ends the
    walk, because it produced the state the merges were applied to. Its timestamp is
    returned too: runs recorded before it belong to an earlier state of the table.
    """
    from delta.tables import DeltaTable

    history = (
        DeltaTable.forPath(spark, path).history()
        .select("version", "timestamp", "operation", "operationMetrics")
        .orderBy(F.desc("version"))
        .collect()
    )
    merges, baseline_at = [], None
    for row in history:
        if row["operation"] == "MERGE":
            metrics = row["operationMetrics"] or {}
            merges.append({
                "version": int(row["version"]),
                "source_rows": int(metrics.get("numSourceRows", 0)),
                "inserted": int(metrics.get("numTargetRowsInserted", 0)),
            })
        elif row["operation"] not in _MAINTENANCE:
            baseline_at = row["timestamp"]
            break
    return sorted(merges, key=lambda m: m["version"]), baseline_at


def write_report(release: str, checks: Checks, facts: dict[str, dict], path: str) -> None:
    lines = [
        f"# Release `{release}`: verification",
        "",
        f"Written by `scripts/verify_incremental.py {release}` after `jobs/run_update.py {release}`.",
        f"{checks.passed} checks passed, {checks.failed} failed.",
        "",
        "## Measured outcome",
        "",
        "| Dataset | Rows before | Inserted | Duplicates ignored | Rejected | Rows after "
        "| New columns | Runs of the release |",
        "|---|---:|---:|---:|---:|---:|---|---:|",
    ]
    for name, f in facts.items():
        lines.append(
            f"| {name} | {f['before']:,} | {f['inserted']:,} | {f['duplicates']:,} "
            f"| {f['rejected']} | {f['after']:,} | {', '.join(f['new_columns']) or 'none'} "
            f"| {f['runs']} |"
        )
    lines += ["", "## Checks", "", "| Dataset | Check | Result | Detail |", "|---|---|---|---|"]
    for dataset, name, ok, detail in checks.results:
        lines.append(f"| {dataset} | {name} | {'pass' if ok else '**FAIL**'} | {detail} |")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    release = argv[0]
    manifest_path = os.path.join(REPO_ROOT, "data", "updates", release, "manifest.json")
    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)

    platform = PlatformConfig.load()
    configs = {cfg.name: cfg for cfg in load_datasets(list(manifest["datasets"]))}
    spark = build_session(platform)
    check = Checks()
    facts: dict[str, dict] = {}

    try:
        metadata = spark.read.format("delta").load(platform.meta_path("ingestion_metadata"))
        if "release" in metadata.columns:
            metadata = metadata.filter((F.col("release") == release) & (F.col("status") == "success"))
        else:
            metadata = None

        for name, info in manifest["datasets"].items():
            cfg = configs[name]
            key = cfg.incremental.merge_key
            path = platform.table_path(cfg.target.layer, cfg.target.table)
            check.dataset = name
            print(f"\n--- {name}  ({cfg.target.table}, key {', '.join(key)})")

            merges, baseline_at = release_merges(spark, path)
            check("applied", merges, f"{len(merges)} merge commit(s) in the table history")
            if not merges:
                continue
            first = merges[0]
            before = spark.read.format("delta").option("versionAsOf", first["version"] - 1).load(path)
            now = spark.read.format("delta").load(path)

            inserted_total = sum(m["inserted"] for m in merges)
            n_before, n_now = before.count(), now.count()
            check("counts", n_now == n_before + inserted_total,
                  f"{n_before:,} before + {inserted_total:,} inserted = {n_before + inserted_total:,}, "
                  f"table has {n_now:,}")

            dup_keys = now.groupBy(*key).count().filter("count > 1").count()
            check("unique key", dup_keys == 0, f"{dup_keys:,} key value(s) appear more than once")

            # A row is unchanged if its key maps to the same hash of every column it had
            # before. Hashing keeps the comparison to two small columns per row.
            old_columns = before.columns
            row_hash = F.xxhash64(*[F.col(c) for c in old_columns]).alias("_row_hash")
            changed = (
                before.select(*key, row_hash).subtract(now.select(*key, row_hash)).count()
            )
            check("preserved", changed == 0,
                  f"{changed:,} of {n_before:,} existing rows missing or changed")

            new_columns = [c for c in now.columns if c not in old_columns]
            declared = [c["column"] for c in info["schema_changes"]]
            check("new columns", sorted(new_columns) == sorted(declared),
                  f"table gained {new_columns or 'none'}, manifest declares {declared or 'none'}")
            old_rows = now.join(before.select(*key), key, "left_semi")
            new_rows = now.join(before.select(*key), key, "left_anti")
            for change in info["schema_changes"]:
                column = change["column"]
                if column not in now.columns:
                    continue
                lo, hi = change["valid_range"]
                filled_old = old_rows.filter(F.col(column).isNotNull()).count()
                check(f"{column} on existing rows", filled_old == 0,
                      f"{filled_old:,} existing row(s) have a value; all should be null")
                stats = new_rows.agg(
                    F.count(F.lit(1)).alias("n"), F.count(column).alias("set"),
                    F.min(column).alias("lo"), F.max(column).alias("hi"),
                ).first()
                in_range = stats["set"] == stats["n"] and (
                    stats["n"] == 0 or (lo <= stats["lo"] and stats["hi"] <= hi)
                )
                check(f"{column} on new rows", in_range,
                      f"{stats['set']:,} of {stats['n']:,} set, values {stats['lo']} to "
                      f"{stats['hi']}, allowed {lo} to {hi}")

            expected = info["expected"]
            duplicates = first["source_rows"] - first["inserted"]
            check("expected inserted", first["inserted"] == expected["inserted"],
                  f"first merge inserted {first['inserted']:,}, manifest expects {expected['inserted']:,}")
            check("expected duplicates", duplicates == expected["duplicates_ignored"],
                  f"first merge ignored {duplicates:,}, "
                  f"manifest expects {expected['duplicates_ignored']:,}")

            # Rejections and re-runs are only visible in the run records: a re-run that
            # inserts nothing leaves no commit in the table history.
            rejected, runs = "n/a", []
            if metadata is not None:
                runs = metadata.filter(F.col("dataset_name") == name).orderBy("started_at").collect()
                if baseline_at is not None:
                    runs = [r for r in runs if r["started_at"] >= baseline_at]
            if runs:
                rejected = runs[0]["records_rejected"]
                check("expected rejected", rejected == expected["rejected_by_quality_rules"],
                      f"first run rejected {rejected:,}, "
                      f"manifest expects {expected['rejected_by_quality_rules']:,}")
            if len(runs) > 1:
                reinserted = sum(r["records_inserted"] or 0 for r in runs[1:])
                check("idempotent", reinserted == 0 and len(merges) == 1,
                      f"{len(runs) - 1} later run(s) of {release} inserted {reinserted:,} rows")

            facts[name] = {
                "before": n_before, "after": n_now, "inserted": inserted_total,
                "duplicates": duplicates, "rejected": f"{rejected:,}" if isinstance(rejected, int) else rejected,
                "new_columns": new_columns, "runs": len(runs),
            }
    finally:
        spark.stop()

    report = os.path.join(REPO_ROOT, "reports", "updates", f"{release}_verification.md")
    write_report(release, check, facts, report)
    print(f"\n{check.passed} passed, {check.failed} failed. Wrote {os.path.relpath(report, REPO_ROOT)}")
    return 1 if check.failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
