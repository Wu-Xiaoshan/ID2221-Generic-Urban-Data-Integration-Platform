"""Week 4 Task 4: time Approach A against Approach B and check that both give the same model.

Approach A is scripts/approach_a_from_raw.py: raw files to model in one process.
Approach B is the platform, measured in two parts:

    platform   run_ingest.py, run_update.py v2, run_integrate.py -- raw files to the gold table
    ml         run_ml_train.py --rebuild                          -- gold to model (Tasks 1 and 3)

B runs in a lakehouse of its own under data/approach_b/, rebuilt from nothing on every repeat,
so data/lakehouse is never touched. The platform writes its ML reports to reports/ml/; they
are restored when the harness ends.

Each approach runs three times, interleaved, the order reversed on the middle repeat, after one
discarded warm-up run of A that brings the raw files into the file cache. Afterwards A is
checked against the platform with scripts/compare_approaches.py, once on the full data and
once with --without-update against the model trained before release v2 (dataset version 3,
model version 1 of data/lakehouse).

Last, a layout experiment on the gold table of the final repeat: run_ml_train.py --rebuild
three more times as it is, and three times with Delta's optimizeWrite and autoCompact off for
that one job, interleaved the same way. Both start without an ml layer. The platform turns
optimizeWrite on for the analytical tables, and it writes the whole training split as one file
of two row groups, so Spark reads it with two tasks on a four-core machine.

    python scripts/run_comparison.py            # about 2.5 hours
    python scripts/run_comparison.py --resume   # continue after an interruption

Writes reports/comparison/timings.md and runs.json, and the per-run results next to them.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(REPO, "reports", "comparison")
LOGS = os.path.join(REPO, "data", "approach_a", "logs")
B_ROOT = os.path.join(REPO, "data", "approach_b")
RUNS_FILE = os.path.join(OUT, "runs.json")
REPEATS = 3

B_STEPS = [
    ("b_ingest", ["jobs/run_ingest.py"]),
    ("b_update", ["jobs/run_update.py", "v2"]),
    ("b_integrate", ["jobs/run_integrate.py"]),
    ("b_ml", ["jobs/run_ml_train.py", "--rebuild"]),
]

# The platform's ML job as configured, against the same job writing its dataset without
# Delta's file compaction.
LAYOUT = {
    "b_ml_same_gold": {},
    "b_ml_no_optimize_write": {"spark.databricks.delta.optimizeWrite.enabled": "false",
                               "spark.databricks.delta.autoCompact.enabled": "false"},
}


def keep_awake() -> None:
    """Keep Windows out of Modern Standby, which throttles the jobs without an error."""
    if os.name == "nt":
        import ctypes

        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001 | 0x00000002)


def remove_tree(path: str) -> None:
    """shutil.rmtree, with the long-path prefix on Windows: a saved Spark pipeline nests deeper
    than the 260 characters Windows allows by default."""
    if os.path.isdir(path):
        shutil.rmtree("\\\\?\\" + os.path.abspath(path) if os.name == "nt" else path)


def b_config(spark_conf: dict | None = None) -> str:
    """conf/platform.yaml with the lakehouse moved to data/approach_b/ and spark_conf merged in."""
    with open(os.path.join(REPO, "conf", "platform.yaml"), encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    raw["lakehouse_root"] = B_ROOT
    raw["spark"].setdefault("config", {}).update(spark_conf or {})
    fd, path = tempfile.mkstemp(suffix=".yaml", prefix="platform-comparison-")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        yaml.safe_dump(raw, fh, sort_keys=False)
    return path


def load_runs() -> list[dict]:
    if os.path.exists(RUNS_FILE):
        with open(RUNS_FILE, encoding="utf-8") as fh:
            return json.load(fh)
    return []


def save_runs(runs: list[dict]) -> None:
    with open(RUNS_FILE, "w", encoding="utf-8") as fh:
        json.dump(runs, fh, indent=1)


def run(name: str, rep: str, command: list[str], env: dict | None = None) -> dict:
    os.makedirs(LOGS, exist_ok=True)
    started_at = dt.datetime.now().isoformat(timespec="seconds")
    t = time.perf_counter()
    with open(os.path.join(LOGS, f"{name}.{rep}.log"), "w", encoding="utf-8") as log:
        rc = subprocess.run([sys.executable, *command], cwd=REPO, env=env or os.environ.copy(),
                            stdout=log, stderr=subprocess.STDOUT).returncode
    wall = round(time.perf_counter() - t, 1)
    print(f"{started_at}  {name:<12} rep {rep}  {wall:>7.1f} s  exit {rc}", flush=True)
    return {"name": name, "rep": rep, "started_at": started_at, "wall_sec": wall, "exit": rc}


def run_a(rep: str) -> list[dict]:
    report = os.path.join(OUT, f"approach_a.{rep}.json")
    return [run("a", rep, ["scripts/approach_a_from_raw.py", "--report", report])]


def run_b(rep: str) -> list[dict]:
    remove_tree(B_ROOT)
    config = b_config()
    env = {**os.environ, "URBANPLATFORM_CONFIG": config}
    out = []
    try:
        for name, command in B_STEPS:
            out.append(run(name, rep, command, env))
            if out[-1]["exit"]:
                break
        card = os.path.join(B_ROOT, "ml", "models", "trip_duration_model", "v1", "model.json")
        if os.path.exists(card):
            shutil.copy2(card, os.path.join(OUT, f"approach_b_model.{rep}.json"))
    finally:
        os.remove(config)
    return out


def run_layout(name: str, rep: str) -> list[dict]:
    """The ML job on the gold table already in data/approach_b/, starting without an ml layer."""
    if not os.path.isdir(os.path.join(B_ROOT, "gold")):
        raise SystemExit("data/approach_b/ has no gold table; the layout runs follow the main repeats")
    remove_tree(os.path.join(B_ROOT, "ml"))
    config = b_config(LAYOUT[name])
    try:
        out = [run(name, rep, ["jobs/run_ml_train.py", "--rebuild"], {**os.environ, "URBANPLATFORM_CONFIG": config})]
        card = os.path.join(B_ROOT, "ml", "models", "trip_duration_model", "v1", "model.json")
        if os.path.exists(card):
            shutil.copy2(card, os.path.join(OUT, f"approach_b_model_{name}.{rep}.json"))
        dataset = os.path.join(B_ROOT, "ml", "trip_duration_dataset", "split=train")
        out[0]["train_files"] = len([f for f in os.listdir(dataset) if f.endswith(".parquet")])
    finally:
        os.remove(config)
    return out


def summarise(runs: list[dict]) -> str:
    timed = [r for r in runs if r["rep"] != "warmup" and r["name"] in {"a", *(n for n, _ in B_STEPS)}]
    by_name: dict[str, list[float]] = {}
    for r in timed:
        by_name.setdefault(r["name"], []).append(r["wall_sec"])
    med = {n: statistics.median(v) for n, v in by_name.items()}

    def stage(prefix: str, key: str) -> list[float]:
        vals = []
        for rep in map(str, range(1, REPEATS + 1)):
            path = os.path.join(OUT, f"{prefix}.{rep}.json")
            if os.path.exists(path):
                with open(path, encoding="utf-8") as fh:
                    data = json.load(fh)
                vals.append(float((data.get("timings") or data)[key]))
        return vals

    lines = [
        "# Approach A against Approach B: timings",
        "",
        f"Generated {dt.datetime.now():%Y-%m-%d %H:%M}. Wall-clock seconds per process, including JVM start-up; "
        f"median of {REPEATS} interleaved repeats after one discarded warm-up run. "
        "B runs in its own lakehouse, rebuilt from nothing on every repeat.",
        "",
        "| Run | What it does | Median | Samples |",
        "|---|---|---:|---|",
    ]
    what = {"a": "A: raw files to evaluated model", "b_ingest": "B: ingest the original files to silver",
            "b_update": "B: merge release v2 into silver", "b_integrate": "B: build the gold table",
            "b_ml": "B: gold to evaluated model (`run_ml_train.py --rebuild`)"}
    for name in ["a", *(n for n, _ in B_STEPS)]:
        if name in by_name:
            lines.append(f"| `{name}` | {what[name]} | {med[name]:.1f} s | "
                         f"{', '.join(f'{v:.1f}' for v in by_name[name])} |")
    platform = sum(med.get(n, 0) for n in ("b_ingest", "b_update", "b_integrate"))
    lines += ["", f"B platform part (ingest + update + integrate): {platform:.1f} s. "
                  f"B end to end: {platform + med.get('b_ml', 0):.1f} s. A end to end: {med.get('a', 0):.1f} s.", ""]

    lines += ["## Stages inside the model runs", "",
              "Seconds measured by the scripts themselves, median of the repeats.", "",
              "| Stage | A | B |", "|---|---:|---:|"]
    a_stage = {k: stage("approach_a", k) for k in ("dataset_sec", "fit_sec", "evaluate_sec")}
    b_stage = {k: stage("approach_b_model", k) for k in ("fit_sec", "evaluate_sec")}

    def m(v: list[float]) -> str:
        return f"{statistics.median(v):.1f}" if v else "-"

    lines += [f"| training dataset | {m(a_stage['dataset_sec'])} | see the dataset report |",
              f"| fit (features and regression) | {m(a_stage['fit_sec'])} | {m(b_stage['fit_sec'])} |",
              f"| scoring and evaluation | {m(a_stage['evaluate_sec'])} | {m(b_stage['evaluate_sec'])} |", ""]

    layout = [r for r in runs if r["name"] in LAYOUT]
    if layout:
        lines += ["## Layout of the training split", "",
                  "`run_ml_train.py --rebuild` on the gold table of the last repeat, starting without an ml layer.", "",
                  "| Run | Delta file compaction | Files in split=train | Job median | Samples | Fit | Scoring |",
                  "|---|---|---:|---:|---|---:|---:|"]
        for name, conf in LAYOUT.items():
            walls = [r["wall_sec"] for r in layout if r["name"] == name]
            files = sorted({r.get("train_files") for r in layout if r["name"] == name} - {None})
            if walls:
                lines.append(f"| `{name}` | {'off' if conf else 'on (platform default)'} | "
                             f"{', '.join(map(str, files))} | {statistics.median(walls):.1f} s | "
                             f"{', '.join(f'{v:.1f}' for v in walls)} | "
                             f"{m(stage('approach_b_model_' + name, 'fit_sec'))} | "
                             f"{m(stage('approach_b_model_' + name, 'evaluate_sec'))} |")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--resume", action="store_true", help="skip runs already recorded in runs.json")
    args = parser.parse_args()

    keep_awake()
    # Spark failed twice at start-up on this laptop when the driver bound to the Wi-Fi address.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.makedirs(OUT, exist_ok=True)
    runs = load_runs() if args.resume else []
    done = {(r["name"], r["rep"]) for r in runs if r["exit"] == 0}

    ml_reports = os.path.join(REPO, "reports", "ml")
    saved = {}
    for f in os.listdir(ml_reports):
        with open(os.path.join(ml_reports, f), encoding="utf-8") as fh:
            saved[f] = fh.read()

    plan = [("a", "warmup")]
    for rep in range(1, REPEATS + 1):
        order = ["b", "a"] if rep == 2 else ["a", "b"]
        plan += [(x, str(rep)) for x in order]
    try:
        for which, rep in plan:
            if (which if which == "a" else "b_ml", rep) in done:
                continue
            new = run_a(rep) if which == "a" else run_b(rep)
            runs = [r for r in runs if not (r["rep"] == rep and r["name"] in {x["name"] for x in new})] + new
            save_runs(runs)

        checks = [
            ("compare", ["scripts/compare_approaches.py", "--report", os.path.join(OUT, f"approach_a.{REPEATS}.json")]),
            ("a_without_update", ["scripts/approach_a_from_raw.py", "--without-update",
                                  "--report", os.path.join(OUT, "approach_a_without_update.json")]),
            ("compare_without_update", ["scripts/compare_approaches.py", "--dataset-version", "3",
                                        "--model-version", "1",
                                        "--report", os.path.join(OUT, "approach_a_without_update.json")]),
        ]
        for name, command in checks:
            if (name, "check") in done:
                continue
            runs.append(run(name, "check", command))
            save_runs(runs)

        first, second = list(LAYOUT)
        for rep in range(1, REPEATS + 1):
            for name in ([second, first] if rep == 2 else [first, second]):
                if (name, str(rep)) in done:
                    continue
                new = run_layout(name, str(rep))
                runs = [r for r in runs if not (r["rep"] == str(rep) and r["name"] == name)] + new
                save_runs(runs)
    finally:
        for f, text in saved.items():
            with open(os.path.join(ml_reports, f), "w", encoding="utf-8") as fh:
                fh.write(text)

    with open(os.path.join(OUT, "timings.md"), "w", encoding="utf-8") as fh:
        fh.write(summarise(runs))
    failed = [r for r in runs if r["exit"]]
    print(f"\n{len(runs)} runs, {len(failed)} failed; written {os.path.relpath(os.path.join(OUT, 'timings.md'), REPO)}")
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        import traceback

        os.makedirs(LOGS, exist_ok=True)
        with open(os.path.join(LOGS, "harness_error.log"), "a", encoding="utf-8") as fh:
            print(dt.datetime.now().isoformat(timespec="seconds"), traceback.format_exc(), sep="\n", file=fh)
        raise
