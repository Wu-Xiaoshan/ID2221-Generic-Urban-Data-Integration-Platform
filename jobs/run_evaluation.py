"""Week 3 Task 5: measure the production readiness of the platform.

    python jobs/run_evaluation.py --prepare        # build the snapshots every experiment starts from
    python jobs/run_evaluation.py                  # run the experiments, three repeats each
    python jobs/run_evaluation.py --storage        # measure storage only; runs no pipeline
    python jobs/run_evaluation.py --only update refresh --repeats 5
    python jobs/run_evaluation.py --resume         # continue an interrupted evaluation

An update changes the data it runs on, so repeating one is not repeating a measurement: the second
run of a release finds every record already present and inserts nothing. Every run therefore starts
from a copy of a snapshot. Snapshot A is the initial load (ingest, integrate, products); B is A plus
release v2 merged into silver; C is B after the refresh. The copy restores silver, gold and the
products and leaves _meta alone, so the monitoring history accumulates across runs. Nothing that
decides what to refresh reads _meta; it reads Delta table properties, which the copy does restore.

Validation and monitoring are switched off through a temporary copy of conf/platform.yaml passed in
URBANPLATFORM_CONFIG, so the committed file is never edited and a crash cannot leave it switched off.

Each run is a separate process, as a scheduled job would be, and its wall-clock time includes starting
the JVM. Runs are interleaved and the order is reversed on every other repeat, so a cache that warms
over the session does not favour one variant. Results go to reports/evaluation/.
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

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from urbanplatform.config import REPO_ROOT, PlatformConfig  # noqa: E402

RELEASE = "v2"
REPORT_DIR = os.path.join(REPO_ROOT, "reports", "evaluation")
SNAPSHOT_DIR = os.path.join(REPO_ROOT, "data", "snapshots")
LOG_DIR = os.path.join(SNAPSHOT_DIR, "logs")
RESTORED = ("silver", "gold", "products")

VALIDATION_OFF = {"validation": {"enabled": False}}
METRICS_OFF = {"validation": {"metrics": False}}
MONITORING_OFF = {"monitoring": {"enabled": False}}

# name -> (snapshot, command, config overrides). A run shared by several comparisons is run once.
RUNS: dict[str, tuple[str, list[str], dict]] = {
    "update":               ("A", ["jobs/run_update.py", RELEASE], {}),
    "update_no_metrics":    ("A", ["jobs/run_update.py", RELEASE], METRICS_OFF),
    "update_no_validation": ("A", ["jobs/run_update.py", RELEASE], VALIDATION_OFF),
    "update_no_monitoring": ("A", ["jobs/run_update.py", RELEASE], MONITORING_OFF),
    "ingest":               ("A", ["jobs/run_ingest.py"], {}),
    "ingest_no_metrics":    ("A", ["jobs/run_ingest.py"], METRICS_OFF),
    "ingest_no_validation": ("A", ["jobs/run_ingest.py"], VALIDATION_OFF),
    "refresh":              ("B", ["jobs/run_refresh.py"], {}),
    "refresh_full":         ("B", ["jobs/run_refresh.py", "--full"], {}),
    "refresh_no_monitoring": ("B", ["jobs/run_refresh.py"], MONITORING_OFF),
    # The same release again, and a refresh with nothing to do: the fixed cost of a run.
    "update_repeat":        ("B", ["jobs/run_update.py", RELEASE], {}),
    "refresh_noop":         ("C", ["jobs/run_refresh.py"], {}),
}

# What each comparison sets against what. The first run is the reference.
COMPARISONS = [
    ("Incremental update against a full re-ingest", ["ingest", "update"]),
    ("Analytical refresh: incremental against full", ["refresh_full", "refresh"]),
    ("Validation overhead on the v2 update", ["update", "update_no_metrics", "update_no_validation"]),
    ("Validation overhead on the full load", ["ingest", "ingest_no_metrics", "ingest_no_validation"]),
    ("Monitoring overhead on the v2 update", ["update", "update_no_monitoring"]),
    ("Monitoring overhead on the refresh", ["refresh", "refresh_no_monitoring"]),
    ("An update with new data against one with none", ["update", "update_repeat"]),
    ("A refresh with changes against one with none", ["refresh", "refresh_noop"]),
]


# ---------------------------------------------------------------- snapshots


def _lakehouse(platform: PlatformConfig) -> str:
    return platform.lakehouse_root


def take_snapshot(platform: PlatformConfig, name: str) -> None:
    dest = os.path.join(SNAPSHOT_DIR, name)
    if os.path.isdir(dest):
        shutil.rmtree(dest)
    os.makedirs(dest)
    for layer in RESTORED:
        src = os.path.join(_lakehouse(platform), layer)
        if os.path.isdir(src):
            shutil.copytree(src, os.path.join(dest, layer))
    print(f"snapshot {name}: {_du(dest) / 1024 ** 2:,.0f} MB")


def restore_snapshot(platform: PlatformConfig, name: str) -> None:
    src = os.path.join(SNAPSHOT_DIR, name)
    if not os.path.isdir(src):
        raise SystemExit(f"snapshot {name} missing -- run jobs/run_evaluation.py --prepare first")
    for layer in RESTORED:
        live = os.path.join(_lakehouse(platform), layer)
        if os.path.isdir(live):
            shutil.rmtree(live)
        if os.path.isdir(os.path.join(src, layer)):
            shutil.copytree(os.path.join(src, layer), live)


def _du(path: str) -> int:
    return sum(os.path.getsize(os.path.join(dp, f)) for dp, _, fs in os.walk(path) for f in fs)


# ---------------------------------------------------------------- running


def _config_file(overrides: dict) -> str | None:
    """A copy of conf/platform.yaml with the overrides merged in, or None for the committed file."""
    if not overrides:
        return None
    with open(os.path.join(REPO_ROOT, "conf", "platform.yaml"), encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    for section, values in overrides.items():
        raw.setdefault(section, {}).update(values)
    fd, path = tempfile.mkstemp(suffix=".yaml", prefix="platform-eval-")
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        yaml.safe_dump(raw, fh, sort_keys=False)
    return path


def keep_awake() -> None:
    """Keep Windows out of Modern Standby while the evaluation runs.

    A laptop that turns its screen off enters Modern Standby and throttles desktop processes.
    On the first attempt that stretched 200 s runs to 2,600 s, with no error to show for it.
    The request lasts as long as this process and Windows drops it when the process exits.
    """
    if os.name != "nt":
        return
    import ctypes

    es_continuous, es_system_required, es_display_required = 0x80000000, 0x00000001, 0x00000002
    ctypes.windll.kernel32.SetThreadExecutionState(
        es_continuous | es_system_required | es_display_required)


def run_once(platform: PlatformConfig, name: str, rep: int) -> dict:
    snapshot, command, overrides = RUNS[name]
    restore_snapshot(platform, snapshot)

    env = dict(os.environ)
    config = _config_file(overrides)
    if config:
        env["URBANPLATFORM_CONFIG"] = config

    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"{name}.{rep}.log")
    started_at = dt.datetime.now().isoformat(timespec="seconds")
    started = time.perf_counter()
    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.run([sys.executable, *command], cwd=REPO_ROOT, env=env,
                              stdout=log, stderr=subprocess.STDOUT)
    wall = time.perf_counter() - started
    if config:
        os.remove(config)

    print(f"  {name:<24} rep {rep}  {wall:8.2f} s  exit {proc.returncode}", flush=True)
    return {"run": name, "rep": rep, "started_at": started_at, "snapshot": snapshot,
            "command": command,
            "overrides": overrides, "wall_sec": round(wall, 3),
            "exit_code": proc.returncode, "log": os.path.relpath(log_path, REPO_ROOT)}


RUNS_FILE = os.path.join(REPORT_DIR, "runs.json")


def _load_done() -> list[dict]:
    if not os.path.exists(RUNS_FILE):
        return []
    with open(RUNS_FILE, encoding="utf-8") as fh:
        return json.load(fh)


def _save(results: list[dict]) -> None:
    os.makedirs(REPORT_DIR, exist_ok=True)
    tmp = RUNS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(results, fh, indent=1)
    os.replace(tmp, RUNS_FILE)


def run_experiments(platform: PlatformConfig, names: list[str], repeats: int,
                    resume: bool) -> list[dict]:
    """Run every (run, repeat) pair, saving after each one.

    A full evaluation takes over an hour, so results are written to runs.json as they come in
    and --resume skips the pairs already there. Only successful runs count as done.
    """
    results = _load_done() if resume else []
    done = {(r["run"], r["rep"]) for r in results if r["exit_code"] == 0}
    results = [r for r in results if (r["run"], r["rep"]) in done]
    for rep in range(1, repeats + 1):
        order = names if rep % 2 else list(reversed(names))
        todo = [n for n in order if (n, rep) not in done]
        if not todo:
            continue
        print(f"repeat {rep}/{repeats}: {', '.join(todo)}", flush=True)
        for name in todo:
            results.append(run_once(platform, name, rep))
            _save(results)
    return results


# ---------------------------------------------------------------- storage


def _delta_tables(root: str):
    for dirpath, dirnames, _ in os.walk(root):
        if "_delta_log" in dirnames:
            yield dirpath
            dirnames[:] = []


def table_storage(path: str) -> dict:
    """Live bytes from the Delta log, on-disk bytes, change-feed bytes and version count.

    Live size replays the log's add and remove actions, so it needs no Spark and is the size a
    reader of the current version sees. The rest of the directory is superseded versions and the
    change data feed, which Delta keeps until VACUUM.
    """
    files: dict[str, int] = {}
    logs = sorted(f for f in os.listdir(os.path.join(path, "_delta_log")) if f.endswith(".json"))
    for name in logs:
        with open(os.path.join(path, "_delta_log", name), encoding="utf-8") as fh:
            for line in fh:
                action = json.loads(line)
                if "add" in action:
                    files[action["add"]["path"]] = action["add"]["size"]
                elif "remove" in action:
                    files.pop(action["remove"]["path"], None)
    cdf_dir = os.path.join(path, "_change_data")
    log_dir = os.path.join(path, "_delta_log")
    return {
        "live": sum(files.values()),
        "disk": _du(path),
        "change_data": _du(cdf_dir) if os.path.isdir(cdf_dir) else 0,
        "log": _du(log_dir),
        "versions": len(logs),
    }


def storage_report(root: str) -> dict[str, dict]:
    return {os.path.relpath(t, root).replace(os.sep, "/"): table_storage(t)
            for t in sorted(_delta_tables(root))}


# ---------------------------------------------------------------- reporting


def _mb(n: int) -> str:
    return f"{n / 1024 ** 2:,.2f} MB"


def render(results: list[dict], storage: dict[str, dict[str, dict]], repeats: int) -> str:
    out: list[str] = []
    add = out.append
    add("# Platform evaluation measurements")
    add("")
    add(f"Generated {dt.datetime.now():%Y-%m-%d %H:%M}. Wall-clock seconds per process, including "
        f"JVM start-up; median of {repeats} interleaved repeats, order reversed on every other "
        "repeat. Every run starts from a restored snapshot.")
    add("")

    by_run: dict[str, list[dict]] = {}
    for r in results:
        by_run.setdefault(r["run"], []).append(r)
    med = {k: statistics.median(x["wall_sec"] for x in v) for k, v in by_run.items()}

    if results:
        add("## Runs")
        add("")
        add("| Run | Snapshot | Command | Overrides | Median | Samples | Exit codes |")
        add("|---|---|---|---|---:|---|---|")
        for name, (snap, cmd, ov) in RUNS.items():
            if name not in by_run:
                continue
            samples = ", ".join(f"{x['wall_sec']:.1f}" for x in sorted(by_run[name], key=lambda x: x["rep"]))
            codes = ",".join(str(x["exit_code"]) for x in sorted(by_run[name], key=lambda x: x["rep"]))
            add(f"| `{name}` | {snap} | `{' '.join(cmd)}` | `{json.dumps(ov) if ov else '-'}` "
                f"| {med[name]:.2f} s | {samples} | {codes} |")
        add("")

        add("## Comparisons")
        add("")
        for title, names in COMPARISONS:
            if not all(n in med for n in names):
                continue
            ref = med[names[0]]
            add(f"### {title}")
            add("")
            add("| Run | Median | Against the first | Share of the first |")
            add("|---|---:|---:|---:|")
            for n in names:
                add(f"| `{n}` | {med[n]:.2f} s | {med[n] - ref:+.2f} s | {100 * med[n] / ref:.1f}% |")
            add("")

    if storage:
        add("## Storage")
        add("")
        add("Live is what the current version of a table holds. Disk adds the superseded versions and "
            "the change data feed, which Delta keeps until VACUUM.")
        add("")
        for label, report in storage.items():
            add(f"### {label}")
            add("")
            add("| Table | Versions | Live | Change data | Log | On disk |")
            add("|---|---:|---:|---:|---:|---:|")
            tot = {"live": 0, "change_data": 0, "log": 0, "disk": 0}
            for table, s in report.items():
                add(f"| `{table}` | {s['versions']} | {_mb(s['live'])} | {_mb(s['change_data'])} "
                    f"| {_mb(s['log'])} | {_mb(s['disk'])} |")
                for k in tot:
                    tot[k] += s[k]
            add(f"| **total** | | {_mb(tot['live'])} | {_mb(tot['change_data'])} "
                f"| {_mb(tot['log'])} | {_mb(tot['disk'])} |")
            add("")
    return "\n".join(out)


# ---------------------------------------------------------------- main


def prepare(platform: PlatformConfig) -> None:
    """Rebuild from data/raw and take the three snapshots."""
    def step(*command: str) -> None:
        print(f"--- {' '.join(command)}")
        subprocess.run([sys.executable, *command], cwd=REPO_ROOT, check=True)

    # Start from empty tables. An overwrite keeps the files of every earlier version until
    # VACUUM, so rebuilding over an existing lakehouse would carry them into the snapshots:
    # the same rows, several times the size on disk, and a wrong storage measurement.
    for layer in RESTORED:
        live = os.path.join(_lakehouse(platform), layer)
        if os.path.isdir(live):
            shutil.rmtree(live)

    step("jobs/run_ingest.py")
    step("jobs/run_integrate.py")
    step("jobs/run_products.py")
    take_snapshot(platform, "A")
    step("jobs/run_update.py", RELEASE)
    take_snapshot(platform, "B")
    step("jobs/run_refresh.py")
    take_snapshot(platform, "C")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prepare", action="store_true", help="rebuild and take snapshots A, B and C")
    parser.add_argument("--storage", action="store_true", help="measure storage only")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--only", nargs="*", help="run only these runs by name")
    parser.add_argument("--resume", action="store_true",
                        help="keep reports/evaluation/runs.json and run only what is missing")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    platform = PlatformConfig.load()
    if not args.storage:
        keep_awake()

    if args.prepare:
        prepare(platform)
        return 0

    results: list[dict] = []
    if not args.storage:
        names = list(RUNS)
        if args.only:
            unknown = [n for n in args.only if n not in RUNS]
            if unknown:
                raise SystemExit(f"unknown run(s): {', '.join(unknown)}; available: {', '.join(RUNS)}")
            names = [n for n in names if n in args.only]
        results = run_experiments(platform, names, args.repeats, args.resume)
        restore_snapshot(platform, "C")  # leave the lakehouse in the post-refresh state

    storage = {}
    for label, snap in (("A: initial load", "A"), ("B: after the v2 update", "B"),
                        ("C: after the refresh", "C")):
        path = os.path.join(SNAPSHOT_DIR, snap)
        if os.path.isdir(path):
            storage[label] = storage_report(path)
    meta = os.path.join(platform.lakehouse_root, platform.layers["meta"])
    if os.path.isdir(meta):
        storage["_meta: monitoring, validation metrics and quarantine"] = storage_report(meta)

    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(os.path.join(REPORT_DIR, "measurements.md"), "w", encoding="utf-8") as fh:
        fh.write(render(results, storage, args.repeats) + "\n")
    print(f"written: {os.path.join(REPORT_DIR, 'measurements.md')}")

    failed = [r for r in results if r["exit_code"] != 0]
    if failed:
        print(f"WARNING: {len(failed)} run(s) failed; see their logs")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
