"""Configuration objects loaded from conf/platform.yaml and conf/datasets/*.yaml.

Adding a dataset to the platform means adding one YAML file here. Nothing in this
module, or in any other module except ``transforms/``, knows the name of any dataset.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from typing import Any

import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _abs(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(REPO_ROOT, path)


@dataclass(frozen=True)
class TimestampSpec:
    """How to build one canonical UTC timestamp column from the source.

    Exactly one of ``from_column``, ``from_parts`` or ``from_concat`` is set. The three
    forms exist because the three sources encode time three different ways: one
    timestamp column, four integer columns, and a date column plus a time column.

    ``tz`` is the timezone the *source values* are expressed in. ``UTC`` means the
    values are already absolute and are used unchanged; anything else is converted.
    """

    target: str
    tz: str = "UTC"
    from_column: str | None = None
    from_parts: dict[str, str] | None = None
    from_concat: list[str] | None = None
    format: str | None = None

    @staticmethod
    def parse(target: str, raw: dict[str, Any]) -> TimestampSpec:
        spec = TimestampSpec(
            target=target,
            tz=raw.get("tz", "UTC"),
            from_column=raw.get("from"),
            from_parts=raw.get("from_parts"),
            from_concat=raw.get("from_concat"),
            format=raw.get("format"),
        )
        sources = [spec.from_column, spec.from_parts, spec.from_concat]
        if sum(s is not None for s in sources) != 1:
            raise ValueError(
                f"timestamp '{target}': set exactly one of from / from_parts / from_concat"
            )
        return spec


@dataclass(frozen=True)
class QualityRule:
    """One data-quality check.

    ``on_fail`` decides what happens to a row that fails:
      reject  the row is removed and written to _meta/quarantine with this rule's name
      flag    the row is kept and a boolean column ``dq_<name>`` records the failure

    Flagging exists because not every anomaly is an error. 2.26% of trips report zero
    distance, which is implausible but not obviously invalid, so it is flagged rather
    than discarded.
    """

    type: str
    name: str
    on_fail: str = "reject"
    params: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def parse(raw: dict[str, Any]) -> QualityRule:
        raw = dict(raw)
        rule_type = raw.pop("type")
        on_fail = raw.pop("on_fail", "reject")
        name = raw.pop("name", None) or _default_rule_name(rule_type, raw)
        if on_fail not in ("reject", "flag"):
            raise ValueError(f"rule '{name}': on_fail must be 'reject' or 'flag'")
        return QualityRule(type=rule_type, name=name, on_fail=on_fail, params=raw)


def _default_rule_name(rule_type: str, params: dict[str, Any]) -> str:
    target = params.get("column") or "_".join(params.get("columns", [])) or "all"
    return f"{rule_type}__{target}"


@dataclass(frozen=True)
class SchemaPolicy:
    """What the platform does when the source layout stops matching the config. Each setting is ``allow``, ``flag`` or ``fail``. A failure is isolated to the dataset that caused it.
    The defaults are deliberately permissive, because a new column is the normal way these feeds evolve.
    """

    on_new_column: str = "allow"
    on_missing_column: str = "flag"
    on_type_change: str = "flag"
    # New columns named here are always allowed, whatever on_new_column says.
    allowed_new_columns: list[str] = field(default_factory=list)

    @staticmethod
    def parse(raw: dict[str, Any] | None) -> SchemaPolicy:
        raw = raw or {}
        policy = SchemaPolicy(
            on_new_column=raw.get("on_new_column", "allow"),
            on_missing_column=raw.get("on_missing_column", "flag"),
            on_type_change=raw.get("on_type_change", "flag"),
            allowed_new_columns=list(raw.get("allowed_new_columns", []) or []),
        )
        for setting in (policy.on_new_column, policy.on_missing_column, policy.on_type_change):
            if setting not in ("allow", "flag", "fail"):
                raise ValueError(
                    f"schema_policy: '{setting}' is not one of allow / flag / fail"
                )
        return policy


@dataclass(frozen=True)
class SourceSpec:
    format: str
    path: str
    options: dict[str, Any] = field(default_factory=dict)
    # Pushed into the scan, not applied afterwards. air_quality is 2.2 GB of which
    # 0.64% is in scope, so this predicate is the difference between reading the
    # file and reading the data.
    filter: str | None = None

    @property
    def abs_path(self) -> str:
        return _abs(self.path)


@dataclass(frozen=True)
class TargetSpec:
    table: str
    layer: str = "silver"
    # overwrite, append, or merge. merge is set only by DatasetConfig.for_release.
    mode: str = "overwrite"
    partition_by: list[str] = field(default_factory=list)
    zorder_by: list[str] = field(default_factory=list)
    replace_where: str | None = None
    merge_key: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class IncrementalSpec:
    """Where a dataset's incremental releases arrive, and what identifies a row.

    ``path`` contains ``{release}``, filled in from the command line. ``merge_key`` is
    at the grain of the *target* table, which is not always the grain of the source:
    air_quality's source rows are individual monitor readings, but its silver table
    holds one row per hour.
    """

    path: str
    merge_key: list[str]


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    source: SourceSpec
    target: TargetSpec
    description: str = ""
    expected_columns: dict[str, str] = field(default_factory=dict)
    required_columns: list[str] = field(default_factory=list)
    rename: dict[str, str] = field(default_factory=dict)
    drop: list[str] = field(default_factory=list)
    timestamps: list[TimestampSpec] = field(default_factory=list)
    cast: dict[str, str] = field(default_factory=dict)
    primary_key: list[str] = field(default_factory=list)
    rules: list[QualityRule] = field(default_factory=list)
    schema_policy: SchemaPolicy = field(default_factory=SchemaPolicy)
    incremental: IncrementalSpec | None = None

    def for_release(self, release: str) -> DatasetConfig:
        """This dataset, read from one incremental release and merged into its table.

        Everything else (schema checks, standardisation, transforms, quality rules) is
        unchanged, so an update goes through exactly the pipeline the initial load did.
        """
        if self.incremental is None:
            raise ValueError(f"[{self.name}] has no incremental section")
        return replace(
            self,
            source=replace(self.source, path=self.incremental.path.format(release=release)),
            target=replace(self.target, mode="merge", merge_key=self.incremental.merge_key),
        )

    @staticmethod
    def load(path: str) -> DatasetConfig:
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        quality = raw.get("quality", {}) or {}
        incremental = raw.get("incremental")
        return DatasetConfig(
            name=raw["name"],
            description=raw.get("description", ""),
            source=SourceSpec(**raw["source"]),
            target=TargetSpec(**raw["target"]),
            expected_columns=raw.get("expected_columns", {}) or {},
            required_columns=raw.get("required_columns", []) or [],
            rename=raw.get("rename", {}) or {},
            drop=raw.get("drop", []) or [],
            timestamps=[
                TimestampSpec.parse(target, spec)
                for target, spec in (raw.get("timestamps", {}) or {}).items()
            ],
            cast=raw.get("cast", {}) or {},
            schema_policy=SchemaPolicy.parse(raw.get("schema_policy")),
            primary_key=quality.get("primary_key", []) or [],
            rules=[QualityRule.parse(r) for r in (quality.get("rules", []) or [])],
            incremental=(
                IncrementalSpec(path=incremental["path"], merge_key=list(incremental["merge_key"]))
                if incremental
                else None
            ),
        )


@dataclass(frozen=True)
class PlatformConfig:
    lakehouse_root: str
    raw_root: str
    layers: dict[str, str]
    spark: dict[str, Any]
    # Both default to on. They are switched off only to measure what they cost, in the validation and monitoring overhead experiments.
    validation: dict[str, Any] = field(default_factory=dict)
    monitoring: dict[str, Any] = field(default_factory=dict)

    @property
    def validation_enabled(self) -> bool:
        return bool(self.validation.get("enabled", True))

    @property
    def validation_metrics_enabled(self) -> bool:
        return self.validation_enabled and bool(self.validation.get("metrics", True))

    @property
    def monitoring_enabled(self) -> bool:
        return bool(self.monitoring.get("enabled", True))

    @staticmethod
    def load(path: str | None = None) -> PlatformConfig:
        path = path or os.path.join(REPO_ROOT, "conf", "platform.yaml")
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        return PlatformConfig(
            lakehouse_root=_abs(raw["lakehouse_root"]),
            raw_root=_abs(raw["raw_root"]),
            layers=raw["layers"],
            spark=raw.get("spark", {}),
            validation=raw.get("validation", {}) or {},
            monitoring=raw.get("monitoring", {}) or {},
        )

    def table_path(self, layer: str, table: str) -> str:
        return os.path.join(self.lakehouse_root, self.layers[layer], table)

    def meta_path(self, table: str) -> str:
        return os.path.join(self.lakehouse_root, self.layers["meta"], table)


def load_datasets(names: list[str] | None = None) -> list[DatasetConfig]:
    """Load dataset configs by name, or all of them when ``names`` is None."""
    conf_dir = os.path.join(REPO_ROOT, "conf", "datasets")
    available = {
        os.path.splitext(f)[0]: os.path.join(conf_dir, f)
        for f in sorted(os.listdir(conf_dir))
        if f.endswith((".yaml", ".yml"))
    }
    if names:
        missing = [n for n in names if n not in available]
        if missing:
            raise SystemExit(
                f"unknown dataset(s): {', '.join(missing)}. "
                f"available: {', '.join(available)}"
            )
        return [DatasetConfig.load(available[n]) for n in names]
    return [DatasetConfig.load(p) for p in available.values()]
