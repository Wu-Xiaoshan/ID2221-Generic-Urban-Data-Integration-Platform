# ML task configuration, loaded from conf/ml/<task>.yaml.

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
from dataclasses import dataclass, field, replace
from typing import Any

import yaml

from ..config import REPO_ROOT

SPLITS = ("train", "validation", "test")
FEATURE_KINDS = ("numeric", "categorical", "binary")


@dataclass(frozen=True)
class SplitSpec:
    strategy: str
    column: str | None = None
    validation_days: int = 14
    test_days: int = 14
    # Set both to pin a chronological split instead of counting back from the newest row.
    validation_start: str | None = None
    test_start: str | None = None
    fractions: dict[str, float] = field(default_factory=dict)
    seed: int = 42

    @staticmethod
    def parse(raw: dict[str, Any]) -> SplitSpec:
        spec = SplitSpec(
            strategy=raw["strategy"],
            column=raw.get("column"),
            validation_days=int(raw.get("validation_days", 14)),
            test_days=int(raw.get("test_days", 14)),
            validation_start=_iso(raw.get("validation_start")),
            test_start=_iso(raw.get("test_start")),
            fractions={k: float(v) for k, v in (raw.get("fractions") or {}).items()},
            seed=int(raw.get("seed", 42)),
        )
        if spec.strategy == "chronological":
            if not spec.column:
                raise ValueError("split: a chronological split needs `column`")
            if (spec.validation_start is None) != (spec.test_start is None):
                raise ValueError("split: set both validation_start and test_start, or neither")
        elif spec.strategy == "hash":
            if set(spec.fractions) != set(SPLITS) or abs(sum(spec.fractions.values()) - 1) > 1e-9:
                raise ValueError(f"split: hash fractions must cover {', '.join(SPLITS)} and sum to 1")
        else:
            raise ValueError(f"split: strategy must be 'chronological' or 'hash', not '{spec.strategy}'")
        return spec


def _iso(value: Any) -> str | None:
    # YAML reads an unquoted 2024-03-01 as a date; the SQL wants the ISO string.
    return None if value is None else str(value)


@dataclass(frozen=True)
class DatasetSpec:
    label: str
    filters: dict[str, str]
    columns: dict[str, str]  # gold column -> the dataset it came from
    split: SplitSpec
    output: str


@dataclass(frozen=True)
class FeatureSpec:
    name: str
    kind: str
    source: str
    log1p: bool = False
    missing_values: list[Any] = field(default_factory=list)
    # Numeric only: fill nulls with the training median of each group of this column
    # instead of one median for all rows.
    impute_by: str | None = None


@dataclass(frozen=True)
class GeneratorSpec:
    type: str
    params: dict[str, Any]


@dataclass(frozen=True)
class FeaturesSpec:
    generate: list[GeneratorSpec]
    features: list[FeatureSpec]
    impute: str = "median"
    missing_indicators: bool = True
    encoding: str = "onehot"
    scaling: str = "standard"
    keep: list[str] = field(default_factory=list)
    output: str = ""
    pipeline: str = ""

    def of_kind(self, kind: str) -> list[FeatureSpec]:
        return [f for f in self.features if f.kind == kind]

    @property
    def sources(self) -> list[str]:
        return list(dict.fromkeys(f.source for f in self.features))

    def without_sources(self, sources: set[str]) -> FeaturesSpec:
        """The same pipeline with every feature from ``sources`` left out.

        What the ablation in scripts/analyze_ml_features.py runs: a variant pipeline from the
        same configuration, with no code of its own.
        """
        return replace(self, features=[f for f in self.features if f.source not in sources])

    @staticmethod
    def parse(raw: dict[str, Any]) -> FeaturesSpec:
        features = []
        for kind in FEATURE_KINDS:
            for name, opts in (raw.get(kind) or {}).items():
                opts = opts or {}
                if "source" not in opts:
                    raise ValueError(f"feature '{name}': state its `source` dataset")
                features.append(
                    FeatureSpec(
                        name=name,
                        kind=kind,
                        source=opts["source"],
                        log1p=bool(opts.get("log1p", False)),
                        missing_values=list(opts.get("missing_values") or []),
                        impute_by=opts.get("impute_by"),
                    )
                )
        grouped = [f.name for f in features if f.impute_by and f.kind != "numeric"]
        if grouped:
            raise ValueError(f"impute_by applies to numeric features only: {', '.join(grouped)}")
        names = [f.name for f in features]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"feature(s) listed under more than one kind: {', '.join(duplicates)}")
        spec = FeaturesSpec(
            generate=[
                GeneratorSpec(type=g["type"], params={k: v for k, v in g.items() if k != "type"})
                for g in raw.get("generate") or []
            ],
            features=features,
            impute=raw.get("impute", "median"),
            missing_indicators=bool(raw.get("missing_indicators", True)),
            encoding=raw.get("encoding", "onehot"),
            scaling=raw.get("scaling", "standard"),
            keep=list(raw.get("keep") or []),
            output=raw["output"],
            pipeline=raw["pipeline"],
        )
        spec._check_settings()
        return spec

    def with_settings(self, **settings: Any) -> FeaturesSpec:
        """The same features with other pipeline settings, e.g. ``encoding="index"``."""
        spec = replace(self, **settings)
        spec._check_settings()
        return spec

    def _check_settings(self) -> None:
        for setting, value, allowed in (
            ("impute", self.impute, ("median", "mean")),
            ("encoding", self.encoding, ("onehot", "index")),
            ("scaling", self.scaling, ("standard", "minmax", "none")),
        ):
            if value not in allowed:
                raise ValueError(f"features.{setting}: '{value}' is not one of {' / '.join(allowed)}")


# Feature settings a model may change for itself, e.g. `encoding: index` for a tree model.
FEATURE_OVERRIDES = ("impute", "missing_indicators", "encoding", "scaling")


@dataclass(frozen=True)
class ModelSpec:
    type: str
    params: dict[str, Any]
    metrics: list[str]
    output: str
    # Error is also reported per value of this dataset column, on the test split.
    breakdown: str | None = None
    features: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def parse(raw: dict[str, Any]) -> ModelSpec:
        spec = ModelSpec(
            type=raw["type"],
            params=dict(raw.get("params") or {}),
            metrics=list(raw.get("metrics") or ["rmse", "mae", "r2"]),
            output=raw["output"],
            breakdown=raw.get("breakdown"),
            features=dict(raw.get("features") or {}),
        )
        unknown = [k for k in spec.features if k not in FEATURE_OVERRIDES]
        if unknown:
            raise ValueError(f"model.features: only {', '.join(FEATURE_OVERRIDES)} can be overridden, "
                             f"not {', '.join(unknown)}")
        return spec


@dataclass(frozen=True)
class MLTaskConfig:
    name: str
    description: str
    source_layer: str
    source_table: str
    key: str
    dataset: DatasetSpec
    features: FeaturesSpec
    model: ModelSpec | None = None

    @staticmethod
    def load(name: str) -> MLTaskConfig:
        path = task_path(name)
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        ds = raw["dataset"]
        return MLTaskConfig(
            name=raw["name"],
            description=raw.get("description", ""),
            source_layer=raw["source"]["layer"],
            source_table=raw["source"]["table"],
            key=raw["key"],
            dataset=DatasetSpec(
                label=ds["label"],
                filters=dict(ds.get("filters") or {}),
                columns=dict(ds["columns"]),
                split=SplitSpec.parse(ds["split"]),
                output=ds["output"],
            ),
            features=FeaturesSpec.parse(raw["features"]),
            model=ModelSpec.parse(raw["model"]) if raw.get("model") else None,
        )

    @property
    def model_features(self) -> FeaturesSpec:
        """The feature pipeline the model is trained with: `features` plus the model's overrides."""
        overrides = self.model.features if self.model else {}
        return self.features.with_settings(**overrides) if overrides else self.features

    @property
    def dataset_fingerprint(self) -> str:
        """A hash of everything that decides the dataset's rows and columns."""
        return _fingerprint(
            {"source": [self.source_layer, self.source_table], "key": self.key,
             "dataset": dataclasses.asdict(self.dataset)}
        )

    @property
    def features_fingerprint(self) -> str:
        return _fingerprint(dataclasses.asdict(self.features))

    @property
    def model_fingerprint(self) -> str:
        """A hash of the features as the model sees them and of the whole `model` section."""
        if self.model is None:
            raise ValueError(f"[{self.name}] has no `model` section")
        return _fingerprint({"features": dataclasses.asdict(self.model_features),
                             "model": dataclasses.asdict(self.model)})


def _fingerprint(definition: dict[str, Any]) -> str:
    payload = json.dumps(definition, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


def task_path(name: str) -> str:
    return os.path.join(REPO_ROOT, "conf", "ml", f"{name}.yaml")


def available_tasks() -> list[str]:
    conf_dir = os.path.join(REPO_ROOT, "conf", "ml")
    return sorted(os.path.splitext(f)[0] for f in os.listdir(conf_dir) if f.endswith((".yaml", ".yml")))
