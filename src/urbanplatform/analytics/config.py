"""Configuration for the analytical layer, loaded from conf/analytics.yaml.
Holds the bucket edges, the condition-code families and the support thresholds that the
six analyses share, plus the helpers that turn them into SQL ``CASE`` expressions.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import yaml

from ..config import REPO_ROOT


@dataclass(frozen=True)
class Bucket:
    label: str
    max: float | None


@dataclass(frozen=True)
class AnalyticsConfig:
    sources: dict[str, dict[str, str]]
    reporting_timezone: str
    weather_families: dict[str, list[int]]
    precip_buckets: list[Bucket]
    aq_quartiles: list[Bucket]
    aq_epa_categories: list[Bucket]
    thresholds: dict[str, int]
    sentinel_zone_ids: list[int] = field(default_factory=lambda: [264, 265])

    @staticmethod
    def load(path: str | None = None) -> AnalyticsConfig:
        path = path or os.path.join(REPO_ROOT, "conf", "analytics.yaml")
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        def buckets(key: str) -> list[Bucket]:
            return [Bucket(label=str(b["label"]), max=b["max"]) for b in raw[key]]

        return AnalyticsConfig(
            sources=raw["sources"],
            reporting_timezone=raw.get("reporting_timezone", "America/New_York"),
            weather_families={str(k): list(v) for k, v in raw["weather_families"].items()},
            precip_buckets=buckets("precip_buckets"),
            aq_quartiles=buckets("aq_quartiles"),
            aq_epa_categories=buckets("aq_epa_categories"),
            thresholds={str(k): int(v) for k, v in raw["thresholds"].items()},
            sentinel_zone_ids=list(raw.get("sentinel_zone_ids", [264, 265])),
        )

    @property
    def sentinels(self) -> str:
        """``264, 265`` -- for use inside ``NOT IN (...)``."""
        return ", ".join(str(i) for i in self.sentinel_zone_ids)

    def threshold(self, name: str) -> int:
        try:
            return self.thresholds[name]
        except KeyError:
            raise KeyError(
                f"threshold '{name}' is not in conf/analytics.yaml; "
                f"known: {', '.join(sorted(self.thresholds))}"
            ) from None


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def bucket_case(column: str, buckets: list[Bucket]) -> str:
    """Assigning ``column`` to the first bucket it fits."""
    branches = [f"WHEN {column} IS NULL THEN NULL"]
    for bucket in buckets[:-1]:
        branches.append(f"WHEN {column} <= {bucket.max} THEN {_sql_str(bucket.label)}")
    branches.append(f"ELSE {_sql_str(buckets[-1].label)}")
    return "CASE " + " ".join(branches) + " END"


def family_case(column: str, families: dict[str, list[int]]) -> str:
    """Grouping categorical codes into named families."""
    branches = [f"WHEN {column} IS NULL THEN NULL"]
    for name, codes in families.items():
        codes_sql = ", ".join(str(c) for c in codes)
        branches.append(f"WHEN {column} IN ({codes_sql}) THEN {_sql_str(name)}")
    return "CASE " + " ".join(branches) + " END"
