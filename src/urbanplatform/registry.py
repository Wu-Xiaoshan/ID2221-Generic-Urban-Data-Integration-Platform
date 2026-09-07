"""Registry for dataset-specific transformation rules.

This is the framework's only extension point for code. Everything else about a dataset
is declared in YAML; what cannot be declared is registered here.

Two stages exist because the two kinds of dataset-specific work sit on opposite sides
of the quality checks:

``pre_quality``   derives the columns the rules are written against — a surrogate key,
                  a trip duration, a composite site id. These must exist before the
                  rules can test them.

``post_quality``  reshapes data that is already known to be clean, typically by changing
                  the grain. Aggregating six air-quality monitors into one city-wide
                  hourly value belongs here: de-duplicating on the five-part key has to
                  happen first, or the average would be weighted by however many
                  instruments each site happens to run.
"""

from __future__ import annotations

from typing import Callable

from pyspark.sql import DataFrame

from .config import DatasetConfig

Transform = Callable[[DataFrame, DatasetConfig], DataFrame]

_PRE_QUALITY: dict[str, Transform] = {}
_POST_QUALITY: dict[str, Transform] = {}


def pre_quality(dataset: str) -> Callable[[Transform], Transform]:
    def register(fn: Transform) -> Transform:
        _PRE_QUALITY[dataset] = fn
        return fn

    return register


def post_quality(dataset: str) -> Callable[[Transform], Transform]:
    def register(fn: Transform) -> Transform:
        _POST_QUALITY[dataset] = fn
        return fn

    return register


def _apply(registry: dict[str, Transform], df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    fn = registry.get(cfg.name)
    return fn(df, cfg) if fn else df


def apply_pre_quality(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    return _apply(_PRE_QUALITY, df, cfg)


def apply_post_quality(df: DataFrame, cfg: DatasetConfig) -> DataFrame:
    return _apply(_POST_QUALITY, df, cfg)


def registered(dataset: str) -> list[str]:
    """Which stages a dataset has registered. Reported in the run log."""
    stages = []
    if dataset in _PRE_QUALITY:
        stages.append("pre_quality")
    if dataset in _POST_QUALITY:
        stages.append("post_quality")
    return stages
