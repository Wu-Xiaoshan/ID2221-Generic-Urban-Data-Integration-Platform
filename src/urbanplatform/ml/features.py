"""Week 4 Task 2: the reusable feature-engineering pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from pyspark import keyword_only
from pyspark.ml import Estimator, Model, Pipeline, PipelineModel, Transformer
from pyspark.ml.feature import (
    Imputer,
    MinMaxScaler,
    OneHotEncoder,
    SQLTransformer,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import FeatureSpec, FeaturesSpec
from .generators import expressions

FEATURES = "features"
MISSING_CATEGORY = "__missing__"

# Intermediate columns.
_NUM, _CAT, _BIN, _MISS = "_f_num__", "_f_cat__", "_f_bin__", "_f_miss__"
_IMP, _IDX, _OHE = "_f_imp__", "_f_idx__", "_f_ohe__"
_NUM_VEC, _NUM_SCALED = "_f_num_vec", "_f_num_scaled"


class SelectColumns(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    """Keep the listed columns that are present, in order, and drop everything else. """

    keep = Param(Params._dummy(), "keep", "columns to keep, where present",
                 typeConverter=TypeConverters.toListString)

    @keyword_only
    def __init__(self, keep: list[str] | None = None):
        super().__init__()
        self._setDefault(keep=[])
        self._set(**self._input_kwargs)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.select(*[c for c in self.getOrDefault(self.keep) if c in dataset.columns])


class _GroupImputerParams(Params):
    inputCols = Param(Params._dummy(), "inputCols", "numeric columns whose nulls are filled",
                      typeConverter=TypeConverters.toListString)
    groupCol = Param(Params._dummy(), "groupCol", "column whose groups each get their own fill value",
                     typeConverter=TypeConverters.toString)
    strategy = Param(Params._dummy(), "strategy", "median or mean", typeConverter=TypeConverters.toString)


class GroupImputer(Estimator, _GroupImputerParams, DefaultParamsReadable, DefaultParamsWritable):
    """Fill a numeric null with the training median (or mean) of the row's group."""

    @keyword_only
    def __init__(self, inputCols: list[str] | None = None, groupCol: str | None = None,
                 strategy: str = "median"):
        super().__init__()
        self._setDefault(strategy="median")
        self._set(**self._input_kwargs)

    def _fit(self, dataset: DataFrame) -> GroupImputerModel:
        cols, group = self.getOrDefault(self.inputCols), self.getOrDefault(self.groupCol)

        def agg(c: str):
            valid = F.when(~F.isnan(c), F.col(c))
            if self.getOrDefault(self.strategy) == "median":
                return F.percentile_approx(valid, 0.5, 100_000).alias(c)
            return F.avg(valid).alias(c)

        overall = dataset.agg(*[agg(c) for c in cols]).first().asDict()
        groups = dataset.groupBy(group).agg(*[agg(c) for c in cols]).collect()
        values = {
            c: {"*": overall[c],
                "groups": {str(r[group]): r[c] for r in groups if r[group] is not None and r[c] is not None}}
            for c in cols
        }
        return GroupImputerModel(inputCols=cols, groupCol=group, strategy=self.getOrDefault(self.strategy),
                                 values=json.dumps(values, sort_keys=True))


class GroupImputerModel(Model, _GroupImputerParams, DefaultParamsReadable, DefaultParamsWritable):
    values = Param(Params._dummy(), "values", "JSON: column -> overall value and value per group",
                   typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, inputCols: list[str] | None = None, groupCol: str | None = None,
                 strategy: str = "median", values: str | None = None):
        super().__init__()
        self._setDefault(strategy="median")
        self._set(**self._input_kwargs)

    def fill_values(self) -> dict[str, Any]:
        return json.loads(self.getOrDefault(self.values))

    def _transform(self, dataset: DataFrame) -> DataFrame:
        group = F.col(self.getOrDefault(self.groupCol))
        for c, v in self.fill_values().items():
            fill = F.lit(v["*"])
            for g, value in sorted(v["groups"].items(), reverse=True):
                fill = F.when(group == F.lit(g), F.lit(value)).otherwise(fill)
            dataset = dataset.withColumn(c, F.when(F.col(c).isNull() | F.isnan(c), fill).otherwise(F.col(c)))
        return dataset


def _q(column: str) -> str:
    return f"`{column}`"


def _literal(value: Any) -> str:
    return str(value) if isinstance(value, (int, float)) else "'" + str(value).replace("'", "''") + "'"


def _without_missing_codes(f: FeatureSpec) -> str:
    if not f.missing_values:
        return _q(f.name)
    codes = ", ".join(_literal(v) for v in f.missing_values)
    return f"CASE WHEN {_q(f.name)} IN ({codes}) THEN NULL ELSE {_q(f.name)} END"


def _prepare_expr(f: FeatureSpec) -> str:
    value = _without_missing_codes(f)
    if f.kind == "numeric":
        value = f"CAST({value} AS DOUBLE)"
        return f"log1p({value})" if f.log1p else value
    if f.kind == "categorical":
        return f"COALESCE(CAST({value} AS STRING), '{MISSING_CATEGORY}')"
    return f"CAST(COALESCE(CAST({value} AS BOOLEAN), false) AS DOUBLE)"


_PREFIX = {"numeric": _NUM, "categorical": _CAT, "binary": _BIN}


def generated_columns(spec: FeaturesSpec) -> dict[str, tuple[str, str]]:
    out = {}
    for g in spec.generate:
        for name, expr in expressions(g).items():
            out[name] = (g.type, expr)
    return out


def preparation_stages(spec: FeaturesSpec) -> list[Transformer]:
    stages: list[Transformer] = []
    for g in spec.generate:
        derived = ", ".join(f"{expr} AS {_q(name)}" for name, expr in expressions(g).items())
        stages.append(SQLTransformer(statement=f"SELECT *, {derived} FROM __THIS__"))
    prepared = ", ".join(f"{_prepare_expr(f)} AS {_q(_PREFIX[f.kind] + f.name)}" for f in spec.features)
    stages.append(SQLTransformer(statement=f"SELECT *, {prepared} FROM __THIS__"))
    return stages


def prepare(spec: FeaturesSpec, df: DataFrame) -> DataFrame:
    return PipelineModel(stages=preparation_stages(spec)).transform(df)


def check_inputs(spec: FeaturesSpec, columns: list[str]) -> None:
    derived = generated_columns(spec)
    missing = [f.name for f in spec.features if f.name not in columns and f.name not in derived]
    missing += [f"{f.impute_by} (impute_by of {f.name})" for f in spec.features
                if f.impute_by and f.impute_by not in columns and f.impute_by not in derived]
    if missing:
        raise ValueError(
            f"feature(s) neither in the dataset nor generated: {', '.join(missing)}. "
            "Add the column under dataset.columns, or a generator that derives it."
        )


def numerics_with_nulls(spec: FeaturesSpec, prepared: DataFrame) -> list[str]:
    numeric = spec.of_kind("numeric")
    if not numeric:
        return []
    aggs = [
        F.sum((F.col(_NUM + f.name).isNull() | F.isnan(_NUM + f.name)).cast("long")).alias(f.name)
        for f in numeric
    ]
    row = prepared.agg(*aggs).first().asDict()
    return [f.name for f in numeric if row[f.name]]


def build_feature_pipeline(spec: FeaturesSpec, indicators: list[str] | None = None) -> Pipeline:
    numeric = [f.name for f in spec.of_kind("numeric")]
    categorical = [f.name for f in spec.of_kind("categorical")]
    binary = [f.name for f in spec.of_kind("binary")]
    indicators = indicators if spec.missing_indicators else []

    stages: list[Any] = preparation_stages(spec)
    vector_parts: list[str] = []

    if indicators:
        flags = ", ".join(
            f"CAST({_q(_NUM + n)} IS NULL OR isnan({_q(_NUM + n)}) AS DOUBLE) AS {_q(_MISS + n)}"
            for n in indicators
        )
        stages.append(SQLTransformer(statement=f"SELECT *, {flags} FROM __THIS__"))

    by_group: dict[str, list[str]] = {}
    for f in spec.of_kind("numeric"):
        if f.impute_by:
            by_group.setdefault(f.impute_by, []).append(_NUM + f.name)
    for group, cols in by_group.items():
        stages.append(GroupImputer(inputCols=cols, groupCol=group, strategy=spec.impute))

    if numeric:
        stages.append(Imputer(inputCols=[_NUM + n for n in numeric], outputCols=[_IMP + n for n in numeric],
                              strategy=spec.impute))
        stages.append(VectorAssembler(inputCols=[_IMP + n for n in numeric], outputCol=_NUM_VEC))
        if spec.scaling == "standard":
            stages.append(StandardScaler(inputCol=_NUM_VEC, outputCol=_NUM_SCALED, withMean=True, withStd=True))
            vector_parts.append(_NUM_SCALED)
        elif spec.scaling == "minmax":
            stages.append(MinMaxScaler(inputCol=_NUM_VEC, outputCol=_NUM_SCALED))
            vector_parts.append(_NUM_SCALED)
        else:
            vector_parts.append(_NUM_VEC)

    if categorical:
        stages.append(StringIndexer(inputCols=[_CAT + n for n in categorical],
                                    outputCols=[_IDX + n for n in categorical],
                                    handleInvalid="keep", stringOrderType="frequencyDesc"))
        if spec.encoding == "onehot":
            stages.append(OneHotEncoder(inputCols=[_IDX + n for n in categorical],
                                        outputCols=[_OHE + n for n in categorical],
                                        handleInvalid="keep", dropLast=True))
            vector_parts += [_OHE + n for n in categorical]
        else:
            vector_parts += [_IDX + n for n in categorical]

    vector_parts += [_BIN + n for n in binary] + [_MISS + n for n in indicators]
    if not vector_parts:
        raise ValueError("the feature list is empty")
    stages.append(VectorAssembler(inputCols=vector_parts, outputCol=FEATURES))
    stages.append(SelectColumns(keep=[*spec.keep, "label", FEATURES]))
    return Pipeline(stages=stages)


def fit_feature_pipeline(spec: FeaturesSpec, train: DataFrame) -> tuple[PipelineModel, list[str]]:
    """Fit on the training split. Returns the model and the numerics given missing flags."""
    check_inputs(spec, train.columns)
    indicators = numerics_with_nulls(spec, prepare(spec, train)) if spec.missing_indicators else []
    return build_feature_pipeline(spec, indicators).fit(train), indicators


@dataclass
class Slot:
    index: int
    feature: str
    source: str
    detail: str = ""


@dataclass
class FeatureManifest:
    """Which feature, and which platform dataset, each slot of the vector comes from."""

    size: int
    slots: list[Slot] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps(
            {"size": self.size,
             "slots": [[s.index, s.feature, s.source, s.detail] for s in self.slots]},
            indent=1,
        )

    def slots_by(self, attr: str) -> dict[str, int]:
        out: dict[str, int] = {}
        for s in self.slots:
            out[getattr(s, attr)] = out.get(getattr(s, attr), 0) + 1
        return out


def feature_manifest(spec: FeaturesSpec, features_schema_field) -> FeatureManifest:
    """Read the vector's slot names from its ML attribute metadata and map them back."""
    attrs = features_schema_field.metadata["ml_attr"]
    size = int(attrs["num_attrs"])
    names: dict[int, str] = {}
    for group in attrs.get("attrs", {}).values():
        for a in group:
            names[int(a["idx"])] = a.get("name", "")

    numeric = [f.name for f in spec.of_kind("numeric")]
    source = {f.name: f.source for f in spec.features}
    by_prefix = sorted(
        [(p + f.name, f.name) for f in spec.features for p in (_OHE, _IDX, _BIN, _MISS)],
        key=lambda item: -len(item[0]),
    )
    manifest = FeatureManifest(size=size)
    for i in range(size):
        name = names.get(i, "")
        if name.startswith((_NUM_SCALED, _NUM_VEC)):
            prefix = _NUM_SCALED if name.startswith(_NUM_SCALED) else _NUM_VEC
            rest = name[len(prefix) + 1:]
            feat = rest[len(_IMP):] if rest.startswith(_IMP) else numeric[int(rest)]
            detail = "numeric"
        else:
            feat, detail = name, ""
            for prefix, fname in by_prefix:
                if name == prefix or name.startswith(prefix + "_"):
                    feat = fname
                    if prefix.startswith(_MISS):
                        detail = "missing flag"
                    elif prefix.startswith(_OHE):
                        detail = "= " + name[len(prefix) + 1:]
                    elif prefix.startswith(_IDX):
                        detail = "index"
                    else:
                        detail = "binary"
                    break
        manifest.slots.append(Slot(index=i, feature=feat, source=source.get(feat, "?"), detail=detail))
    return manifest


def fitted_stage(model: PipelineModel, cls: type):
    return next((s for s in model.stages if isinstance(s, cls)), None)


def learnt_state(spec: FeaturesSpec, model: PipelineModel) -> dict[str, Any]:
    from pyspark.ml.feature import ImputerModel, StandardScalerModel, StringIndexerModel

    state: dict[str, Any] = {}
    numeric = [f.name for f in spec.of_kind("numeric")]
    imputer = fitted_stage(model, ImputerModel)
    if imputer is not None:
        row = imputer.surrogateDF.first().asDict()
        state["impute"] = {n: row[_NUM + n] for n in numeric}
    for stage in model.stages:
        if isinstance(stage, GroupImputerModel):
            by = stage.getOrDefault(stage.groupCol)
            for c, v in stage.fill_values().items():
                state.setdefault("group_impute", {})[c[len(_NUM):]] = {"by": by, **v}
    scaler = fitted_stage(model, StandardScalerModel)
    if scaler is not None:
        state["mean"] = dict(zip(numeric, scaler.mean.toArray().tolist()))
        state["std"] = dict(zip(numeric, scaler.std.toArray().tolist()))
    indexer = fitted_stage(model, StringIndexerModel)
    if indexer is not None:
        state["labels"] = dict(zip([f.name for f in spec.of_kind("categorical")], indexer.labelsArray))
    return state
