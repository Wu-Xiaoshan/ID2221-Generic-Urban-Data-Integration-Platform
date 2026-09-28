from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..analytics.config import AnalyticsConfig
from .config import GeneratorSpec

# name -> function(params) -> {output column: SQL expression}
Generator = Callable[[dict[str, Any]], dict[str, str]]
_GENERATORS: dict[str, Generator] = {}


def generator(name: str) -> Callable[[Generator], Generator]:
    def register(fn: Generator) -> Generator:
        _GENERATORS[name] = fn
        return fn

    return register


def expressions(spec: GeneratorSpec) -> dict[str, str]:
    try:
        fn = _GENERATORS[spec.type]
    except KeyError:
        raise ValueError(
            f"unknown generator type '{spec.type}'; known: {', '.join(sorted(_GENERATORS))}"
        ) from None
    return fn(spec.params)


def _sql_str(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@generator("temporal")
def temporal(params: dict[str, Any]) -> dict[str, str]:
    """Calendar features of one UTC timestamp, read on a local clock.
    ``hour_sin`` and ``hour_cos`` place the time of day on a circle, so 23:50 and 00:10 are
    close together, which an hour number from 0 to 23 does not say.
    """
    column = params["column"]
    local = f"from_utc_timestamp({column}, {_sql_str(params.get('timezone', 'UTC'))})"
    day_fraction = f"((hour({local}) * 60 + minute({local})) / 1440D)"
    holidays = ", ".join(f"DATE {_sql_str(d)}" for d in params.get("holidays") or [])

    available = {
        "hour_of_day": f"hour({local})",
        "day_of_week": f"dayofweek({local})",  # 1 = Sunday
        "month": f"month({local})",
        "is_weekend": f"dayofweek({local}) IN (1, 7)",
        "is_holiday": f"to_date({local}) IN ({holidays})" if holidays else "false",
        "hour_sin": f"sin(2 * pi() * {day_fraction})",
        "hour_cos": f"cos(2 * pi() * {day_fraction})",
        "hour_of_week": f"(dayofweek({local}) - 1) * 24 + hour({local})",
    }
    parts = params.get("parts") or list(available)
    unknown = [p for p in parts if p not in available]
    if unknown:
        raise ValueError(f"temporal: unknown part(s) {', '.join(unknown)}; known: {', '.join(available)}")
    prefix = params.get("prefix", column)
    return {f"{prefix}_{part}": available[part] for part in parts}


@generator("zone_pair")
def zone_pair(params: dict[str, Any]) -> dict[str, str]:
    """Features of a trip's two endpoints taken together."""
    pickup, dropoff = params["pickup"], params["dropoff"]
    out = {
        "same_borough": f"{pickup['borough']} = {dropoff['borough']}",
        "borough_pair": f"concat_ws(' -> ', {pickup['borough']}, {dropoff['borough']})",
    }
    airports = params.get("airports") or []
    if airports:
        ids = ", ".join(str(int(a)) for a in airports)
        out["pickup_is_airport"] = f"{pickup['zone']} IN ({ids})"
        out["dropoff_is_airport"] = f"{dropoff['zone']} IN ({ids})"
    return out


@generator("code_groups")
def code_groups(params: dict[str, Any]) -> dict[str, str]:
    groups = params["groups"]
    if isinstance(groups, str):
        prefix = "analytics."
        if not groups.startswith(prefix):
            raise ValueError(f"code_groups: '{groups}' is neither a mapping nor analytics.<name>")
        groups = getattr(AnalyticsConfig.load(), groups[len(prefix):])
    column = params["column"]
    branches = " ".join(
        f"WHEN {column} IN ({', '.join(str(c) for c in codes)}) THEN {_sql_str(name)}"
        for name, codes in groups.items()
    )
    return {params["output"]: f"CASE {branches} END"}


@generator("expression")
def expression(params: dict[str, Any]) -> dict[str, str]:
    return {name: str(expr) for name, expr in params["columns"].items()}
