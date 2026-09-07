"""Dataset-specific transformation rules.

Importing this package registers every rule. Each module below is the complete set of
code the platform contains for one dataset; everything else about it lives in
conf/datasets/<name>.yaml.
"""

from . import air_quality, taxi_trips, taxi_zones, weather  # noqa: F401

__all__ = ["taxi_trips", "weather", "air_quality", "taxi_zones"]
