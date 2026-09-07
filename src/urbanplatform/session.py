"""SparkSession construction.

Also fixes up the environment PySpark needs on Windows, so that a plain
``python jobs/run_ingest.py`` works without the caller having to export anything.
"""

from __future__ import annotations

import os
import sys

from .config import PlatformConfig


def _prepare_environment() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if os.name == "nt":
        hadoop_home = os.environ.get("HADOOP_HOME")
        if not hadoop_home:
            raise SystemExit(
                "HADOOP_HOME is not set. Spark needs winutils.exe and hadoop.dll on "
                "Windows; see README, 'Windows only: Hadoop native helpers'."
            )
        hadoop_bin = os.path.join(hadoop_home, "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


def build_session(platform: PlatformConfig):
    """Return a SparkSession configured from conf/platform.yaml."""
    _prepare_environment()

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    spark_conf = platform.spark
    builder = SparkSession.builder.appName(spark_conf.get("app_name", "urban-data-platform"))
    if spark_conf.get("master"):
        builder = builder.master(spark_conf["master"])
    for key, value in (spark_conf.get("config") or {}).items():
        builder = builder.config(key, str(value))

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
