"""Verify that Java, PySpark, Delta Lake and the raw data are all usable.

    python scripts/check_env.py

Exits non-zero on the first failure, with the fix printed. Safe to re-run.
"""

import os
import shutil
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(REPO, "data", "raw")


def fail(what, fix):
    print(f"  FAIL  {what}")
    print(f"        fix: {fix}")
    sys.exit(1)


def ok(what, detail=""):
    print(f"  ok    {what}{'  ' + detail if detail else ''}")


print("1. Java")
java_home = os.environ.get("JAVA_HOME")
if not java_home:
    fail("JAVA_HOME is not set",
         "conda install -c conda-forge openjdk=17, then set JAVA_HOME to <conda>/Library/lib/jvm")
java = os.path.join(java_home, "bin", "java.exe" if os.name == "nt" else "java")
if not os.path.exists(java):
    fail(f"no java executable under JAVA_HOME ({java_home})", "check that JAVA_HOME points at the JDK root")
# java -version writes to stderr
ver = subprocess.run([java, "-version"], capture_output=True, text=True).stderr.splitlines()[0]
if not any(f'"{v}' in ver for v in ("17.", "11.")):
    fail(f"unsupported Java: {ver}", "Spark 3.5 needs Java 11 or 17")
ok("java", ver.strip())

if os.name == "nt":
    print("\n2. Hadoop native helpers (Windows only)")
    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        fail("HADOOP_HOME is not set",
             "download winutils.exe and hadoop.dll for Hadoop 3.3.x into <dir>/bin, set HADOOP_HOME=<dir>")
    for f in ("winutils.exe", "hadoop.dll"):
        p = os.path.join(hadoop_home, "bin", f)
        if not os.path.exists(p):
            fail(f"{f} missing from {hadoop_home}\\bin", "see README, 'Windows setup'")
    ok("winutils.exe and hadoop.dll", hadoop_home)

print("\n3. Python packages")
import pyspark  # noqa: E402
import delta  # noqa: E402
ok("pyspark", pyspark.__version__)
ok("delta-spark", getattr(delta, "__version__", "installed"))

print("\n4. Raw data")
expected = {
    "taxi_trips": ".parquet",
    "weather": ".csv",
    "air_quality": ".csv",
    "taxi_zones": ".csv",
}
for name, ext in expected.items():
    d = os.path.join(RAW, name)
    files = [f for f in os.listdir(d) if f.endswith(ext)] if os.path.isdir(d) else []
    if not files:
        fail(f"no {ext} files in data/raw/{name}/", "download the datasets, see README")
    size = sum(os.path.getsize(os.path.join(d, f)) for f in files)
    ok(f"data/raw/{name}", f"{len(files)} file(s), {size / 1e6:.1f} MB")

print("\n5. Spark + Delta round trip")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

builder = (
    SparkSession.builder.appName("check_env")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # first run downloads the Delta jars; this mirror is much faster from CN
    .config("spark.jars.repositories", "https://maven.aliyun.com/repository/public")
    .config("spark.ui.enabled", "false")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
ok("SparkSession", f"Spark {spark.version}")

tmp = os.path.join(tempfile.gettempdir(), "check_env_delta")
shutil.rmtree(tmp, ignore_errors=True)
try:
    spark.range(100).write.format("delta").mode("overwrite").save(tmp)
    n = spark.read.format("delta").load(tmp).count()
    if n != 100:
        fail(f"Delta round trip returned {n} rows, expected 100", "unexpected — report this")
    ok("Delta write and read", f"{n} rows")

    trips = spark.read.parquet(os.path.join(RAW, "taxi_trips", "*.parquet")).count()
    ok("read all taxi parquet", f"{trips:,} rows")
finally:
    shutil.rmtree(tmp, ignore_errors=True)
    spark.stop()

print("\nEnvironment OK.")
