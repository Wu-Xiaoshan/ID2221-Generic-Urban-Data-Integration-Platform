import time
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_storage_metrics(directory):
    """Calculate total size (MB) and parquet file count."""
    total_size = 0
    file_count = 0
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            if f.endswith('.parquet'):
                file_count += 1
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size / (1024 * 1024), file_count

def run_queries(spark, df_trips, df_zones, strategy_name):
    """Run Task 6 benchmark queries."""
    print(f"\n--- Running Queries on {strategy_name} Table ---")
    
    df_trips.createOrReplaceTempView("trips")
    df_zones.createOrReplaceTempView("zones")

    
    start = time.time()
    spark.sql("""
        SELECT z.borough, COUNT(*) as trip_count
        FROM trips t
        JOIN zones z ON t.pu_location_id = z.location_id
        GROUP BY z.borough
    """).collect()
    print(f"Query 1 Latency: {time.time() - start:.4f} sec")

    
    start = time.time()
    spark.sql("""
        SELECT pickup_date, AVG(CAST(dropoff_ts AS LONG) - CAST(pickup_ts AS LONG))/60 as avg_duration_min
        FROM trips
        GROUP BY pickup_date
    """).collect()
    print(f"Query 2 Latency: {time.time() - start:.4f} sec")

    
    start = time.time()
    spark.sql("""
        SELECT z.borough, AVG(t.fare_amount) as avg_fare
        FROM trips t
        JOIN zones z ON t.pu_location_id = z.location_id
        GROUP BY z.borough
    """).collect()
    print(f"Query 3 Latency: {time.time() - start:.4f} sec")

def main():
    
    builder = SparkSession.builder \
        .appName("Task6_Benchmark") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver_trips_path = "data/lakehouse/silver/taxi_trips"
    silver_zones_path = "data/lakehouse/silver/taxi_zones"
    
    benchmark_dir = "data/lakehouse/benchmark"
    path_unpartitioned = f"{benchmark_dir}/unpartitioned"
    path_partitioned = f"{benchmark_dir}/partitioned"

    df_trips = spark.read.format("delta").load(silver_trips_path)
    df_zones = spark.read.format("delta").load(silver_zones_path)

    
    print("\n[Strategy A] Writing Unpartitioned Data...")
    start_time = time.time()
    df_trips.write.format("delta").mode("overwrite").save(path_unpartitioned)
    ingestion_time_a = time.time() - start_time
    
    size_a, count_a = get_storage_metrics(path_unpartitioned)
    print(f"-> Ingestion Time: {ingestion_time_a:.2f} sec | Size: {size_a:.2f} MB | Files: {count_a}")

   
    print("\n[Strategy B] Writing Partitioned Data...")
    start_time = time.time()
    df_trips.write.format("delta").partitionBy("pickup_date").mode("overwrite").save(path_partitioned)
    ingestion_time_b = time.time() - start_time
    
    size_b, count_b = get_storage_metrics(path_partitioned)
    print(f"-> Ingestion Time: {ingestion_time_b:.2f} sec | Size: {size_b:.2f} MB | Files: {count_b}")


    df_unpart = spark.read.format("delta").load(path_unpartitioned)
    df_part = spark.read.format("delta").load(path_partitioned)

    run_queries(spark, df_unpart, df_zones, "Strategy A (Unpartitioned)")
    run_queries(spark, df_part, df_zones, "Strategy B (Partitioned)")

    spark.stop()

if __name__ == "__main__":
    main()