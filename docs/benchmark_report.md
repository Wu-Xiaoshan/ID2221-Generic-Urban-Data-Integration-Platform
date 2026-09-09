# Task 6 - Benchmark Report: Storage Strategies

*This report evaluates the scalability of the implementation by comparing two different storage strategies for the Taxi Trips dataset.* All measurements were taken using `jobs/run_benchmark.py` against the cleaned `silver` tables. 

## 1. Storage strategies evaluated

To understand the trade-offs between read and write performance, we implemented two variants of the fact table:

*   **Strategy A (Baseline):** Data is written to Delta Lake without any partitioning.
*   **Strategy B (Partitioned):** Data is partitioned by the `pickup_date` column before being written to Delta Lake.

## 2. Benchmark results

The pipeline ingested approximately 9.55 million rows for this test[cite: 4]. The performance metrics are recorded below:

| Metric | Strategy A (Unpartitioned) | Strategy B (Partitioned by `pickup_date`) |
|---|---|---|
| **Ingestion time** | 30.50 sec | 32.21 sec |
| **Storage size** | 724.20 MB | 611.65 MB |
| **Generated files** | 10 | 244 |
| **Query 1** (Trips per borough) | 4.8392 sec | 3.3056 sec |
| **Query 2** (Avg duration per day) | 1.9832 sec | 1.9479 sec |
| **Query 3** (Avg fare per borough) | 1.3301 sec | 1.5646 sec |

## 3. Engineering decisions and trade-offs

**The cost of partitioning on ingestion.** Strategy A exhibited a slightly faster ingestion time (30.50s vs 32.21s). Partitioning by `pickup_date` forces Spark to perform a shuffle operation to route records to their respective date folders, adding computational overhead during the write phase.

**Storage efficiency vs. the small file problem.** Strategy B generated significantly more files (244 against 10). Because partitioned data groups similar values together, Parquet's dictionary encoding becomes highly efficient, dropping the total footprint from 724 MB down to 611 MB. However, as data scales, generating hundreds of small files per batch will eventually bottleneck the Delta Log and NameNode metadata operations.

### When does partitioning improve query latency?

**When queries align with the partition key.** Strategy B outperformed Strategy A on Query 1 and Query 2. For Query 2 specifically (average duration per day), grouping by `pickup_date` directly benefits from partition pruning. Spark reads the Delta metadata and entirely skips scanning irrelevant data files, reducing I/O.

### When does partitioning become harmful?

**When queries require full table scans.** Strategy A performed better on Query 3 (average fare per borough). Because this query groups by a dimension (`borough`) that requires joining with a lookup table without filtering by date, Spark is forced to read the entire dataset. In this scenario, the I/O efficiency of reading 10 larger, contiguous files outweighs the overhead of opening and closing 244 small files.