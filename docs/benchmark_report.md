# Task 6 — Benchmark Report: Storage Strategies

Two storage strategies for the `taxi_trips` fact table, measured by `jobs/run_benchmark.py` against
the 9,551,907-row silver table. Both are written through the same `write_delta` + `optimize` pair
the ingestion framework uses, in a session built from `conf/platform.yaml`, so what is measured here
is the layout the platform would really produce rather than one produced by Spark defaults.

## 1. Storage strategies evaluated

The two candidates are the ones [Storage Architecture §5](storage_architecture.md#5-partitioning-strategy)
weighs against each other. Neither is what the platform ships — `taxi_trips` uses monthly partitions
plus Z-order — because the point is to measure the two ends of that decision.

| | Strategy A | Strategy B |
|---|---|---|
| Layout | `partitionBy(pickup_date)` | unpartitioned |
| Post-write | `OPTIMIZE` (compaction) | `OPTIMIZE ... ZORDER BY (pu_location_id)` |
| Table | `benchmark/bench_taxi_daily` | `benchmark/bench_taxi_zordered` |
| Idea | the obvious time key, one directory per day | one large file, ordered so Delta can skip on location |

## 2. How the numbers were taken

Three details decide whether these numbers mean anything, and the first version of this benchmark
got all three wrong.

**Size and file count come from the Delta log, not from the directory.** `mode("overwrite")` writes
a new version and leaves the superseded files in place until `VACUUM`, so walking the directory
measures how many times the script has been run. We use
`DeltaTable.forPath(spark, path).detail()` for `sizeInBytes` and `numFiles`, and report the directory
total separately as what is on disk before `VACUUM`.

**Query latency is the median of three runs, after a discarded warm-up.** Measured cold, the first
query of a run took 3.17 s against a 1.07 s steady state — larger than any difference between the
two strategies, and it lands entirely on whichever strategy is measured first.

**The two strategies are interleaved, not run in blocks.** Each pass runs query *n* on A and then on
B, so drift over the run — page cache filling, JIT still settling — is shared instead of being
handed to one side.

Environment: Spark 3.5.3, Delta 3.2.1, `local[4]`, 4 GB driver heap, `shuffle.partitions = 8`,
snappy, `targetFileSize = 128 MB`, `optimizeWrite` and `autoCompact` on.

## 3. Benchmark results

| Metric | A: daily partitions | B: unpartitioned + ZORDER |
|---|---|---|
| **Ingestion time** (write + OPTIMIZE) | **38.02 s** | 82.85 s |
| &nbsp;&nbsp;· write | 37.56 s | **25.20 s** |
| &nbsp;&nbsp;· OPTIMIZE | 0.46 s | 57.65 s |
| **Storage size** (live) | **300.6 MB** | 359.3 MB |
| **Generated files** (live) | 91 | **1** |
| On disk before `VACUUM` | 303.2 MB | 724.9 MB |
| **Q1** trips per borough | 0.99 s | **0.78 s** |
| **Q2** average trip duration per day | 0.53 s | **0.49 s** |
| **Q3** average fare per borough | 0.97 s | **0.79 s** |
| **Q4** Q1 filtered to one day *(diagnostic, see §5)* | **0.50 s** | 0.52 s |

## 4. Discussion

**Ingestion: partitioning is the cheaper write, Z-ordering is the expensive one.** A's *write* is the
slower of the two, 37.56 s against 25.20 s, which is the shuffle that routes each record to its date
directory. But that is the whole of A's cost: its `OPTIMIZE` finds nothing to compact and returns in
0.46 s, because every partition is already one file well below the 128 MB target. B pays 57.65 s to
Z-order, more than twice its own write, since Z-ordering sorts and rewrites the entire table. Total
ingestion is 38.02 s against 82.85 s — the opposite of the intuition that partitioning is what makes
writes slow. It does; it is simply not the dominant term once a post-write step is in the picture.

**Storage: the partitioned copy is 16% smaller.** 300.6 MB against 359.3 MB, which is not what we
expected either. Partitioning by `pickup_date` hoists that column into the directory name, but that
is a few megabytes at most. The real cause is what each file contains: an A file holds one day, so
`pickup_ts`, `pickup_hour` and `pickup_year_month` all have a narrow range inside it and encode
tightly, while B's Z-order is on `pu_location_id` and scatters a whole quarter of timestamps through
one file. Clustering on the column a query filters by and clustering on the column that compresses
well are not the same goal, and here they point in different directions.

**File count: 91 against 1, and both numbers are wrong.** A's 91 files average 3.3 MB, a
fortieth of the 128 MB target — the "partitions below one target file" failure
[Storage Architecture §5](storage_architecture.md#5-partitioning-strategy) predicts for this dataset,
visible directly in the ingestion numbers. B lands at the other extreme for a reason worth recording:
`OPTIMIZE` does not use `targetFileSize` but `spark.databricks.delta.optimize.maxFileSize`, which
defaults to 1 GB, so the whole 359 MB table became a single file. Parquet row groups still let Spark
read it in parallel, so this costs nothing at this size, but neither layout is what the table
property asked for.

**Query latency: B wins the three required queries.** A is 27% slower on Q1, 8% on Q2 and 24% on Q3.
None of the three queries filters on a date, so A prunes nothing and pays 91 file opens and 91
Parquet footers to read exactly the same rows B reads from one. Q2 is the narrowest gap because it
touches no lookup table and groups by the partition key itself; Q1 and Q3 both join `taxi_zones` and
show the full penalty.

## 5. When does partitioning improve query latency?

Only when a query **filters** on the partition key. Partition pruning is a decision made against the
`WHERE` clause while Delta builds the file list; a `GROUP BY` on the partition column prunes nothing,
because every partition contributes at least one group and so every partition has to be read. This
is worth stating plainly because two of the three required queries look like they should benefit and
do not: Q1 groups by borough, which is not the partition key at all, and Q2 groups by `pickup_date`,
which is — and still scans all 91 directories.

Q4 exists to measure the case that does work. It is Q1 with `WHERE pickup_date = DATE '2024-02-15'`
added and nothing else changed, so the difference between them is the filter:

| | Q1 (no filter) | Q4 (one-day filter) | change |
|---|---|---|---|
| A: daily partitions | 0.99 s | 0.50 s | **−49%** |
| B: unpartitioned + ZORDER | 0.78 s | 0.52 s | −33% |

Both get faster, because the filter cuts the rows reaching the join and the aggregate either way.
A gets *more* out of it — half its time against a third — and the extra is the pruning: it opens 1
directory instead of 91 and reads 3.3 MB instead of 300.6 MB. Q4 is also the only query where A is
not behind, and even then it only draws level (0.50 s against 0.52 s). At 9.55M rows the query is
already down to about half a second of mostly fixed cost — planning, broadcasting the zone table,
scheduling — and pruning a 300 MB scan cannot show up under that floor. The mechanism is real and
the measurement is at the wrong scale to reward it, which is a statement about this extract rather
than about partitioning.

## 6. When does partitioning become harmful?

**When the partitions are smaller than one target file.** That is this dataset: 91 days of 3.3 MB
against a 128 MB target. Every scan pays per-file overhead, and every commit adds 91 entries to the
transaction log, to skip data that reads in milliseconds.

**When queries do not filter on the key.** All three required queries are of this shape, and A loses
all three. A full scan of a partitioned table is strictly worse than a full scan of an unpartitioned
one — same bytes, more file handles.

**When the key is chosen for pruning and the data has to pay for it elsewhere.** The storage result
above is the mild version of this, in A's favour. The general point is that the partition key
decides file contents, and file contents decide compression, so a key picked purely for pruning can
cost storage — or, as here, accidentally save it.

## 7. What this says about the shipped design

[Storage Architecture §5](storage_architecture.md#5-partitioning-strategy) predicted A would lose on
all four metrics and said the report would record it if the numbers disagreed. They disagree on two:

| | Predicted | Measured |
|---|---|---|
| Ingestion time | A slower | **A faster** (38.02 s vs 82.85 s) — Z-order dominates B |
| Storage size | A larger | **A smaller** (300.6 MB vs 359.3 MB) — better encoding within a day |
| File count | A worse | A worse, 91 against 1 |
| Query latency | A slower | A slower on all three required queries, 8–27% |

The conclusion the architecture draws is unchanged, and the two surprises do not rescue daily
partitioning: it still loses every query the assignment asks for, and it produces 91 files averaging
3.3 MB, which is the thing that gets worse rather than better as more days arrive. What the numbers
correct is the *reason*. Daily partitioning is not rejected because it writes slowly or wastes space
— it does neither — but because the partitions are two orders of magnitude too small and nothing
queries them by date. `pickup_year_month` keeps the time key at 3 partitions of ~125 MB, and the
location filter that Z-order serves is the one queries actually use.

One operational note the measurement made concrete. B's directory held 724.9 MB for a 359.3 MB
table, almost exactly double, because `OPTIMIZE` rewrote every file and Delta keeps the originals
until `VACUUM`. A's directory held 303.2 MB for 300.6 MB, because its `OPTIMIZE` rewrote nothing.
Any table that gets a real `OPTIMIZE` should be assumed to occupy twice its size until retention is
managed deliberately — the point [Storage Architecture §6](storage_architecture.md#6-design-discussion)
makes about the 20× case, reproduced here at 1×.
