# Task 6 — Benchmark Report: Storage Strategies

Two storage strategies for the `taxi_trips` fact table, measured by `jobs/run_benchmark.py` against
the 9,551,907-row silver table. Both are written through the same `write_delta` + `optimize` pair
the ingestion framework uses, in a session built from `conf/platform.yaml`, so the layouts measured
here are the ones the platform produces in normal operation.

## 1. Strategies

These are the two ends of the decision
[Storage Architecture §5](storage_architecture.md#5-partitioning-strategy) weighs. Neither is what
the platform ships: `taxi_trips` uses monthly partitions plus Z-order.

| | Strategy A | Strategy B |
|---|---|---|
| Layout | `partitionBy(pickup_date)` | unpartitioned |
| Post-write | `OPTIMIZE` (compaction) | `OPTIMIZE ... ZORDER BY (pu_location_id)` |
| Table | `benchmark/bench_taxi_daily` | `benchmark/bench_taxi_zordered` |

## 2. Method

Three things decide whether these numbers mean anything, and the first version of this benchmark got
all three wrong.

Size and file count come from `DeltaTable.forPath(spark, path).detail()`, not from walking the
directory. `mode("overwrite")` leaves superseded files in place until `VACUUM`, so a directory total
measures how many times the script has been run. That total is still reported, separately, as what
is on disk before `VACUUM`.

Query latency is the median of three runs after a discarded warm-up. Measured cold, the first query
took 3.17 s against a 1.07 s steady state, which is larger than the gap between the two strategies
on any query. The strategies are also interleaved rather than run in blocks, so drift over the run
is shared instead of landing on whichever went first.

Environment: Spark 3.5.3, Delta 3.2.1, `local[4]`, 4 GB driver heap, `shuffle.partitions = 8`,
snappy, `optimizeWrite` and `autoCompact` on, `optimize.maxFileSize` at its 1 GiB default.

## 3. Results

| Metric | A: daily partitions | B: unpartitioned + ZORDER |
|---|---|---|
| Ingestion time (write + OPTIMIZE) | 38.02 s | 82.85 s |
| &nbsp;&nbsp;· write | 37.56 s | 25.20 s |
| &nbsp;&nbsp;· OPTIMIZE | 0.46 s | 57.65 s |
| Storage size (live) | 300.6 MB | 359.3 MB |
| Generated files (live) | 91 | 1 |
| On disk before `VACUUM` | 303.2 MB | 724.9 MB |
| Q1 trips per borough | 0.99 s | 0.78 s |
| Q2 average trip duration per day | 0.53 s | 0.49 s |
| Q3 average fare per borough | 0.97 s | 0.79 s |
| Q4 Q1 filtered to one day (diagnostic, §5) | 0.50 s | 0.52 s |

## 4. Discussion

**Partitioning is the cheaper write, Z-ordering the expensive one.** A's write is the slower of the
two, 37.56 s against 25.20 s, from the shuffle that routes each record to its date directory. That
is all of A's cost, because its `OPTIMIZE` finds nothing to compact and returns in 0.46 s, every
partition already being one file. B pays 57.65 s to Z-order, more than twice its own write, since
Z-ordering sorts and rewrites the whole table.

**The partitioned copy is 16% smaller.** 300.6 MB against 359.3 MB, which we did not expect. The
cause is what each file holds. An A file holds one day, so `pickup_ts`, `pickup_hour` and
`pickup_year_month` all have a narrow range inside it and encode tightly. B's Z-order on
`pu_location_id` scatters a whole quarter of timestamps through one file. Clustering for pruning and
clustering for compression are different goals, and here they pull in opposite directions.

**Both file counts are wrong, in opposite ways.** A's 91 files average 3.3 MB, a fortieth of the
128 MB that §5 calls the lower bound of a useful partition, which is the failure that section
predicts for this dataset. B's single file is `optimize.maxFileSize` at its 1 GiB default, which the
whole 359 MB table fits under. Parquet row groups still let Spark read one large file in parallel,
so B's extreme costs nothing at this size.

**B wins all three required queries.** A is 27% slower on Q1, 8% on Q2 and 24% on Q3. None of the
three filters on a date, so A prunes nothing and pays 91 file opens to read the rows B reads from
one. Q2 is the narrowest gap because it touches no lookup table; Q1 and Q3 both join `taxi_zones`.

## 5. When does partitioning improve query latency?

Only when a query filters on the partition key. Pruning is decided against the `WHERE` clause while
Delta builds the file list, so a `GROUP BY` on the partition column prunes nothing: every partition
contributes at least one group, and so every partition is read. Two of the three required queries
look like they should benefit and do not. Q1 groups by borough, which is not the partition key at
all; Q2 groups by `pickup_date`, which is, and still scans all 91 directories.

Q4 measures the case that does work: Q1 with `WHERE pickup_date = DATE '2024-02-15'` added and
nothing else changed.

| | Q1 (no filter) | Q4 (one-day filter) | change |
|---|---|---|---|
| A: daily partitions | 0.99 s | 0.50 s | −49% |
| B: unpartitioned + ZORDER | 0.78 s | 0.52 s | −33% |

Both get faster, because the filter cuts the rows reaching the join either way. A gains more, half
its time against a third, and the extra is the pruning: 1 directory instead of 91, 3.3 MB instead of
300.6 MB. Q4 is also the only query where A is not behind, though even there it only draws level. At
9.55M rows the query is already at about half a second of fixed planning, broadcast and scheduling
cost, and pruning a 300 MB scan does not show up under that floor. The mechanism works; this extract
is too small for it to matter.

## 6. When does partitioning become harmful?

When partitions fall below one target file, which is this dataset at 91 days of 3.3 MB: every scan
pays per-file overhead and every commit adds 91 entries to the transaction log, to skip data that
reads in milliseconds. Also when queries do not filter on the key, which covers all three required
queries and is why A loses all three, since a full scan of a partitioned table is strictly worse
than a full scan of an unpartitioned one, same bytes and more file handles.

A third case is milder, and this run happened to land on its good side. The partition key decides
file contents and file contents decide compression, so a key picked purely for pruning can cost
storage. Here it saved storage instead.

## 7. What this says about the shipped design

§5 predicted A would lose on all four metrics and said this report would record it if the numbers
disagreed. They disagree on two.

| | Predicted | Measured |
|---|---|---|
| Ingestion time | A slower | A faster, 38.02 s against 82.85 s |
| Storage size | A larger | A smaller, 300.6 MB against 359.3 MB |
| File count | A worse | A worse, 91 against 1 |
| Query latency | A slower | A slower on all three, by 8 to 27% |

The conclusion is unchanged. Daily partitioning still loses every query the assignment asks for and
produces 91 files averaging 3.3 MB, which gets worse as more days arrive. What the numbers correct
is the reason. It is not rejected for writing slowly or wasting space, since it does neither, but
because the partitions are two orders of magnitude too small and nothing queries them by date.
`pickup_year_month` keeps the time key at 3 partitions of ~125 MB, and the location filter Z-order
serves is the one queries actually use.

One measurement is worth carrying into operations. B's directory held 724.9 MB for a 359.3 MB table
because `OPTIMIZE` rewrote every file and Delta keeps the originals until `VACUUM`; A's held
303.2 MB for 300.6 MB, because its `OPTIMIZE` rewrote nothing. Any table that gets a real `OPTIMIZE`
should be assumed to occupy twice its size until retention is managed, which is the point
[Storage Architecture §6](storage_architecture.md#6-design-discussion) makes about the 20× case,
reproduced at 1×.
