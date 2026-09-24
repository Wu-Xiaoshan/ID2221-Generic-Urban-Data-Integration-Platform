# Release `v2`: verification

Written by `scripts/verify_incremental.py v2` after `jobs/run_update.py v2`.
31 checks passed, 0 failed.

## Measured outcome

| Dataset | Rows before | Inserted | Duplicates ignored | Rejected | Rows after | New columns | Runs of the release |
|---|---:|---:|---:|---:|---:|---|---:|
| taxi_trips | 9,551,909 | 668,620 | 10,185 | 214 | 10,220,529 | none | 2 |
| weather | 8,784 | 744 | 0 | 0 | 9,528 | humidity | 2 |
| air_quality | 8,784 | 744 | 0 | 0 | 9,528 | aqi | 2 |

## Checks

| Dataset | Check | Result | Detail |
|---|---|---|---|
| taxi_trips | applied | pass | 1 merge commit(s) in the table history |
| taxi_trips | counts | pass | 9,551,909 before + 668,620 inserted = 10,220,529, table has 10,220,529 |
| taxi_trips | unique key | pass | 0 key value(s) appear more than once |
| taxi_trips | preserved | pass | 0 of 9,551,909 existing rows missing or changed |
| taxi_trips | new columns | pass | table gained none, manifest declares none |
| taxi_trips | expected inserted | pass | first merge inserted 668,620, manifest expects 668,620 |
| taxi_trips | expected duplicates | pass | first merge ignored 10,185, manifest expects 10,185 |
| taxi_trips | expected rejected | pass | first run rejected 214, manifest expects 214 |
| taxi_trips | idempotent | pass | 1 later run(s) of v2 inserted 0 rows |
| weather | applied | pass | 1 merge commit(s) in the table history |
| weather | counts | pass | 8,784 before + 744 inserted = 9,528, table has 9,528 |
| weather | unique key | pass | 0 key value(s) appear more than once |
| weather | preserved | pass | 0 of 8,784 existing rows missing or changed |
| weather | new columns | pass | table gained ['humidity'], manifest declares ['humidity'] |
| weather | humidity on existing rows | pass | 0 existing row(s) have a value; all should be null |
| weather | humidity on new rows | pass | 744 of 744 set, values 25.2 to 97.1, allowed 20.0 to 100.0 |
| weather | expected inserted | pass | first merge inserted 744, manifest expects 744 |
| weather | expected duplicates | pass | first merge ignored 0, manifest expects 0 |
| weather | expected rejected | pass | first run rejected 0, manifest expects 0 |
| weather | idempotent | pass | 1 later run(s) of v2 inserted 0 rows |
| air_quality | applied | pass | 1 merge commit(s) in the table history |
| air_quality | counts | pass | 8,784 before + 744 inserted = 9,528, table has 9,528 |
| air_quality | unique key | pass | 0 key value(s) appear more than once |
| air_quality | preserved | pass | 0 of 8,784 existing rows missing or changed |
| air_quality | new columns | pass | table gained ['aqi'], manifest declares ['aqi'] |
| air_quality | aqi on existing rows | pass | 0 existing row(s) have a value; all should be null |
| air_quality | aqi on new rows | pass | 744 of 744 set, values 12 to 109, allowed 0 to 500 |
| air_quality | expected inserted | pass | first merge inserted 744, manifest expects 744 |
| air_quality | expected duplicates | pass | first merge ignored 0, manifest expects 0 |
| air_quality | expected rejected | pass | first run rejected 0, manifest expects 0 |
| air_quality | idempotent | pass | 1 later run(s) of v2 inserted 0 rows |
