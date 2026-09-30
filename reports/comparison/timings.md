# Approach A against Approach B: timings

Generated 2026-09-30 22:51. Wall-clock seconds per process, including JVM start-up; median of 3 interleaved repeats after one discarded warm-up run. B runs in its own lakehouse, rebuilt from nothing on every repeat.

| Run | What it does | Median | Samples |
|---|---|---:|---|
| `a` | A: raw files to evaluated model | 391.7 s | 394.9, 391.7, 389.7 |
| `b_ingest` | B: ingest the original files to silver | 174.1 s | 173.8, 175.4, 174.1 |
| `b_update` | B: merge release v2 into silver | 106.2 s | 103.6, 106.8, 106.2 |
| `b_integrate` | B: build the gold table | 180.5 s | 178.0, 180.5, 187.2 |
| `b_ml` | B: gold to evaluated model (`run_ml_train.py --rebuild`) | 627.0 s | 627.1, 627.0, 620.6 |

B platform part (ingest + update + integrate): 460.8 s. B end to end: 1087.8 s. A end to end: 391.7 s.

## Stages inside the model runs

Seconds measured by the scripts themselves, median of the repeats.

| Stage | A | B |
|---|---:|---:|
| training dataset | 86.9 | see the dataset report |
| fit (features and regression) | 155.8 | 343.0 |
| scoring and evaluation | 142.1 | 174.8 |

## Layout of the training split

`run_ml_train.py --rebuild` on the gold table of the last repeat, starting without an ml layer.

| Run | Delta file compaction | Files in split=train | Job median | Samples | Fit | Scoring |
|---|---|---:|---:|---|---:|---:|
| `b_ml_same_gold` | on (platform default) | 1 | 629.3 s | 629.3, 620.9, 631.2 | 345.6 | 171.9 |
| `b_ml_no_optimize_write` | off | 4 | 411.9 s | 411.0, 411.9, 413.6 | 220.3 | 116.0 |
