# Feature analysis: trip_duration

Probe models on a hash sample of 6,797,379 training rows (100% of train),
judged on 1,551,915 validation rows (100% of validation). Predicting the training
mean (15.21 min) gives RMSE 12.877, MAE 8.719.

- `lr`: linear regression (ridge 0.01) on the pipeline as configured: one-hot, standardised.
- `rf`: random forest, 30 trees of depth 10, on the same pipeline with `encoding: index` and
  `scaling: none`, switched in the configuration object only.

## Ablation by source dataset

| model | features from | features | slots | RMSE | MAE | R² | RMSE change vs all | time |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| lr | all datasets | 29 | 639 | 6.158 | 3.977 | 0.7702 | - | 361 s |
| lr | without weather | 21 | 625 | 6.159 | 3.969 | 0.7701 | +0.001 | 142 s |
| lr | without air_quality | 27 | 637 | 6.156 | 3.963 | 0.7703 | -0.002 | 342 s |
| lr | without taxi_zones | 23 | 588 | 6.212 | 3.999 | 0.7661 | +0.054 | 231 s |
| lr | taxi_trips only | 13 | 572 | 6.214 | 3.988 | 0.7660 | +0.056 | 107 s |
