# Urban Data Integration Platform

A Spark + Delta Lake platform that ingests NYC taxi trips, weather, air quality and taxi zone data,
standardises them into Delta tables that share one set of join keys, and integrates them into a
single analytical table with one row per trip. New releases of the data are merged into the
existing tables without rebuilding them. On top of that table, a configurable machine-learning
pipeline builds training datasets, engineers features and trains, evaluates and retrains models
(see Machine learning).

## Setup

Tested on Windows 11 with Anaconda Python 3.11, JDK 17, Spark 3.5.3 and Delta Lake 3.2.1.

```bash
conda install -c conda-forge openjdk=17     # Spark 3.5 needs Java 11 or 17
pip install -r requirements.txt
```

Set `JAVA_HOME` to the JDK root. For Anaconda that is `<anaconda>\Library\lib\jvm`.

On Windows, also download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x from
[cdarlint/winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin) into
`<dir>\bin`, then set `HADOOP_HOME=<dir>` and add `%HADOOP_HOME%\bin` to `PATH`. PySpark does not
ship these and Spark will not start without them. Linux and macOS do not need this step.

Then download the datasets into `data/raw/`:

```
data/raw/taxi_trips/yellow_tripdata_2024-{01,02,03}.parquet
data/raw/weather/weather.csv
data/raw/air_quality/hourly_88101_2024.csv
data/raw/taxi_zones/taxi_zone_lookup.csv
```

Finally, check that everything works:

```bash
python scripts/check_env.py
```

It verifies Java, the Hadoop helpers, the Python packages, the raw files and a real Delta write, and
prints a fix for the first thing that fails.

On Windows, Spark prints `ERROR ShutdownHookManager: Exception while deleting Spark temp dir` when
it exits. The run has already finished at that point, so ignore it.

## Initial load

```bash
python jobs/run_ingest.py                      # the four datasets into data/lakehouse/silver/
python jobs/run_ingest.py weather taxi_zones   #   or just these
python jobs/run_integrate.py                   # the silver tables joined into gold/integrated_taxi_trips
python jobs/run_products.py                    # the four analytical data products
```

Ingestion reads `data/raw/` and takes a few minutes. Re-running it is safe: the silver tables are
overwritten and the surrogate keys are content hashes, so the same input gives the same result.
`run_integrate.py` produces one row per trip with the weather and air quality at the pickup hour and
the zone and borough of both endpoints. Its config is `conf/integration.yaml`.

Every table loads the same way:

```python
spark.read.format("delta").load("data/lakehouse/gold/integrated_taxi_trips")
```

## Processing incremental updates

An incremental release is a folder of new or re-delivered records under
`data/updates/<release>/<dataset>/`. Release `v2`, taxi trips for 1 to 7 April 2024 plus weather and
air quality for January 2025, is generated from the initial data:

```bash
python scripts/generate_updates.py            # writes data/updates/v2/ and reports/updates/v2.md
python jobs/run_update.py v2                  # merges it into the silver tables
python jobs/run_refresh.py                    # brings gold and the data products up to date
python scripts/verify_incremental.py v2       # checks the merge
python scripts/verify_refresh.py              # checks gold and the products against a rebuild from scratch
```

`run_update.py` runs each dataset through the same pipeline as the initial load and inserts only
the rows whose key is new, so applying a release twice changes nothing. `run_refresh.py` reads the
silver change feeds and merges the new trips into gold, where the ML jobs pick them up (see
Training and retraining). A full rebuild is `run_ingest.py`, `run_integrate.py` and
`run_products.py`, then `run_update.py` and `run_refresh.py` for each release in order.

## Monitoring

Every job appends a record of what it did to a Delta table under `data/lakehouse/_meta/`:
`ingestion_metadata`, `validation_metrics`, `quarantine`, `integration_metadata`,
`data_product_metadata`, and `ml_metadata` for the ML jobs. `run_monitoring.py` answers the
questions in `conf/monitoring.yaml` over these tables and writes the answers to
`reports/monitoring/<timestamp>.md`:

```bash
python jobs/run_monitoring.py                       # every question
python jobs/run_monitoring.py ml_builds ml_models   # what each dataset, feature table and model was built from
python jobs/run_monitoring.py --list                # the questions and the tables they read
```

A new question is an entry in `conf/monitoring.yaml`, with no code change.

## Analytical layer

Twelve Spark SQL queries and four materialised data products from Week 2 read
`gold/integrated_taxi_trips`:

```bash
python jobs/run_queries.py               # run all twelve
python jobs/run_products.py              # rebuild all four products
```

Their bucket edges, weather families and thresholds are in `conf/analytics.yaml`. The ML pipeline
reuses the weather families for its `weather_family` feature.

## Machine learning

Week 4 builds machine-learning datasets on top of gold. A task is one file in `conf/ml/`: its
label, which trips qualify, which gold columns it reads, how they are split, and which features
are built from them. The one that ships, `trip_duration`, predicts a trip's minutes from pickup
to drop-off from what is known when it starts.

```bash
python jobs/run_ml_dataset.py                 # Task 1: gold -> ml/trip_duration_dataset
python jobs/run_ml_features.py                # Task 2: fit the feature pipeline, write ml/trip_duration_features
python scripts/verify_ml.py                   # leakage, split order, reproducibility, the saved pipeline
python jobs/run_ml_train.py                   # Task 3: train, evaluate and save the next model version
python scripts/verify_ml_model.py             # lineage, reloading, a deterministic refit, no test data in the fit
python scripts/analyze_ml_features.py         # which datasets' features carry signal, on a sample (~10 min)
python scripts/analyze_ml_features.py --models lr --train-frac 1.0 --val-frac 1.0 \
       --report trip_duration_feature_analysis_full_lr.md   # the same for the linear model on all data (~20 min)
python jobs/run_ml_dataset.py --list          # the tasks in conf/ml/
```

Each step writes a report to `reports/ml/` and a row to `_meta/ml_metadata`, which
`run_monitoring.py ml_builds` lists.

### The training dataset

`run_ml_dataset.py` reads gold, keeps the trips that pass every filter in the task file, and
writes one row per trip with `trip_id`, `label`, the declared columns and `split`, partitioned by
split. For `trip_duration` that is 9,753,369 of gold's 10,220,529 trips. Each filter is counted
separately in `reports/ml/trip_duration_dataset.md`.

- Columns known only after the trip ends are not carried: fares, tips, tolls, payment type.
  `fare_amount` is metered from the duration itself.
- The split is chronological. Validation and test are the last 14 days each, counted back from
  the newest trip in gold, so after a release they move forward on their own. With release v2
  applied, train is 1 January to 10 March, validation 11 to 24 March and test 25 March to 7 April.
  `validation_start` and `test_start` pin them; `strategy: hash` splits by a hash of the key
  instead.
- The dataset records the gold version it was read from and the fingerprint of its
  configuration, as `ml.*` table properties. `--source-version N` reads gold at an earlier
  version, and `verify_ml.py` rebuilds the dataset from the recorded version and compares.

### The feature pipeline

`run_ml_features.py` turns the `features` section of the task file into one Spark ML Pipeline,
fits it on the training split, applies it to all three, and writes
`ml/trip_duration_features` (`trip_id`, `pickup_date`, `split`, `label` and a 639-slot
`features` vector) and the fitted PipelineModel under `ml/trip_duration_feature_pipeline`.

| Stage | Does |
|---|---|
| generators | one `SQLTransformer` each: `temporal` (local hour, day of week, month, weekend, holiday, hour as sine and cosine), `zone_pair` (same borough, borough pair, airport ends), `code_groups` (Meteostat codes to the weather families of `conf/analytics.yaml`), `expression` |
| prepare | casts; declared missing codes (`passenger_count` 0, `ratecode_id` 99) to null; `log1p`; null categories to `__missing__` |
| missing flags | a 0/1 slot for each numeric feature with nulls in training |
| `GroupImputer` | nulls of a feature with `impute_by`, with the training median of the row's group: precipitation per weather family, since its nulls fall mostly in rain and snow |
| `Imputer` | the remaining numeric nulls, with the training median |
| `StandardScaler` | numerics to zero mean and unit variance |
| `StringIndexer` + `OneHotEncoder` | categories indexed by training frequency; a value first seen later (April, in test) goes to an unknown slot, so scoring does not fail |
| `VectorAssembler` | one `features` vector |
| `SelectColumns` | drops every intermediate and raw column |

Every statistic is learnt from the training split. The saved model carries its SQL, so it
scores new trips with no labels and nothing from `conf/`:

```python
from pyspark.ml import PipelineModel
PipelineModel.load("data/lakehouse/ml/trip_duration_feature_pipeline").transform(trips)
```

Loading it needs `src/` on the path, for the `SelectColumns` and `GroupImputer` stages.
`feature_manifest.json` beside the stages names the feature and source dataset of every slot.
`reports/ml/trip_duration_features.md` lists what was done to each feature, how many values were
imputed and how many categories were unseen per split.

### Training and retraining

`run_ml_train.py` reads the `model` section of the task file, appends that estimator to the
feature stages above, and fits the whole thing as one Spark ML Pipeline on the training split.
For `trip_duration` the estimator is a linear regression with a ridge penalty of 0.01. The job
scores all three splits, compares them with a model that predicts the training mean, and saves
the fitted pipeline as the next version:

```
data/lakehouse/ml/models/trip_duration_model/
  latest.json                 {"version": N}, the version load_model returns
  v1/ v2/ ...                 pipeline/ (the PipelineModel), model.json, feature_manifest.json
```

`model.json` records the dataset version and the gold version behind it, the split dates, the
configuration fingerprint, every metric and the coefficients per feature. Because training reads
the dataset at that Delta version, `--dataset-version N` retrains any earlier model exactly.
`reports/ml/trip_duration_model.md` has the evaluation, the test error per pickup borough and
the coefficients. `run_monitoring.py ml_models` lists every version with its metrics.

```python
from urbanplatform.config import PlatformConfig
from urbanplatform.ml import MLTaskConfig
from urbanplatform.ml.model import load_model

model, card = load_model(PlatformConfig.load(), MLTaskConfig.load("trip_duration"))  # or version=N
model.transform(trips)            # rows shaped like the training dataset; the label may be missing
```

New data reaches the model in one command, once `run_update.py` and `run_refresh.py` have brought
gold up to date:

```bash
python jobs/run_ml_train.py --rebuild             # dataset from the newest gold, then the next version
python jobs/run_ml_train.py --rebuild --source-version N   # or from gold as it was at version N
```

The split windows move forward with the new data. Earlier versions stay where they are, and the
report scores the previous version on the new test split next to the new one. The model does not
read the Task 2 feature table, so `run_ml_features.py` is only needed afterwards to bring that
table level with the new dataset, which `verify_ml.py` checks. For release v2,
version 1 was trained with `--rebuild --source-version N`, where N is gold's last version before
`run_refresh.py` applied the release (3 for us; `DESCRIBE HISTORY` on the gold table shows it),
with its test split from 19 March. Version 2 was trained with `--rebuild` after the release, with
its test split from 25 March. On version 2's test split,
version 1 has an RMSE of 6.61 minutes and version 2 6.59; predicting the training mean gives 13.14.

### Adding a feature, a dataset or a task

- A feature from a column the dataset has is one line under `numeric`, `categorical` or `binary`.
- A feature from a gold column the dataset lacks also goes under `dataset.columns`, and the
  dataset is rebuilt.
- A derived feature is an `expression` entry, or a generator for a family of them. A generator
  is a function registered with `@generator` in `src/urbanplatform/ml/generators.py`.
- A new dataset is declared in `conf/datasets/<name>.yaml` (source, schema, renames, quality rules)
  and integrated into gold through `context` in `conf/integration.yaml`. From there its columns
  are gold columns like any other.
- Another prediction task is a new file in `conf/ml/`, run by name with the same jobs:
  `python jobs/run_ml_dataset.py <task>`.
- Another model is a function registered with `@estimator` in `src/urbanplatform/ml/model.py` and
  named in `model.type`. The registry records its task type, which picks the evaluator and the
  metrics; a classification task adds a classifier and an entry for its evaluator.
- `encoding: index` and `scaling: none` suit tree models. A model sets them for itself under
  `model.features`, and `analyze_ml_features.py` switches to them for its random forest, so
  neither needs a second pipeline.

### Comparing with a pipeline built from the raw files

Week 4 Task 4 builds the same model without the platform. `scripts/approach_a_from_raw.py` reads
the raw files and the release v2 taxi file, and does the cleaning, the joins, the filters, the
split, the feature stages and the regression itself, using no code from `src/urbanplatform` and
no file from `conf/`. Every rule in it copies one the platform already applies. It needs release
v2 on disk, so run `scripts/generate_updates.py` first.

```bash
python scripts/approach_a_from_raw.py             # raw files -> data/approach_a/, reports/comparison/approach_a.json
python scripts/compare_approaches.py              # the same trips, columns, metrics and predictions as the platform
python scripts/run_comparison.py                  # the timings in the evaluation report (about 2.5 hours)
```

`compare_approaches.py` needs the platform's dataset and model first (`run_ml_train.py`). With
`--without-update`, A leaves out release v2, and `compare_approaches.py --dataset-version N
--model-version 1 --report reports/comparison/approach_a_without_update.json` compares it with
the model trained before the release, where N is the dataset version in that model's
`model.json` (3 after the commands above).

`run_comparison.py` runs A and the platform three times each, interleaved, after a discarded
warm-up of A. The platform runs in its own lakehouse under `data/approach_b/`, rebuilt from
nothing each time (`run_ingest.py`, `run_update.py v2`, `run_integrate.py`,
`run_ml_train.py --rebuild`), so `data/lakehouse` is left alone and `reports/ml/` is restored
afterwards. It then runs both comparisons, and times the ML job three more times with Delta's
optimizeWrite and autoCompact switched off, which writes the training split as four files instead
of one. The results are in `reports/comparison/timings.md`; `--resume` continues an interrupted
run.

## Project layout

```
DesignReportWeek4.pdf         the Week 4 design report (DesignReportWeek4.docx is its source)
EvaluationReportWeek4.pdf     the Week 4 evaluation report (EvaluationReportWeek4.docx is its source)
README.md                     this file
conf/
  platform.yaml               Spark settings and the lakehouse root
  datasets/*.yaml             one file per dataset: source, schema, renames, timestamps, quality rules, updates
  integration.yaml            how the integration joins the four tables
  analytics.yaml              bucket edges, weather families and thresholds for the analytical queries
  products.yaml               the analytical data products: grain, schema version, partitioning
  ml/*.yaml                   one file per ML task: label, filters, columns, split, features, model
  monitoring.yaml             the operational questions the platform answers about itself
src/urbanplatform/            the ingestion framework, the integration pipeline and the analytical layer
  ingest.py                   one pipeline that runs every dataset, for the initial load and for updates
  writer.py                   Delta writes: overwrite for a full load, merge for an update
  quality.py                  the validation engine: applies the rules, splits and reports
  rules.py                    the rule type registry and the context a rule may reach beyond its frame
  rules_builtin.py            the eight rule types that ship with the platform
  metadata.py                 the run, validation and quarantine tables
  monitoring.py               views over the _meta tables, and the query runner behind run_monitoring.py
  integrate.py                builds gold/integrated_taxi_trips and refreshes it from the silver change feeds
  transforms/                 the only dataset-specific code
  analytics/                  the analytical queries, the data products and the benchmark harness
  ml/                         training datasets, feature generators, the feature pipeline and the model
jobs/                         one entry point per step (see Scripts)
scripts/                      the environment check, the update generator, the correctness checks and the Task 4 comparison
docs/common_data_model.md     the common data model, with every column transformation listed
reports/query_benchmark/      written by run_query_benchmark.py
reports/updates/              written by generate_updates.py and verify_incremental.py
reports/monitoring/           written by run_monitoring.py, one file per run
reports/evaluation/           written by run_evaluation.py: measurements.md and runs.json
reports/ml/                   written by run_ml_dataset.py, run_ml_features.py, run_ml_train.py and analyze_ml_features.py
reports/comparison/           written by approach_a_from_raw.py, compare_approaches.py and run_comparison.py
data/raw/                     source files, not included (see Setup)
data/updates/                 incremental releases, written by generate_updates.py, not included
data/snapshots/               lakehouse copies run_evaluation.py restores before each run, not included
data/approach_a/              the dataset, model and logs of approach_a_from_raw.py and run_comparison.py, not included
data/approach_b/              the platform's own lakehouse during run_comparison.py, not included
data/lakehouse/               created by the jobs
  silver/                     one Delta table per dataset
  gold/                       integrated_taxi_trips
  benchmark/                  the two storage variants the Week 1 benchmark compares
  products/                   the analytical data products
  ml/                         training datasets, feature tables and fitted feature pipelines
    models/<model>/v<N>/      one trained model per version: pipeline/, model.json, feature_manifest.json
  _meta/                      ingestion_metadata, validation_metrics, quarantine, integration_metadata, data_product_metadata, ml_metadata
```

## Scripts

| Script | What it does | Needs Spark |
|---|---|---|
| `scripts/check_env.py` | verifies the whole stack end to end | yes |
| `jobs/run_ingest.py` | loads the raw files into the silver tables | yes |
| `jobs/run_integrate.py` | joins the silver tables into `gold/integrated_taxi_trips` | yes |
| `jobs/run_products.py` | rebuilds the analytical data products and records their metadata | yes |
| `jobs/run_queries.py` | runs the analytical query library | yes |
| `scripts/generate_updates.py` | generates an incremental release from the raw files, with its manifest | no |
| `jobs/run_update.py` | merges an incremental release into the silver tables | yes |
| `jobs/run_refresh.py` | brings gold and the data products up to date after an update | yes |
| `jobs/run_monitoring.py` | answers the operational questions in `conf/monitoring.yaml` and writes a report | yes |
| `scripts/verify_refresh.py` | checks gold and the products against a rebuild from scratch | yes |
| `scripts/verify_incremental.py` | checks an applied release against the table before it and the manifest | yes |
| `scripts/verify_analytics.py` | checks the analytical queries' correctness invariants | yes |
| `scripts/verify_products.py` | checks the data products against their sources, and their metadata | yes |
| `jobs/run_benchmark.py` | Week 1: rewrites the fact table two ways and measures both | yes |
| `jobs/run_query_benchmark.py` | Week 2: runs the query optimisation experiments | yes |
| `jobs/run_evaluation.py` | Week 3: runs the evaluation experiments from snapshots and measures storage | yes |
| `jobs/run_ml_dataset.py` | Week 4: builds a task's training dataset from gold | yes |
| `jobs/run_ml_features.py` | Week 4: fits the feature pipeline and writes the feature table | yes |
| `jobs/run_ml_train.py` | Week 4: trains, evaluates and saves the next model version; `--rebuild` retrains on the newest gold | yes |
| `scripts/verify_ml.py` | checks the training dataset and feature table: leakage, splits, reproducibility | yes |
| `scripts/verify_ml_model.py` | checks the saved models: lineage, reloading, a deterministic refit, no test data in the fit | yes |
| `scripts/analyze_ml_features.py` | ablation and importance of each dataset's features | yes |
| `scripts/approach_a_from_raw.py` | Week 4 Task 4: the same model built from the raw files without the platform | yes |
| `scripts/compare_approaches.py` | checks Approach A's dataset and model against the platform's, row by row | yes |
| `scripts/run_comparison.py` | times both approaches and runs the checks | yes |
| `scripts/profile_raw.py` | profiles the raw files and produces every number quoted in the data catalog | no |
