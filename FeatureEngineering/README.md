# Feature Engineering Benchmarks

This suite compares feature engineering implementations across Rodeo, PolarsFE, and direct engine idioms. It is separate from the dataframe operation benchmarks and intentionally excludes Spark for this phase.

## Insights Report

See [Feature Engineering Performance and Best Practices Insights](reports/feature_engineering_performance_insights.md) for the current modernization summary, benchmark findings, implementation recommendations, and stability lessons.

## Scope

Benchmarked families:

- Numeric transforms
- Categorical encodings
- Calendar/date features
- Text features
- Missingness indicators
- Interactions
- Combined feature plans
- Model prep partitions and folds

## Engines

R:

- Rodeo vNext
- Direct data.table

Python:

- PolarsFE vNext
- Direct Polars eager
- Direct Polars lazy where practical
- pandas where installed

DuckDB hooks can be added for SQL-friendly workloads later. Spark is out of scope.

## Scripts

```powershell
Rscript FeatureEngineering/run_feature_engineering_smoke.R
python FeatureEngineering/run_feature_engineering_smoke.py
Rscript FeatureEngineering/summarize_feature_engineering_results.R
```

Moderate benchmark matrix:

```powershell
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$env:FEATURE_ENGINEERING_BENCHMARK_OUTPUT = "C:\Users\Bizon\Documents\GitHub\Benchmarks\FeatureEngineering\outputs\moderate_$stamp"
Rscript FeatureEngineering/run_feature_engineering_moderate.R
python FeatureEngineering/run_feature_engineering_moderate.py
Rscript FeatureEngineering/summarize_feature_engineering_results.R
```

Each script writes to `FeatureEngineering/outputs/<timestamp>/` unless `FEATURE_ENGINEERING_BENCHMARK_OUTPUT` is set.

The moderate runner covers:

- Rows: 10k, 100k, 500k by default.
- Cardinality: low = 5, medium = 50, high = 500.
- Shapes: narrow, medium, wide.
- Families: numeric, categorical, calendar, text, missingness, interactions, combined plans.

Large benchmark matrix:

```powershell
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$env:FEATURE_ENGINEERING_BENCHMARK_OUTPUT = "C:\Users\Bizon\Documents\GitHub\Benchmarks\FeatureEngineering\outputs\large_$stamp"
$env:FE_BENCH_ROWS = "1000000,5000000,10000000"
$env:FE_BENCH_RAM_FRACTION = "0.5"
$env:FE_BENCH_REPEATS = "1"
$env:FE_BENCH_RESUME = "true"
Rscript FeatureEngineering/run_feature_engineering_large.R
python FeatureEngineering/run_feature_engineering_large.py
Rscript FeatureEngineering/summarize_feature_engineering_results.R
```

The large runners are checkpointed and resume-safe. They support 25M, 50M, and 100M flags, but 100M is disabled unless `FE_BENCH_ENABLE_100M=true`. See `docs/feature_engineering_large_benchmark_guide.md` for conservative, aggressive, and R-only 100M command templates.

Python large benchmark safety:

- Python defaults to a 1M row cap.
- Python medium/wide shapes are skipped unless `FE_BENCH_ALLOW_PYTHON_MEDIUM_WIDE=true`.
- Python rows above 1M require `FE_BENCH_ALLOW_PYTHON_LARGE=true`.
- Python rows at or above 10M require `FE_BENCH_ALLOW_PYTHON_10M=true`.
- Each executable Python case runs in a subprocess with `FE_BENCH_CASE_TIMEOUT_MINUTES`.
- Native crashes are recorded as case-level errors when possible instead of killing the whole matrix.

Model-prep smoke coverage:

- random, stratified, grouped, time, and fold assignment contracts live in Rodeo vNext and PolarsFE vNext.
- The smoke scripts include a `model_prep` family with stratified partition/fold assignment.
- Large Python model-prep benchmarks remain opt-in through the same Python large-run guardrails.

Guardrails:

- `FE_BENCH_MAX_GENERATED_CELLS` defaults to `80000000`.
- Cases exceeding the guardrail are recorded as skipped with reason `generated-cell guardrail`.
- `FE_BENCH_ROWS` can override the row grid, for example `10000,100000`.
- Large runs also use memory and engine row limits: `FE_BENCH_MAX_RAM_GB`, `FE_BENCH_RAM_FRACTION`, `FE_BENCH_R_MAX_ROWS`, `FE_BENCH_POLARS_MAX_ROWS`, `FE_BENCH_PANDAS_MAX_ROWS`, and `FE_BENCH_DUCKDB_MAX_ROWS`.
- Optional matrix filters are available: `FE_BENCH_SHAPES`, `FE_BENCH_CARDINALITY`, `FE_BENCH_FAMILIES`, and `FE_BENCH_ENGINES`.

## Outputs

- `r_feature_engineering_summary.csv`
- `python_feature_engineering_summary.csv`
- `r_feature_engineering_moderate_summary.csv`
- `python_feature_engineering_moderate_summary.csv`
- `feature_engineering_summary.md`
- `decision_table.csv`
- `implementation_decision_table.csv`
- `fastest_by_family.csv`
- `fastest_by_rows.csv`
- `fastest_by_cardinality.csv`
- `fastest_by_shape.csv`
- `engine_family_medians.csv`
- `status_counts.csv`
- `r_feature_engineering_large_summary.csv`
- `python_feature_engineering_large_summary.csv`
- `r_large_checkpoint.csv`
- `python_large_checkpoint.csv`
- `r_large_skips_failures.csv`
- `python_large_skips_failures.csv`
- `scalability_curves.csv`
- `memory_skip_reasons.csv`
- `large_data_decision_table.csv`
- `session_info.txt`
- `system_info.txt`

The `outputs/` directory is ignored to avoid committing large results.

## Current Moderate Checkpoint

Latest local moderate run:

`FeatureEngineering/outputs/moderate_20260705_205740`

This run produced 2,079 result rows, 1,233 successful benchmark rows, 846 intentional skips, and 0 hard failures. Skips were dependency/coverage/guardrail driven: missing `duckdb`, missing `pyarrow` for pandas conversion, base-vectorized coverage limits, collapse numeric-only coverage, generated-cell guardrails, and no directly comparable legacy function for some families.
