# Feature Engineering Large Benchmark Guide

Large FeatureEngineering benchmarks are designed for overnight and checkpointed runs. They compare vNext orchestration layers against direct engine hot paths while keeping memory, generated-cell counts, and optional engines guarded.

## Default Scope

Default row scales:

- 1,000,000
- 5,000,000
- 10,000,000

Optional row scales:

- 25,000,000 with `FE_BENCH_ENABLE_25M=true`
- 50,000,000 with `FE_BENCH_ENABLE_50M=true`
- 100,000,000 with `FE_BENCH_ENABLE_100M=true`

The 100M scale is disabled unless explicitly enabled. Do not run 100M as a mixed R/Python benchmark by default.

## Guardrail Environment Variables

General:

- `FEATURE_ENGINEERING_BENCHMARK_OUTPUT`: output folder.
- `FE_BENCH_ROWS`: comma-separated row grid.
- `FE_BENCH_REPEATS`: repeats per case, default `1`.
- `FE_BENCH_RESUME`: resume completed cases, default `true`.
- `FE_BENCH_MAX_RAM_GB`: physical or practical memory cap, default `236`.
- `FE_BENCH_RAM_FRACTION`: fraction of the cap available to a case, default `0.6`.
- `FE_BENCH_MAX_GENERATED_CELLS`: generated cell guardrail, default `250000000`.

Matrix filters:

- `FE_BENCH_SHAPES`: comma-separated subset of `narrow,medium,wide`.
- `FE_BENCH_CARDINALITY`: comma-separated subset of `low,medium,high`.
- `FE_BENCH_FAMILIES`: comma-separated subset of `numeric,categorical,calendar,missingness,interactions,combined_plan,text`.
- `FE_BENCH_ENGINES`: comma-separated engine subset.

Engine row limits:

- `FE_BENCH_R_MAX_ROWS`: R runner row cap, default `50000000`.
- `FE_BENCH_POLARS_MAX_ROWS`: Polars row cap, default `1000000`.
- `FE_BENCH_PANDAS_MAX_ROWS`: pandas row cap, default `1000000`.
- `FE_BENCH_DUCKDB_MAX_ROWS`: DuckDB row cap, default `1000000`.

Python-specific hard safety limits:

- `FE_BENCH_ALLOW_PYTHON_LARGE`: required for any Python case above 1M rows, default `false`.
- `FE_BENCH_ALLOW_PYTHON_10M`: required for any Python case at or above 10M rows, default `false`.
- `FE_BENCH_ALLOW_PYTHON_MEDIUM_WIDE`: required for medium/wide Python shapes, default `false`.
- `FE_BENCH_CASE_TIMEOUT_MINUTES`: per-case subprocess timeout, default `10`.

Every executable Python benchmark case runs in a subprocess. The parent writes a result row after each case, records subprocess exits/timeouts as case-level errors, and continues the matrix when possible.

Skip reasons are written into the summary and skip/failure logs:

- `predicted_memory_exceeds_cap`
- `engine_row_limit`
- `generated_cell_guardrail`
- `dependency_missing`
- `user_disabled_100m`
- `text_large_disabled_by_default`
- `python_large_disabled`
- `python_10m_disabled`
- `python_medium_wide_disabled`
- `case_timeout_seconds:<seconds>`

## Outputs

Large runners write incrementally:

- `r_feature_engineering_large_summary.csv`
- `python_feature_engineering_large_summary.csv`
- `r_large_checkpoint.csv`
- `python_large_checkpoint.csv`
- `r_large_skips_failures.csv`
- `python_large_skips_failures.csv`
- `large_benchmark_config.csv`
- `large_benchmark_config_python.json`
- `session_info.txt`
- `system_info.txt`

The summary script also writes:

- `feature_engineering_summary_all.csv`
- `scalability_curves.csv`
- `memory_skip_reasons.csv`
- `large_data_decision_table.csv`
- `feature_engineering_summary.md`

## Conservative Overnight Run

```powershell
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$env:FEATURE_ENGINEERING_BENCHMARK_OUTPUT = "C:\Users\Bizon\Documents\GitHub\Benchmarks\FeatureEngineering\outputs\large_$stamp"
$env:FE_BENCH_ROWS = "1000000,5000000,10000000"
$env:FE_BENCH_RAM_FRACTION = "0.5"
$env:FE_BENCH_REPEATS = "1"
$env:FE_BENCH_RESUME = "true"
Rscript FeatureEngineering\run_feature_engineering_large.R
python FeatureEngineering\run_feature_engineering_large.py
Rscript FeatureEngineering\summarize_feature_engineering_results.R
```

The Python command above is intentionally conservative by default: it runs only 1M-row narrow Python cases unless Python large/shape flags are explicitly enabled.

## Aggressive 25M/50M Run

```powershell
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$env:FEATURE_ENGINEERING_BENCHMARK_OUTPUT = "C:\Users\Bizon\Documents\GitHub\Benchmarks\FeatureEngineering\outputs\large_aggressive_$stamp"
$env:FE_BENCH_ROWS = "1000000,5000000,10000000"
$env:FE_BENCH_ENABLE_25M = "true"
$env:FE_BENCH_ENABLE_50M = "true"
$env:FE_BENCH_RAM_FRACTION = "0.7"
$env:FE_BENCH_PANDAS_MAX_ROWS = "1000000"
$env:FE_BENCH_DUCKDB_MAX_ROWS = "1000000"
$env:FE_BENCH_REPEATS = "1"
$env:FE_BENCH_RESUME = "true"
Rscript FeatureEngineering\run_feature_engineering_large.R
Rscript FeatureEngineering\summarize_feature_engineering_results.R
```

Run Python large probes separately after the R run, with narrow scope and explicit opt-in:

```powershell
$env:FE_BENCH_ROWS = "1000000,5000000"
$env:FE_BENCH_SHAPES = "narrow"
$env:FE_BENCH_ENGINES = "polarsfe_vnext,polars_eager,polars_lazy"
$env:FE_BENCH_ALLOW_PYTHON_LARGE = "true"
$env:FE_BENCH_POLARS_MAX_ROWS = "5000000"
$env:FE_BENCH_CASE_TIMEOUT_MINUTES = "10"
python FeatureEngineering\run_feature_engineering_large.py
Rscript FeatureEngineering\summarize_feature_engineering_results.R
```

## 100M R-Only Probe

Use this only as a targeted run. Keep Python engines disabled unless there is a specific reason to test them.

```powershell
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$env:FEATURE_ENGINEERING_BENCHMARK_OUTPUT = "C:\Users\Bizon\Documents\GitHub\Benchmarks\FeatureEngineering\outputs\large_100m_r_$stamp"
$env:FE_BENCH_ROWS = "100000000"
$env:FE_BENCH_ENABLE_100M = "true"
$env:FE_BENCH_R_MAX_ROWS = "100000000"
$env:FE_BENCH_SHAPES = "narrow"
$env:FE_BENCH_CARDINALITY = "low"
$env:FE_BENCH_FAMILIES = "numeric,missingness,calendar"
$env:FE_BENCH_REPEATS = "1"
Rscript FeatureEngineering\run_feature_engineering_large.R
Rscript FeatureEngineering\summarize_feature_engineering_results.R
```

## Notes

- Spark remains skipped.
- Model-Based Features remain deferred.
- DuckDB is guarded and currently placeholder-only in the Python large runner until SQL cases are implemented.
- pandas is limited by optional dependency availability and should remain capped unless the benchmark environment is prepared for large conversions.
- Python large benchmarks have native-crash risk at high row counts and wider shapes. Keep them narrow, subprocess-isolated, and explicitly opted in.
- Do not convert these results into hard-coded Rodeo or PolarsFE thresholds until the large matrix has repeated evidence across row scales and data shapes.
