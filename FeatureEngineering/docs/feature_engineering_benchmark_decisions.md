# Feature Engineering Benchmark Decisions

## Model Prep vNext

Rodeo and PolarsFE now include additive vNext model-prep contracts for random, stratified, grouped, and time partitions plus fold assignment. Benchmark coverage starts at smoke scale only.

Current implementation guidance:

- Keep partition/fold contracts separate from feature transformation contracts.
- Treat row IDs, partition manifests, fold manifests, diagnostics, and warnings as first-class outputs.
- Do not add model training, target encoding, WOE, credibility encoding, or model-based features to this layer.
- Do not run large Python model-prep benchmarks without the explicit Python large-run guardrails.

Checkpoint output:

`FeatureEngineering/outputs/moderate_20260705_205740`

## Run Coverage

- Result rows: 2,079
- Successful benchmark rows: 1,233
- Skipped rows: 846
- Hard failures: 0
- Spark: skipped by design
- Model-Based Features: skipped by design

Intentional skips:

- `duckdb` package not installed.
- pandas path requires `pyarrow` for Polars-to-pandas conversion in this environment.
- base-vectorized runner only covers numeric and missingness.
- collapse runner only covers numeric.
- generated-cell guardrail skipped very large generated-output combinations.
- Rodeo legacy comparisons were limited to numeric, categorical, and calendar where directly comparable.

## Top Findings

1. Direct engine paths are still the speed reference.
   - `data.table::set()` / `:=` remain the right benchmark target for Rodeo internals.
   - Direct Polars eager/lazy expressions are generally faster than PolarsFE vNext orchestration for isolated family operations.

2. vNext layers are useful orchestration layers, not yet optimized kernels.
   - Rodeo vNext and PolarsFE vNext provide fit/transform specs, diagnostics, manifests, and artifact generation.
   - Direct implementation paths should be selectively pulled into vNext internals after validating behavior.

3. Legacy Rodeo remains a valid baseline.
   - Legacy numeric/categorical/calendar functions ran without hard failures in the moderate runner.
   - Do not delete or bypass legacy APIs; use them as behavior/performance comparators.

4. Python dependency gaps affect coverage.
   - pandas benchmark rows were skipped because `pyarrow` is missing.
   - DuckDB benchmark rows were skipped because `duckdb` is missing.
   - These should be installed before publishing cross-engine results.

5. The generated-cell guardrail is necessary.
   - Wide + high-cardinality + 500k row combinations can generate extremely large outputs.
   - The benchmark suite records skipped combinations rather than risking an unbounded local run.

## Rodeo Recommendations

| Area | Recommendation | Evidence / rationale |
|---|---|---|
| Numeric transforms | Keep vNext safety/spec behavior, but continue benchmarking direct `set()`, batch assignment, `:=`, collapse, and base vectorized paths. | Focused Rodeo run showed direct `set()` still winning median numeric time; vNext keeps extra train/scoring and invalid-value safety work. |
| Categorical encoding | Keep vNext scoring-safe top-N/rare/unseen spec; use batch assignment where it wins and direct `set()` where wide/cardinality shapes favor it. | Focused run showed batch winning narrow categorical cases and `set()` winning wide categorical cases. |
| Calendar features | Keep deterministic vNext calendar spec; compare against legacy `CreateCalendarVariables()` and direct date extraction before optimizing. | Legacy calendar is directly comparable and should stay in the benchmark matrix. |
| Text features | Use precomputed vectors and family-level batch assignment for lightweight counts; continue comparing against direct `set()`. | Text is simple enough that column-growth overhead can dominate. |
| Missingness | Use precomputed vectors and batch assignment, while keeping direct `set()` in the benchmark matrix. | Missingness is a low-complexity hot path. |
| Interactions | Keep vNext caps and manifest; optimized vNext is currently competitive. | Focused run showed `Rodeo vNext optimized` winning interaction cases because the fitted capped spec avoids extra direct-run setup. |
| Combined plans | Keep vNext as the user-facing plan/spec API; batch family outputs underneath, but keep direct `set()` and batch competitors in benchmarks. | Focused run showed direct `set()` and batch alternating wins by shape/cardinality, with optimized vNext close behind. |
| Legacy APIs | Keep as baselines. | Legacy functions are stable user-facing behavior and useful performance comparators. |

## Rodeo Focused Optimization Checkpoint

Latest focused run:

`FeatureEngineering/outputs/rodeo_focused_20260705_212757`

- Rows: 168
- Successful rows: 168
- Hard failures: 0

Median results by engine/family:

| Family | Fastest median engine | Notes |
|---|---|---|
| Numeric | `data.table set` | vNext preserves safety/spec behavior and remains slower than direct kernel. |
| Categorical | `data.table set` / `data.table batch` depending on shape | Batch won narrow cases; direct set won wide cases. |
| Calendar | `data.table set` | Direct date extraction still fastest. |
| Interactions | `Rodeo vNext optimized` | Capped fitted spec is paying off. |
| Combined plan | `data.table set` / `data.table batch` | Direct kernels still edge out vNext, but vNext remains close while providing manifest/spec behavior. |

Important caveat: do not blindly replace all vNext internals with `data.table::set()`. `set()` can be excellent for repeated vector assignment, but for large/wide generated outputs batch assignment, precomputed vectors, `:=`, grouped `:=`, collapse, or base vectorized paths may win. Keep thresholds provisional in benchmark docs until repeated evidence supports adaptive runtime choices.

## PolarsFE Recommendations

| Area | Recommendation | Evidence / rationale |
|---|---|---|
| Numeric transforms | Prefer direct Polars expressions inside vNext; test eager vs lazy by plan size. | Direct Polars eager/lazy rows are faster than orchestration for isolated numeric transforms. |
| Categorical encoding | Keep vNext scoring spec, but reduce repeated `unique`/loop overhead where possible. | PolarsFE vNext categorical is useful but slower than direct Polars in the moderate run. |
| Calendar features | Prefer direct Polars expressions; eager/lazy are both viable. | Calendar extraction is expression-native in Polars. |
| Text features | Prefer direct Polars string expressions; keep vNext spec for scoring consistency. | Text features are expression-native and should not need Python loops. |
| Missingness | Use direct Polars expressions. | Missingness is a simple expression list. |
| Interactions | Keep caps and specs; direct expressions should be generated in one `with_columns` call. | Avoid repeated dataframe materialization. |
| Combined plans | Explore lazy execution when many families are chained. | Lazy can optimize chained expression plans, but eager remains competitive for small/narrow shapes. |
| pandas / DuckDB | Add optional dependencies only to the benchmark environment, not PolarsFE runtime. | Missing dependencies limited benchmark coverage; package runtime should stay lean. |

## Do Not Hard-Code Yet

Do not hard-code thresholds into Rodeo or PolarsFE from this run alone. The moderate run is strong enough to suggest internal optimization targets, but publishable thresholds need:

- Higher-resolution repeated timings.
- Dependency-complete Python environment.
- A larger overnight run for selected safe combinations.
- Memory profiling if output size becomes the limiting factor.

## Large Benchmark Readiness

Large-data runners are now available for checkpointed overnight runs:

- `FeatureEngineering/run_feature_engineering_large.R`
- `FeatureEngineering/run_feature_engineering_large.py`

Default row scales are 1M, 5M, and 10M. Optional 25M and 50M runs are enabled with flags. The 100M scale is disabled unless explicitly enabled and should usually be run as a targeted R-only probe.

Large-run guardrails include:

- predicted memory cap via `FE_BENCH_MAX_RAM_GB` and `FE_BENCH_RAM_FRACTION`
- generated-cell cap via `FE_BENCH_MAX_GENERATED_CELLS`
- engine-specific row caps for R, Polars, pandas, and DuckDB
- checkpoint/resume files for interrupted overnight runs
- skip/failure logs with explicit reasons
- Python subprocess isolation, per-case timeouts, and fail-closed defaults after native crashes were observed on large Python cases

Large-run summary outputs include:

- `scalability_curves.csv`
- `memory_skip_reasons.csv`
- `large_data_decision_table.csv`

The first large-run objective is not to set adaptive production thresholds. It is to learn where column-growth overhead, repeated assignment, batch assignment, direct engine expressions, and orchestration layers change ranking as row counts and generated feature counts rise.

Python large-run caveat: keep Python default runs at 1M/narrow unless explicitly testing a focused large probe. Medium/wide Python shapes and 10M+ Python rows should be treated as opt-in experiments because native process crashes can occur before Python raises a catchable exception.

## Recommended Next Task

Optimize vNext internals without changing public APIs:

1. Rodeo: swap numeric, text, missingness, and selected categorical internals to tighter `data.table::set()` helper kernels.
2. PolarsFE: generate direct expression lists and apply them in fewer `with_columns()` calls, with optional lazy execution for combined plans.
3. Re-run the moderate matrix plus a selected overnight matrix after the changes.
