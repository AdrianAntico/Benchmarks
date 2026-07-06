# Feature Engineering Performance and Best Practices Insights

## 1. Executive Summary

Rodeo vNext and PolarsFE vNext now provide plan/spec-based feature engineering APIs. The modernization work creates a clean fit/transform contract, reusable manifests, diagnostics, warnings, QA helpers, and artifact generators without removing the legacy package APIs.

The Benchmarks repo now contains a repeatable FeatureEngineering benchmark suite that compares R and Python feature engineering engines across feature families, shapes, cardinalities, and row scales. The benchmark evidence shows that vNext APIs are best understood as orchestration, spec, manifest, and reuse layers. Hot paths should use benchmark-proven direct kernels internally.

The central lesson is that stability is part of performance. A feature engineering system is not just fast when one isolated operation runs quickly; it is fast when it can be rerun, scored, diagnosed, resumed, and safely benchmarked without hidden leakage, unbounded memory growth, or native process crashes.

## 2. Scope

Included in the current modernization and benchmark scope:

- numeric transforms
- categorical encoding
- calendar/date features
- text features
- missingness indicators
- interactions
- model prep/table operations, including partitions and folds

Deferred from the current scope:

- Model-Based Features
- Spark
- target encoding / WOE / credibility encoding
- huge unbounded benchmarks

## 3. Package Roles

| Repo | Role |
|---|---|
| Rodeo | R-oriented feature engineering over `data.table`, `collapse`, and legacy Rodeo functions. Rodeo vNext provides scoring-safe plan/spec APIs and model-prep partitions. |
| PolarsFE | Python-oriented feature engineering over Polars. PolarsFE vNext mirrors the Rodeo plan/spec pattern while preserving existing public helpers. |
| Benchmarks | Repeatable performance comparisons, smoke/moderate/large runners, safety guardrails, and implementation decision evidence. |

## 4. vNext API Summary

### Rodeo Feature Engineering

- `rodeo_feature_plan()`
- `rodeo_fit_feature_plan()`
- `rodeo_transform_feature_plan()`
- `rodeo_fit_transform_feature_plan()`
- `generate_rodeo_feature_engineering_artifacts()`

The fitted plan stores numeric parameters, categorical levels, generated feature manifests, diagnostics, warnings, interaction definitions, and fit metadata.

### Rodeo Model Prep

- `rodeo_partition_plan()`
- `rodeo_fit_partition_plan()`
- `rodeo_apply_partition_plan()`
- `rodeo_create_folds()`
- `generate_rodeo_model_prep_artifacts()`

The fitted partition plan stores row-level partition assignments, fold assignments, partition manifests, fold manifests, diagnostics, warnings, and seed/method metadata.

### PolarsFE Feature Engineering

- `polars_feature_plan()`
- `polars_fit_feature_plan()`
- `polars_transform_feature_plan()`
- `polars_fit_transform_feature_plan()`
- `generate_polars_feature_engineering_artifacts()`

The fitted spec stores reusable Polars-friendly transformation metadata and a feature manifest.

### PolarsFE Model Prep

- `polars_partition_plan()`
- `polars_fit_partition_plan()`
- `polars_apply_partition_plan()`
- `polars_create_folds()`
- `generate_polars_model_prep_artifacts()`

The PolarsFE model-prep contract mirrors Rodeo's partition/fold contract for comparable benchmark and future app integration behavior.

## 5. Benchmark Methodology

The FeatureEngineering benchmark suite currently includes:

- smoke benchmarks for fast validation
- moderate benchmarks across 10k, 100k, and 500k row cases
- focused Rodeo benchmarks comparing vNext, direct `data.table::set()`, and batch assignment
- large benchmark runners with checkpoint/resume support
- Python subprocess isolation and per-case timeouts
- generated-cell, memory, row-count, shape, and engine-specific guardrails

Large benchmark guardrails are intentionally conservative. Python large benchmarks default to a 1M row cap, medium/wide shapes are disabled by default, rows above 1M require `FE_BENCH_ALLOW_PYTHON_LARGE=true`, and 10M+ Python cases require `FE_BENCH_ALLOW_PYTHON_10M=true`.

Engine-specific skips are recorded as data. For example, `duckdb` rows were skipped when `duckdb` was not installed, and pandas rows were skipped when Polars-to-pandas conversion required missing `pyarrow`.

## 6. Results Summary

### Moderate Matrix

Source: `FeatureEngineering/outputs/moderate_20260705_205740`

- Result rows: 2,079
- Successful benchmark rows: 1,233
- Skipped rows: 846
- Hard failures: 0

Skips were intentional and explainable: missing optional dependencies, limited direct comparator coverage, generated-cell guardrails, and no directly comparable legacy function for some families.

### Focused Rodeo Run

Source: `FeatureEngineering/outputs/rodeo_focused_20260705_212757`

- Result rows: 168
- Successful benchmark rows: 168
- Hard failures: 0

Fastest median engines by family:

| Family | Fastest median engine | Median seconds |
|---|---:|---:|
| numeric | `data.table set` | 0.110350 |
| categorical | `data.table set` | 0.052561 |
| calendar | `data.table set` | 0.028628 |
| interactions | `Rodeo vNext optimized` | 0.041616 |
| combined_plan | `data.table set` | 1.068216 |

Important focused cases:

| Family | Shape | Cardinality | Fastest engine | Seconds |
|---|---|---|---|---:|
| numeric | narrow | high | `data.table set` | 0.006418 |
| categorical | narrow | low | `data.table batch` | 0.002748 |
| calendar | narrow | low | `data.table set` | 0.001582 |
| interactions | narrow | high | `Rodeo vNext optimized` | 0.004179 |
| combined_plan | narrow | high | `data.table batch` | 0.126932 |

### Overnight / Large Checkpoint

Source: `FeatureEngineering/outputs/overnight_20260705_221309`

- Result rows: 2,059
- Successful benchmark rows: 770
- Skipped rows: 1,289

The run intentionally skipped high-risk combinations after guardrails and resume rules were applied. Earlier high-scale wide R cases showed native memory pressure around 25M wide cases, and Python large cases motivated the subprocess isolation/safety patch.

Important 10M narrow Polars timings:

| Family | Engine | Rows | Shape | Cardinality | Seconds |
|---|---|---:|---|---|---:|
| numeric | Polars eager | 10,000,000 | narrow | low | 0.227393 |
| numeric | Polars lazy | 10,000,000 | narrow | low | 0.238972 |
| numeric | PolarsFE vNext | 10,000,000 | narrow | low | 0.491758 |
| categorical | Polars eager | 10,000,000 | narrow | low | 0.224835 |
| categorical | PolarsFE vNext | 10,000,000 | narrow | low | 0.721883 |

### Latest Smoke Checkpoint

Source: `FeatureEngineering/outputs/checkpoint_smoke_20260706_082234`

- Successful rows: 10
- Skipped rows: 1
- `model_prep` rows: 4 successful rows

Model-prep smoke timings:

| Engine | Family | Rows | Seconds |
|---|---|---:|---:|
| PolarsFE vNext | model_prep | 10,000 | 0.034896 |
| Polars eager direct | model_prep | 10,000 | 0.001016 |
| Rodeo vNext | model_prep | 10,000 | 0.110000 |
| data.table direct | model_prep | 10,000 | 0.000000 |

## 7. Implementation Findings

`data.table::set()` can be fast, but it is not universal. It is strong for repeated direct vector assignment and many narrow hot paths. It can become less attractive when many generated columns are added one at a time on large/wide data.

`:=` and batch assignment can win by shape. For large/wide generated outputs, implementation strategies should reduce repeated column growth, precompute output vectors, and assign multiple columns together where evidence supports it.

`collapse` remains useful where its grouped/vectorized kernels fit the operation. It should be benchmarked as a candidate implementation path, not forced into all families.

Rodeo vNext overhead is acceptable for train/scoring safety, manifests, diagnostics, and artifact output. Direct kernels still define the performance target for internal hot paths.

Direct Polars eager/lazy expressions outperform PolarsFE vNext orchestration for isolated operations. PolarsFE vNext should keep the plan/spec/manifest contract while generating batched native Polars expressions internally.

Python large benchmarks require crash isolation and conservative defaults. Native crashes are not ordinary exceptions; they can restart the host process before Python can report a friendly error.

## 8. Model Prep Findings

The new model-prep vNext layer supports:

- random train/test and train/validation/test splits
- stratified splits
- grouped splits
- time splits
- random, stratified, and grouped k-fold assignments

Partition manifests are valuable because they make row counts, fold counts, methods, seeds, diagnostics, and warnings explicit. This makes downstream model readiness and training workflows easier to audit.

Leakage-safe grouped and time splits are especially important. Grouped splits prevent the same entity from appearing across train/test partitions. Time splits preserve the temporal order needed for realistic validation.

Model prep should sit between feature engineering and Model Readiness. It prepares the table for modeling, while Model Readiness should evaluate whether the table, target, leakage risk, drift, class balance, missingness, and sample size are suitable for modeling.

## 9. Best Practices

### Best Practice By Feature Family

| Feature family | Best practice |
|---|---|
| numeric | Fit train-only parameters once; reuse them for scoring; benchmark direct `set()`, batch assignment, and vectorized paths. |
| categorical | Store fitted levels, rare-level handling, and unseen-level handling; avoid scoring-time leakage. |
| calendar/date | Prefer deterministic direct date extraction; store selected units in the plan. |
| text | Keep lightweight text features expression/vector based; avoid rowwise loops. |
| missingness | Generate indicators in batches where column count is high. |
| interactions | Cap generated features and store the exact interaction spec. |
| combined plans | Use vNext as the public contract, with benchmark-selected internal kernels. |
| model prep | Always return partition/fold manifests and diagnostics; preserve groups and time order where required. |

### Recommended Engine / Method By Data Shape

| Shape | Recommendation |
|---|---|
| narrow/small | Direct kernels often win; vNext overhead is acceptable when reuse/spec artifacts matter. |
| narrow/large | Direct Polars eager/lazy and direct `data.table` paths are the target for vNext internals. |
| wide/high-cardinality | Avoid one-column-at-a-time growth when batch assignment wins; cap features and record skips. |
| combined plans | Use the vNext orchestration layer, but generate family-level native kernels internally. |
| grouped/time model prep | Prefer explicit manifests and leakage-safe assignment over purely fastest split code. |

### Stability Guardrails

| Guardrail | Purpose |
|---|---|
| generated-cell cap | Avoid accidental explosive output sizes. |
| memory cap / RAM fraction | Keep large runs bounded. |
| engine row caps | Avoid unsafe engines at unsafe row scales. |
| Python subprocess isolation | Prevent a native crash from killing the whole benchmark matrix. |
| per-case timeouts | Keep hung cases from blocking overnight runs. |
| checkpoint/resume files | Make long runs restartable. |
| explicit skip reasons | Treat skipped work as evidence, not silent absence. |

### Benchmark Command Recommendations

| Use case | Command pattern |
|---|---|
| smoke validation | `Rscript FeatureEngineering/run_feature_engineering_smoke.R`; `python FeatureEngineering/run_feature_engineering_smoke.py`; `Rscript FeatureEngineering/summarize_feature_engineering_results.R` |
| moderate comparison | Use `run_feature_engineering_moderate.R` and `.py` with a timestamped output folder. |
| focused Rodeo comparison | Use `Rscript FeatureEngineering/run_rodeo_vnext_focused.R`. |
| large R/Polars probe | Use `run_feature_engineering_large.R` / `.py` with explicit row, shape, family, and engine filters. |
| Python 10M+ probe | Require `FE_BENCH_ALLOW_PYTHON_LARGE=true` and `FE_BENCH_ALLOW_PYTHON_10M=true`; keep shape narrow unless specifically testing crash behavior. |

## 10. Decision Table

| Area | Current recommendation |
|---|---|
| Rodeo hot paths | Use mixed strategies. Do not replace everything with `data.table::set()`. Benchmark `set()`, `:=`, grouped `:=`, batch assignment, collapse, and base vectorized paths by operation/shape. |
| PolarsFE hot paths | Keep the vNext public contract, but internally generate batched Polars expressions and minimize repeated materialization. |
| Benchmarks ownership | Keep large performance tests in Benchmarks. Do not move threshold experiments into Rodeo or PolarsFE until results repeat. |
| Adaptive thresholds | Do not hard-code thresholds yet. Use benchmark docs and output summaries as provisional decision evidence. |
| Legacy APIs | Keep legacy Rodeo and existing PolarsFE helpers as behavior/performance baselines. |
| Model prep | Keep split/fold contracts separate from feature transforms and model training. |

## 11. Crash / Stability Lessons

A Python native crash was observed during high-scale benchmark work. That led to fail-closed Python defaults, subprocess-per-case isolation, per-case timeouts, and explicit opt-in flags for large Python cases.

R wide 25M cases showed memory pressure in large/wide combinations. The benchmark suite should treat that as evidence about shape/scale limits, not just a failed run.

Benchmark failures are evidence. They tell us when a strategy is not operationally stable at a given size, dependency state, or shape. A publishable benchmark suite should preserve those records instead of hiding them.

## 12. Next Steps

Recommended next work:

1. Optimize PolarsFE hot paths only after the current safety checkpoint is committed.
2. Refine this report after additional R-only and Polars-only large runs with narrow, bounded matrices.
3. Use the vNext contracts as the foundation for a future AnalyticsShinyApp Feature Engineering module.
4. Redesign Rodeo/PolarsFE Model-Based Features later as a separate leakage-safe system.
5. Keep target encoding, WOE, and credibility encoding outside the first model-prep/table-operations layer until their leakage contracts are explicit.

