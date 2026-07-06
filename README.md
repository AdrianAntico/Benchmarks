<p align="center">
  <img src="https://raw.githubusercontent.com/AdrianAntico/Benchmarks/main/logo-v2.png" width="1100" alt="Dataframe Benchmarks: Collapse, data.table, Polars, DuckDB, pandas, and PySpark">
</p>

# Dataframe Benchmarks

A reproducible comparison of dataframe engines on realistic beverage-sales data, from 1 million through 1 billion rows. The benchmark measures transformations after data loading so that the charts compare operation time rather than CSV parser speed.

Benchmark suite updated: **2026-06-21**<br>
Published result charts last updated: **2025-11-23**

## Competitors

| Framework | Language | Version represented by published results |
|---|---|---:|
| data.table | R | 1.17.99 |
| Collapse | R | 2.1.5 |
| DuckDB | R / SQL | 1.4.2 |
| Polars | Python | 1.35.2 |
| pandas | Python | 2.3.3 |
| PySpark | Python / JVM | 4.0+ benchmark implementation; results pending |

PySpark uses all available local Spark cores unless your Spark configuration says otherwise. Spark 4.0 or newer is required for the native SQL `ASOF JOIN` used by the rolling-join test.

## Operations

| Operation | Workload |
|---|---|
| Sum aggregation | Three numeric columns grouped by date and four categorical keys |
| Melt | Four numeric columns from wide to long |
| Cast | Long data back to wide form |
| Windowing | Five lags for three numeric columns within four groups |
| Union | Append a dataframe to itself |
| Left join | Equality join on date and four categorical keys |
| Inner join | Equality join on date and four categorical keys |
| Filter | Date, categorical, and numeric predicates |
| Rolling join | Latest prior observation by date within four categorical keys |

### Rolling-join contract

The left side contains every observation. The right side contains one aggregated reference value for every seventh day. For each left row, the benchmark returns the right row with matching `Customer`, `Brand`, `Category`, and `Beverage Flavor` whose date is the latest date less than or equal to the left date.

The implementation uses each engine's native as-of/rolling primitive: data.table rolling joins, DuckDB `ASOF JOIN`, pandas `merge_asof`, Polars `join_asof`, and Spark SQL `ASOF JOIN`. Collapse is omitted from this operation because it does not expose a native rolling/as-of join.

## Data

The synthetic datasets model beverage-company sales and contain one date, four categorical keys, and four numeric measures.

| Dataset | Customer levels | Other categorical levels |
|---:|---:|---|
| 1M rows | 99 | 13 brands, 6 categories, 21 flavors |
| 10M rows | 1,071 | 13 brands, 6 categories, 21 flavors |
| 100M rows | 10,793 | 13 brands, 6 categories, 21 flavors |
| 1B rows | 108,017 | 13 brands, 6 categories, 21 flavors |

Generate the large files with [`Data/FakeBevDataBuilds.R`](Data/FakeBevDataBuilds.R). The repository only tracks the seed dataset.

## Running the suite

Like the existing benchmark scripts, all new scripts default to the external data directory `C:/Users/Bizon/Documents/GitHub/rappwd/`. They also support these environment variables:

- `BENCHMARK_DATA_PATH`: directory containing `FakeBevData1M.csv`, `FakeBevData10M.csv`, and `FakeBevData100M.csv`.
- `BENCHMARK_RESULTS_PATH`: output directory for benchmark CSVs; defaults to the same external data directory.

On Windows, install and verify the complete local Spark environment:

```powershell
# Use this when Java 17 is already installed:
.\Spark\Setup_Spark.ps1

# Or let the setup install Microsoft OpenJDK 17 with winget:
.\Spark\Setup_Spark.ps1 -InstallJava
```

The setup creates `.venv-spark`, installs the pinned PySpark range, checks Java, and executes an end-to-end local Spark job. If PowerShell blocks local scripts, run `Set-ExecutionPolicy -Scope Process Bypass` once in that terminal.

PySpark 4 requires Java 17 or newer. An error mentioning class-file version `61.0` versus `55.0` means Java 11 is active; rerun `Setup_Spark.ps1 -InstallJava` so the script installs/selects Java 17 and updates `JAVA_HOME`.

Run one Spark benchmark or the complete Spark matrix:

```powershell
$env:BENCHMARK_DATA_PATH = "D:\Benchmarks\Data"
$env:BENCHMARK_RESULTS_PATH = "D:\Benchmarks\Results"

.\.venv-spark\Scripts\python.exe Spark/RollingJoin_Spark.py
.\.venv-spark\Scripts\python.exe Spark/benchmark.py agg_sum
.\.venv-spark\Scripts\python.exe Spark/benchmark.py cast
.\.venv-spark\Scripts\python.exe Spark/benchmark.py filter
.\.venv-spark\Scripts\python.exe Spark/benchmark.py inner_join
.\.venv-spark\Scripts\python.exe Spark/benchmark.py lags
.\.venv-spark\Scripts\python.exe Spark/benchmark.py left_join
.\.venv-spark\Scripts\python.exe Spark/benchmark.py melt
.\.venv-spark\Scripts\python.exe Spark/benchmark.py rolling_join
.\.venv-spark\Scripts\python.exe Spark/benchmark.py union
```

The operation-specific Spark files are convenient entry points; they all delegate to [`Spark/benchmark.py`](Spark/benchmark.py). Pass `--repeats N` to change the default three timed runs.

Run the rolling-join competitors, then combine their result files in R:

```powershell
Rscript Datatable/RollingJoin_datatable.R
Rscript DuckDB/RollingJoin_DuckDB.R
python Pandas/RollingJoin_Pandas.py
python Polars/RollingJoin_Polars.py
python Spark/RollingJoin_Spark.py
Rscript CombineResults_RollingJoin.R
```

The legacy operation scripts still expose a `Path` variable at the top. Point it at the same generated-data directory before reproducing the published charts. Every existing `CombineResults_*.R` script now also loads the corresponding `BenchmarkResultsSpark*.csv` file.

## Methodology

- Input loading and one-time setup are outside the measured region.
- Operations run three times where practical; the reported value is the median.
- Lazy engines are explicitly materialized. Spark uses its JVM no-op sink so every output column is evaluated without adding disk-write time.
- All frameworks run locally on the same machine. This measures local execution, not a distributed Spark cluster.
- Validate output shape before comparing timings. Fast wrong answers are still wrong—just with better branding.

The published machine has Windows 10, 256 GB RAM, and an AMD Ryzen CPU with 32 cores / 64 threads. Results from macOS, Linux, and distributed Spark environments are welcome, but should be reported separately because they are not directly comparable.

## Published results

Experiment labels use `M` for millions of rows, `N` for numeric columns, `D` for date columns, `G` for grouping columns, and `L` for lags.

| Operation | Chart |
|---|---|
| Sum aggregation | ![Sum aggregation benchmark](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/AggSum_TotalRunTime.PNG) |
| Melt | ![Melt benchmark](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Melt_TotalRunTime.PNG) |
| Cast | ![Cast benchmark](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Cast_TotalRunTime.PNG) |
| Windowing (lags) | ![Lags benchmark](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Lags_TotalRunTime.PNG) |
| Union | ![Union benchmark](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Union_TotalRunTime.PNG) |
| Left join | ![Left join benchmark](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/LeftJoin_TotalRunTime.PNG) |
| Inner join | ![Inner join benchmark](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/InnerJoin_TotalRunTime.PNG) |
| Filter | ![Filter benchmark](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Filter_TotalRunTime.PNG) |

PySpark and rolling-join charts will be added after the full matrix has been run on the benchmark machine; the README does not present placeholder timings as measured results.

## Contributing results

Include the framework and version, operating system, CPU/core count, memory, Spark master/configuration when applicable, exact commit, and raw result CSVs. Please avoid comparing runs from different hardware in a single ranking chart.
