import os
import statistics
import time
from pathlib import Path

import polars as pl


DEFAULT_DATA_PATH = Path("C:/Users/Bizon/Documents/GitHub/rappwd")
DATA_PATH = Path(os.environ.get("BENCHMARK_DATA_PATH", DEFAULT_DATA_PATH))
RESULT_PATH = Path(os.environ.get("BENCHMARK_RESULTS_PATH", DATA_PATH))
SIZES = ("1M", "10M", "100M")
GROUPS = ["Customer", "Brand", "Category", "Beverage Flavor"]
rows = []

for size in SIZES:
    data = pl.read_csv(DATA_PATH / f"FakeBevData{size}.csv", try_parse_dates=True, rechunk=True)
    left = data.select(*GROUPS, "Date", "Daily Liters").sort(["Date", *GROUPS])
    right = (
        data.filter(pl.col("Date").dt.ordinal_day() % 7 == 0)
        .group_by(*GROUPS, "Date", maintain_order=True)
        .agg(pl.sum("Daily Revenue").alias("Reference Revenue"))
        .rename({"Date": "Reference Date"})
        .sort(["Reference Date", *GROUPS])
    )

    timings = []
    for attempt in range(3):
        start = time.perf_counter()
        answer = left.join_asof(
            right,
            left_on="Date",
            right_on="Reference Date",
            by=GROUPS,
            strategy="backward",
            check_sortedness=False,
        )
        timings.append(time.perf_counter() - start)
        assert answer.height == left.height
        del answer
    rows.append({
        "Framework": "polars",
        "Method": "rolling join",
        "Experiment": f"{size} 1N 1D 4G",
        "TimeInSeconds": statistics.median(timings),
    })

RESULT_PATH.mkdir(parents=True, exist_ok=True)
pl.DataFrame(rows).write_csv(RESULT_PATH / "BenchmarkResultsPolars_RollingJoin.csv")
