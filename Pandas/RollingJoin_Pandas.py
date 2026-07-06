import csv
import os
import statistics
import time
from pathlib import Path

import pandas as pd


DEFAULT_DATA_PATH = Path("C:/Users/Bizon/Documents/GitHub/rappwd")
DATA_PATH = Path(os.environ.get("BENCHMARK_DATA_PATH", DEFAULT_DATA_PATH))
RESULT_PATH = Path(os.environ.get("BENCHMARK_RESULTS_PATH", DATA_PATH))
SIZES = ("1M", "10M", "100M")
GROUPS = ["Customer", "Brand", "Category", "Beverage Flavor"]
rows = []

for size in SIZES:
    data = pd.read_csv(DATA_PATH / f"FakeBevData{size}.csv", engine="pyarrow")
    data["Date"] = pd.to_datetime(data["Date"])
    left = data[[*GROUPS, "Date", "Daily Liters"]].sort_values(["Date", *GROUPS])
    right = (
        data.loc[data["Date"].dt.dayofyear.mod(7).eq(0), [*GROUPS, "Date", "Daily Revenue"]]
        .groupby([*GROUPS, "Date"], as_index=False, sort=False)["Daily Revenue"].sum()
        .rename(columns={"Daily Revenue": "Reference Revenue", "Date": "Reference Date"})
        .sort_values(["Reference Date", *GROUPS])
    )

    timings = []
    for attempt in range(3):
        start = time.perf_counter()
        answer = pd.merge_asof(
            left,
            right,
            left_on="Date",
            right_on="Reference Date",
            by=GROUPS,
            direction="backward",
        )
        timings.append(time.perf_counter() - start)
        assert len(answer) == len(left)
        del answer
    rows.append({
        "Framework": "pandas",
        "Method": "rolling join",
        "Experiment": f"{size} 1N 1D 4G",
        "TimeInSeconds": statistics.median(timings),
    })

RESULT_PATH.mkdir(parents=True, exist_ok=True)
with (RESULT_PATH / "BenchmarkResultsPandas_RollingJoin.csv").open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=rows[0])
    writer.writeheader()
    writer.writerows(rows)
