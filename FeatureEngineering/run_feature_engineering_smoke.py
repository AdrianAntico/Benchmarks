from __future__ import annotations

import csv
import json
import os
import platform
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = Path(os.environ.get("FEATURE_ENGINEERING_BENCHMARK_OUTPUT", ROOT / "FeatureEngineering" / "outputs" / datetime.now().strftime("%Y%m%d_%H%M%S")))
OUT_DIR.mkdir(parents=True, exist_ok=True)

POLARSFE_ROOT = ROOT.parent / "polars_feature_engineering"
if str(POLARSFE_ROOT) not in sys.path:
    sys.path.insert(0, str(POLARSFE_ROOT))

rows = []


def record(engine, family, n_rows, fn):
    start = time.perf_counter()
    status = "success"
    error = ""
    try:
        fn()
    except Exception as exc:  # noqa: BLE001
        status = "error"
        error = str(exc)
    elapsed = time.perf_counter() - start
    rows.append(
        {
            "engine": engine,
            "family": family,
            "rows": n_rows,
            "elapsed_seconds": round(elapsed, 6),
            "status": status,
            "error": error,
        }
    )


def main():
    import polars as pl

    n = 10000
    base_date = datetime(2024, 1, 1)
    df = pl.DataFrame(
        {
            "id": list(range(1, n + 1)),
            "x": [(i % 101) + 0.1 for i in range(n)],
            "y": [50 + ((i * 17) % 31) for i in range(n)],
            "cat": [["A", "B", "C", "D", "E"][i % 5] for i in range(n)],
            "cat2": [["K", "L", "M"][i % 3] for i in range(n)],
            "target": [["no", "yes"][i % 2] for i in range(n)],
            "group": [f"g{(i % 200) + 1}" for i in range(n)],
            "date": [base_date + timedelta(days=i % 365) for i in range(n)],
            "text": [["Hello WORLD", "two words", "ABC123!", "small", "", None][i % 6] for i in range(n)],
        }
    )

    import PolarsFE

    plan = PolarsFE.polars_feature_plan(
        numeric={"columns": ["x", "y"], "transforms": ["log1p", "sqrt", "standardize", "winsorize"], "winsorize_probs": [0.01, 0.99]},
        categorical={"columns": ["cat"], "top_n": 4, "rare_level": "__RARE__", "unseen_level": "__UNSEEN__", "one_hot": True, "keep_original": True},
        calendar={"columns": ["date"], "features": ["year", "month", "wday", "quarter", "is_weekend"]},
        text={"columns": ["text"], "features": ["char_count", "word_count", "digit_count", "blank"]},
        missingness={"columns": ["x", "cat", "text"], "suffix": "_is_missing"},
        interactions={
            "numeric_pairs": [["x", "y"]],
            "categorical_numeric": [{"categorical": "cat", "numeric": "x"}],
            "categorical_pairs": [["cat", "cat2"]],
            "max_features": 20,
        },
    )

    record("PolarsFE vNext", "combined_plan", n, lambda: PolarsFE.polars_fit_transform_feature_plan(df, plan))

    partition_plan = PolarsFE.polars_partition_plan(
        method="stratified",
        fractions={"train": 0.7, "validation": 0.1, "test": 0.2},
        target_col="target",
        seed=42,
        k=5,
    )
    record(
        "PolarsFE vNext",
        "model_prep",
        n,
        lambda: PolarsFE.polars_apply_partition_plan(df, PolarsFE.polars_fit_partition_plan(df, partition_plan)),
    )

    record(
        "Polars eager direct",
        "model_prep",
        n,
        lambda: df.with_row_index(".row_id", offset=1).with_columns(
            [
                pl.when(pl.arange(0, pl.len()) % 10 < 7)
                .then(pl.lit("train"))
                .when(pl.arange(0, pl.len()) % 10 == 7)
                .then(pl.lit("validation"))
                .otherwise(pl.lit("test"))
                .alias(".partition"),
                ((pl.arange(0, pl.len()) % 5) + 1).alias(".fold_id"),
            ]
        ),
    )

    record(
        "Polars eager direct",
        "numeric",
        n,
        lambda: df.with_columns(
            [
                pl.col("x").log1p().alias("x_log1p"),
                pl.col("x").sqrt().alias("x_sqrt"),
                ((pl.col("x") - pl.col("x").mean()) / pl.col("x").std()).alias("x_standardize"),
            ]
        ),
    )

    record(
        "Polars lazy direct",
        "numeric",
        n,
        lambda: df.lazy()
        .with_columns(
            [
                pl.col("x").log1p().alias("x_log1p"),
                pl.col("x").sqrt().alias("x_sqrt"),
                ((pl.col("x") - pl.col("x").mean()) / pl.col("x").std()).alias("x_standardize"),
            ]
        )
        .collect(),
    )

    try:
        import pandas as pd

        pdf = df.to_pandas()
        record(
            "pandas direct",
            "numeric",
            n,
            lambda: pdf.assign(
                x_log1p=lambda d: (d["x"] + 1).map(__import__("math").log),
                x_sqrt=lambda d: d["x"] ** 0.5,
                x_standardize=lambda d: (d["x"] - d["x"].mean()) / d["x"].std(),
            ),
        )
    except Exception as exc:  # noqa: BLE001
        rows.append({"engine": "pandas direct", "family": "numeric", "rows": n, "elapsed_seconds": 0, "status": "skipped", "error": str(exc)})

    with (OUT_DIR / "python_feature_engineering_summary.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["engine", "family", "rows", "elapsed_seconds", "status", "error"])
        writer.writeheader()
        writer.writerows(rows)

    decisions = {}
    for row in rows:
        if row["status"] != "success":
            continue
        family = row["family"]
        if family not in decisions or row["elapsed_seconds"] < decisions[family]["elapsed_seconds"]:
            decisions[family] = row
    with (OUT_DIR / "python_decision_table.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["family", "engine", "elapsed_seconds", "decision"])
        writer.writeheader()
        for family, row in decisions.items():
            writer.writerow(
                {
                    "family": family,
                    "engine": row["engine"],
                    "elapsed_seconds": row["elapsed_seconds"],
                    "decision": f"{row['engine']} was fastest in this smoke run; confirm with moderate/overnight matrix.",
                }
            )

    (OUT_DIR / "system_info_python.json").write_text(
        json.dumps({"python": sys.version, "platform": platform.platform(), "machine": platform.machine()}, indent=2),
        encoding="utf-8",
    )

    md = ["# Feature Engineering Python Smoke Benchmark", "", f"Output path: {OUT_DIR.as_posix()}", "", "## Results", ""]
    for row in rows:
        md.append(f"- {row['engine']} / {row['family']}: {row['status']} in {row['elapsed_seconds']} sec {row['error']}")
    (OUT_DIR / "feature_engineering_python_summary.md").write_text("\n".join(md), encoding="utf-8")
    print(OUT_DIR.as_posix())


if __name__ == "__main__":
    main()
