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
OUT_DIR = Path(os.environ.get("FEATURE_ENGINEERING_BENCHMARK_OUTPUT", ROOT / "FeatureEngineering" / "outputs" / f"moderate_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))
OUT_DIR.mkdir(parents=True, exist_ok=True)
POLARSFE_ROOT = ROOT.parent / "polars_feature_engineering"
if str(POLARSFE_ROOT) not in sys.path:
    sys.path.insert(0, str(POLARSFE_ROOT))

ROWS_GRID = [int(x) for x in os.environ.get("FE_BENCH_ROWS", "10000,100000,500000").split(",")]
CARD_GRID = {"low": 5, "medium": 50, "high": 500}
SHAPES = {
    "narrow": {"numeric": 5, "categorical": 2, "date": 1, "text": 1},
    "medium": {"numeric": 20, "categorical": 5, "date": 2, "text": 3},
    "wide": {"numeric": 100, "categorical": 10, "date": 2, "text": 5},
}
FAMILIES = ["numeric", "categorical", "calendar", "text", "interactions", "missingness", "combined_plan"]
MAX_GENERATED_CELLS = int(os.environ.get("FE_BENCH_MAX_GENERATED_CELLS", "80000000"))


def feature_estimate(family, shape, cardinality):
    top_n = min(cardinality, 10)
    if family == "numeric":
        return shape["numeric"] * 4
    if family == "categorical":
        return shape["categorical"] * (top_n + 2)
    if family == "calendar":
        return shape["date"] * 5
    if family == "text":
        return shape["text"] * 4
    if family == "interactions":
        return min(20, max(1, shape["numeric"] - 1) + top_n + shape["categorical"])
    if family == "missingness":
        return shape["numeric"] + shape["categorical"] + shape["text"]
    if family == "combined_plan":
        return (
            shape["numeric"] * 4
            + shape["categorical"] * (top_n + 2)
            + shape["date"] * 5
            + shape["text"] * 4
            + min(20, max(1, shape["numeric"] - 1) + top_n + shape["categorical"])
            + shape["numeric"]
            + shape["categorical"]
            + shape["text"]
        )
    return 0


def make_data(pl, n, shape, cardinality):
    base_date = datetime(2024, 1, 1)
    data = {"id": list(range(1, n + 1))}
    for j in range(1, shape["numeric"] + 1):
        data[f"num{j}"] = [float((i * (j + 3)) % 1000) / 10.0 + j for i in range(n)]
    levels = [f"L{i}" for i in range(1, cardinality + 1)]
    for j in range(1, shape["categorical"] + 1):
        data[f"cat{j}"] = [levels[(i + j) % cardinality] for i in range(n)]
    for j in range(1, shape["date"] + 1):
        data[f"date{j}"] = [base_date + timedelta(days=(i + j) % 730) for i in range(n)]
    text_values = ["Hello WORLD", "two words", "ABC123!", "small text sample", "", None]
    for j in range(1, shape["text"] + 1):
        data[f"text{j}"] = [text_values[(i + j) % len(text_values)] for i in range(n)]
    return pl.DataFrame(data)


def make_plan(PolarsFE, family, shape, cardinality):
    num_cols = [f"num{i}" for i in range(1, shape["numeric"] + 1)]
    cat_cols = [f"cat{i}" for i in range(1, shape["categorical"] + 1)]
    date_cols = [f"date{i}" for i in range(1, shape["date"] + 1)]
    text_cols = [f"text{i}" for i in range(1, shape["text"] + 1)]
    top_n = min(cardinality, 10)
    kwargs = {}
    if family in ("numeric", "combined_plan"):
        kwargs["numeric"] = {"columns": num_cols, "transforms": ["log1p", "sqrt", "standardize", "winsorize"], "winsorize_probs": [0.01, 0.99]}
    if family in ("categorical", "combined_plan"):
        kwargs["categorical"] = {"columns": cat_cols, "top_n": top_n, "rare_level": "__RARE__", "unseen_level": "__UNSEEN__", "one_hot": True, "keep_original": True}
    if family in ("calendar", "combined_plan"):
        kwargs["calendar"] = {"columns": date_cols, "features": ["year", "month", "wday", "quarter", "is_weekend"]}
    if family in ("text", "combined_plan"):
        kwargs["text"] = {"columns": text_cols, "features": ["char_count", "word_count", "digit_count", "blank"]}
    if family in ("missingness", "combined_plan"):
        kwargs["missingness"] = {"columns": num_cols + cat_cols + text_cols, "suffix": "_is_missing"}
    if family in ("interactions", "combined_plan"):
        kwargs["interactions"] = {
            "numeric_pairs": [[num_cols[i], num_cols[i + 1]] for i in range(max(0, len(num_cols) - 1))],
            "categorical_numeric": [{"categorical": cat_cols[0], "numeric": num_cols[0]}],
            "categorical_pairs": [[cat_cols[0], cat_cols[1]]] if len(cat_cols) > 1 else [],
            "max_features": 20,
        }
    return PolarsFE.polars_feature_plan(**kwargs)


def record(rows, engine, family, df, shape_name, card_name, cardinality, generated_features, fn):
    start = time.perf_counter()
    status = "success"
    error = ""
    output_columns = None
    output_mb = None
    try:
        result = fn()
        if isinstance(result, dict) and "engineered_data" in result:
            result = result["engineered_data"]
        output_columns = len(result.columns)
        output_mb = round(result.estimated_size("mb"), 3)
    except Exception as exc:  # noqa: BLE001
        status = "error"
        error = str(exc)
    rows.append(
        {
            "engine": engine,
            "family": family,
            "rows": df.height,
            "cardinality": card_name,
            "cardinality_levels": cardinality,
            "shape": shape_name,
            "generated_features": generated_features,
            "output_columns": output_columns,
            "output_mb": output_mb,
            "elapsed_seconds": round(time.perf_counter() - start, 6),
            "status": status,
            "error": error,
        }
    )


def skip(rows, engine, family, n, shape_name, card_name, cardinality, generated_features, reason):
    rows.append(
        {
            "engine": engine,
            "family": family,
            "rows": n,
            "cardinality": card_name,
            "cardinality_levels": cardinality,
            "shape": shape_name,
            "generated_features": generated_features,
            "output_columns": "",
            "output_mb": "",
            "elapsed_seconds": "",
            "status": "skipped",
            "error": reason,
        }
    )


def run_direct_polars(pl, df, family, shape, lazy=False):
    num_cols = [f"num{i}" for i in range(1, shape["numeric"] + 1)]
    cat_cols = [f"cat{i}" for i in range(1, shape["categorical"] + 1)]
    date_cols = [f"date{i}" for i in range(1, shape["date"] + 1)]
    text_cols = [f"text{i}" for i in range(1, shape["text"] + 1)]
    base = df.lazy() if lazy else df
    exprs = []
    if family == "numeric":
        for col in num_cols:
            exprs.extend([pl.col(col).log1p().alias(f"{col}_log1p"), pl.col(col).sqrt().alias(f"{col}_sqrt"), ((pl.col(col) - pl.col(col).mean()) / pl.col(col).std()).alias(f"{col}_standardize")])
    elif family == "categorical":
        for col in cat_cols:
            levels = df[col].unique().head(10).to_list()
            exprs.extend([(pl.col(col) == level).cast(pl.Int8).alias(f"{col}__{level}") for level in levels])
    elif family == "calendar":
        for col in date_cols:
            d = pl.col(col).cast(pl.Date)
            exprs.extend([d.dt.year().alias(f"{col}_year"), d.dt.month().alias(f"{col}_month"), d.dt.weekday().alias(f"{col}_wday"), d.dt.quarter().alias(f"{col}_quarter"), d.dt.weekday().is_in([6, 7]).cast(pl.Int8).alias(f"{col}_is_weekend")])
    elif family == "text":
        for col in text_cols:
            x = pl.col(col).cast(pl.Utf8).fill_null("")
            exprs.extend([x.str.len_chars().alias(f"{col}_char_count"), x.str.count_matches(r"\S+").alias(f"{col}_word_count"), x.str.count_matches(r"\d").alias(f"{col}_digit_count"), (x.str.strip_chars().str.len_chars() == 0).cast(pl.Int8).alias(f"{col}_blank")])
    elif family == "missingness":
        exprs.extend([pl.col(col).is_null().cast(pl.Int8).alias(f"{col}_is_missing") for col in num_cols + cat_cols + text_cols])
    elif family == "interactions":
        for i in range(min(len(num_cols) - 1, 10)):
            exprs.append((pl.col(num_cols[i]) * pl.col(num_cols[i + 1])).alias(f"{num_cols[i]}_x_{num_cols[i + 1]}"))
    else:
        return run_direct_polars(pl, df, "numeric", shape, lazy=lazy)
    out = base.with_columns(exprs)
    return out.collect() if lazy else out


def main():
    import polars as pl
    import PolarsFE

    rows = []
    for n in ROWS_GRID:
        for shape_name, shape in SHAPES.items():
            for card_name, cardinality in CARD_GRID.items():
                df = make_data(pl, n, shape, cardinality)
                for family in FAMILIES:
                    generated = feature_estimate(family, shape, cardinality)
                    engines = ["PolarsFE vNext", "Polars eager", "Polars lazy", "pandas", "duckdb"]
                    for engine in engines:
                        if n * generated > MAX_GENERATED_CELLS:
                            skip(rows, engine, family, n, shape_name, card_name, cardinality, generated, "generated-cell guardrail")
                            continue
                        if engine == "pandas":
                            try:
                                import pandas as pd  # noqa: F401
                                pdf = df.to_pandas()
                            except Exception as exc:  # noqa: BLE001
                                skip(rows, engine, family, n, shape_name, card_name, cardinality, generated, str(exc))
                                continue
                            if family != "numeric":
                                skip(rows, engine, family, n, shape_name, card_name, cardinality, generated, "pandas runner only covers numeric")
                                continue
                            record(rows, engine, family, df, shape_name, card_name, cardinality, generated, lambda pdf=pdf: pdf.assign(**{f"{col}_log1p": __import__("numpy").log1p(pdf[col]) for col in [f"num{i}" for i in range(1, shape["numeric"] + 1)]}))
                        elif engine == "duckdb":
                            try:
                                import duckdb  # noqa: F401
                            except Exception as exc:  # noqa: BLE001
                                skip(rows, engine, family, n, shape_name, card_name, cardinality, generated, str(exc))
                                continue
                            skip(rows, engine, family, n, shape_name, card_name, cardinality, generated, "duckdb runner reserved for SQL-friendly follow-up")
                        elif engine == "PolarsFE vNext":
                            plan = make_plan(PolarsFE, family, shape, cardinality)
                            record(rows, engine, family, df, shape_name, card_name, cardinality, generated, lambda plan=plan: PolarsFE.polars_fit_transform_feature_plan(df, plan))
                        elif engine == "Polars eager":
                            record(rows, engine, family, df, shape_name, card_name, cardinality, generated, lambda family=family: run_direct_polars(pl, df, family, shape, lazy=False))
                        elif engine == "Polars lazy":
                            record(rows, engine, family, df, shape_name, card_name, cardinality, generated, lambda family=family: run_direct_polars(pl, df, family, shape, lazy=True))

    out_file = OUT_DIR / "python_feature_engineering_moderate_summary.csv"
    with out_file.open("w", newline="", encoding="utf-8") as fh:
        fieldnames = ["engine", "family", "rows", "cardinality", "cardinality_levels", "shape", "generated_features", "output_columns", "output_mb", "elapsed_seconds", "status", "error"]
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    (OUT_DIR / "system_info_python.json").write_text(
        json.dumps({"python": sys.version, "platform": platform.platform(), "machine": platform.machine(), "rows_grid": ROWS_GRID, "max_generated_cells": MAX_GENERATED_CELLS}, indent=2),
        encoding="utf-8",
    )
    print(OUT_DIR.as_posix())


if __name__ == "__main__":
    main()
