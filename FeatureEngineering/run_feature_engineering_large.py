from __future__ import annotations

import csv
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = Path(os.environ.get("FEATURE_ENGINEERING_BENCHMARK_OUTPUT", ROOT / "FeatureEngineering" / "outputs" / f"large_{datetime.now().strftime('%Y%m%d_%H%M%S')}"))
OUT_DIR.mkdir(parents=True, exist_ok=True)

POLARSFE_ROOT = ROOT.parent / "polars_feature_engineering"
if str(POLARSFE_ROOT) not in sys.path:
    sys.path.insert(0, str(POLARSFE_ROOT))

RESULT_FILE = OUT_DIR / "python_feature_engineering_large_summary.csv"
CHECKPOINT_FILE = OUT_DIR / "python_large_checkpoint.csv"
SKIP_FILE = OUT_DIR / "python_large_skips_failures.csv"


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name, "true" if default else "false").lower()
    return value in {"true", "t", "1", "yes", "y"}


def env_csv(name: str) -> list[str]:
    value = os.environ.get(name, "")
    if not value.strip():
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def normalize_filter_value(value: str) -> str:
    aliases = {
        "combined": "combined_plan",
        "polars_eager": "Polars eager",
        "polars_lazy": "Polars lazy",
        "polarsfe_vnext": "PolarsFE vNext",
        "polarsfe": "PolarsFE vNext",
    }
    return aliases.get(value, value)


def filter_dict(values: dict, env_name: str) -> dict:
    keep = env_csv(env_name)
    if not keep:
        return values
    keep = [normalize_filter_value(x) for x in keep]
    return {k: values[k] for k in keep if k in values}


def filter_list(values: list[str], env_name: str) -> list[str]:
    keep = env_csv(env_name)
    if not keep:
        return values
    keep = [normalize_filter_value(x) for x in keep]
    return [x for x in values if x in keep]


def parse_rows() -> list[int]:
    raw = os.environ.get("FE_BENCH_ROWS", "1000000,5000000,10000000")
    rows = [int(float(x)) for x in raw.split(",") if x.strip()]
    if env_bool("FE_BENCH_ENABLE_25M"):
        rows.append(25_000_000)
    if env_bool("FE_BENCH_ENABLE_50M"):
        rows.append(50_000_000)
    if env_bool("FE_BENCH_ENABLE_100M"):
        rows.append(100_000_000)
    rows = sorted({x for x in rows if x <= 100_000_000})
    if not env_bool("FE_BENCH_ENABLE_100M"):
        rows = [x for x in rows if x < 100_000_000]
    return rows


ROWS_GRID = parse_rows()
CARD_GRID = {"low": 5, "medium": 50, "high": 500}
SHAPES = {
    "narrow": {"numeric": 5, "categorical": 2, "date": 1, "text": 1},
    "medium": {"numeric": 20, "categorical": 5, "date": 2, "text": 3},
    "wide": {"numeric": 100, "categorical": 10, "date": 2, "text": 5},
}
FAMILIES = ["numeric", "categorical", "calendar", "missingness", "interactions", "combined_plan", "text"]
ENGINES = ["PolarsFE vNext", "Polars eager", "Polars lazy", "pandas", "duckdb"]

CARD_GRID = filter_dict(CARD_GRID, "FE_BENCH_CARDINALITY")
SHAPES = filter_dict(SHAPES, "FE_BENCH_SHAPES")
FAMILIES = filter_list(FAMILIES, "FE_BENCH_FAMILIES")
ENGINES = filter_list(ENGINES, "FE_BENCH_ENGINES")

if not ROWS_GRID:
    raise SystemExit("No row scales selected after guardrails.")
if not CARD_GRID:
    raise SystemExit("No cardinality levels selected. Check FE_BENCH_CARDINALITY.")
if not SHAPES:
    raise SystemExit("No shapes selected. Check FE_BENCH_SHAPES.")
if not FAMILIES:
    raise SystemExit("No feature families selected. Check FE_BENCH_FAMILIES.")
if not ENGINES:
    raise SystemExit("No engines selected. Check FE_BENCH_ENGINES.")

MAX_RAM_GB = float(os.environ.get("FE_BENCH_MAX_RAM_GB", "236"))
RAM_FRACTION = float(os.environ.get("FE_BENCH_RAM_FRACTION", "0.6"))
MEMORY_CAP_GB = MAX_RAM_GB * RAM_FRACTION
MAX_GENERATED_CELLS = int(float(os.environ.get("FE_BENCH_MAX_GENERATED_CELLS", "250000000")))
POLARS_MAX_ROWS = int(float(os.environ.get("FE_BENCH_POLARS_MAX_ROWS", "1000000")))
PANDAS_MAX_ROWS = int(float(os.environ.get("FE_BENCH_PANDAS_MAX_ROWS", "1000000")))
DUCKDB_MAX_ROWS = int(float(os.environ.get("FE_BENCH_DUCKDB_MAX_ROWS", "1000000")))
REPEATS = max(1, int(os.environ.get("FE_BENCH_REPEATS", "1")))
RESUME = env_bool("FE_BENCH_RESUME", True)
ALLOW_PYTHON_LARGE = env_bool("FE_BENCH_ALLOW_PYTHON_LARGE", False)
ALLOW_PYTHON_10M = env_bool("FE_BENCH_ALLOW_PYTHON_10M", False)
ALLOW_PYTHON_MEDIUM_WIDE = env_bool("FE_BENCH_ALLOW_PYTHON_MEDIUM_WIDE", False)
CASE_TIMEOUT_SECONDS = max(1, int(float(os.environ.get("FE_BENCH_CASE_TIMEOUT_MINUTES", "10")) * 60))


def feature_estimate(family, shape, cardinality):
    top_n = min(cardinality, 10)
    if family == "numeric":
        return shape["numeric"] * 4
    if family == "categorical":
        return shape["categorical"] * (top_n + 2)
    if family == "calendar":
        return shape["date"] * 5
    if family == "missingness":
        return shape["numeric"] + shape["categorical"] + shape["text"]
    if family == "interactions":
        return min(20, max(1, shape["numeric"] - 1) + top_n + shape["categorical"])
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
    if family == "text":
        return shape["text"] * 4
    return 0


def estimate_memory_gb(n, shape, family, generated_features):
    input_bytes = n * (
        shape["numeric"] * 8
        + shape["categorical"] * 16
        + shape["date"] * 8
        + shape["text"] * 32
        + 8
    )
    generated_bytes = n * generated_features * 8
    return (input_bytes + generated_bytes) * 2.5 / (1024**3)


def case_id(engine, family, rows, shape, cardinality, repeat_id):
    return "|".join([engine, family, str(rows), shape, cardinality, str(repeat_id)])


def completed_cases():
    if not RESUME or not RESULT_FILE.exists():
        return set()
    with RESULT_FILE.open("r", newline="", encoding="utf-8") as fh:
        return {row["case_id"] for row in csv.DictReader(fh)}


FIELDNAMES = [
    "case_id",
    "engine",
    "family",
    "rows",
    "cardinality",
    "cardinality_levels",
    "shape",
    "repeat_id",
    "generated_features",
    "predicted_memory_gb",
    "memory_cap_gb",
    "output_columns",
    "output_mb",
    "elapsed_seconds",
    "status",
    "error",
]


def append_row(row, path=RESULT_FILE):
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def append_skip_failure(row):
    append_row(row, SKIP_FILE)


def skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, reason):
    row = {
        "case_id": case_id(engine, family, n, shape_name, card_name, repeat_id),
        "engine": engine,
        "family": family,
        "rows": n,
        "cardinality": card_name,
        "cardinality_levels": cardinality,
        "shape": shape_name,
        "repeat_id": repeat_id,
        "generated_features": generated,
        "predicted_memory_gb": round(predicted_gb, 3),
        "memory_cap_gb": round(MEMORY_CAP_GB, 3),
        "output_columns": "",
        "output_mb": "",
        "elapsed_seconds": "",
        "status": "skipped",
        "error": reason,
    }
    append_row(row)
    append_skip_failure(row)


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
    if family in ("missingness", "combined_plan"):
        kwargs["missingness"] = {"columns": num_cols + cat_cols + text_cols, "suffix": "_is_missing"}
    if family in ("text", "combined_plan"):
        kwargs["text"] = {"columns": text_cols, "features": ["char_count", "word_count", "digit_count", "blank"]}
    if family in ("interactions", "combined_plan"):
        kwargs["interactions"] = {
            "numeric_pairs": [[num_cols[i], num_cols[i + 1]] for i in range(max(0, len(num_cols) - 1))],
            "categorical_numeric": [{"categorical": cat_cols[0], "numeric": num_cols[0]}],
            "categorical_pairs": [[cat_cols[0], cat_cols[1]]] if len(cat_cols) > 1 else [],
            "max_features": 20,
        }
    return PolarsFE.polars_feature_plan(**kwargs)


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
    elif family == "missingness":
        exprs.extend([pl.col(col).is_null().cast(pl.Int8).alias(f"{col}_is_missing") for col in num_cols + cat_cols + text_cols])
    elif family == "interactions":
        for i in range(min(len(num_cols) - 1, 10)):
            exprs.append((pl.col(num_cols[i]) * pl.col(num_cols[i + 1])).alias(f"{num_cols[i]}_x_{num_cols[i + 1]}"))
    elif family == "text":
        for col in text_cols:
            x = pl.col(col).cast(pl.Utf8).fill_null("")
            exprs.extend([x.str.len_chars().alias(f"{col}_char_count"), x.str.count_matches(r"\S+").alias(f"{col}_word_count"), x.str.count_matches(r"\d").alias(f"{col}_digit_count"), (x.str.strip_chars().str.len_chars() == 0).cast(pl.Int8).alias(f"{col}_blank")])
    else:
        return run_direct_polars(pl, df, "numeric", shape, lazy=lazy)
    out = base.with_columns(exprs)
    return out.collect() if lazy else out


def record(engine, family, df, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, fn):
    start = time.perf_counter()
    status = "success"
    error = ""
    output_columns = ""
    output_mb = ""
    try:
        result = fn()
        if isinstance(result, dict) and "engineered_data" in result:
            result = result["engineered_data"]
        output_columns = len(result.columns)
        output_mb = round(result.estimated_size("mb"), 3)
        del result
    except Exception as exc:  # noqa: BLE001
        status = "error"
        error = str(exc)
    row = {
        "case_id": case_id(engine, family, df.height, shape_name, card_name, repeat_id),
        "engine": engine,
        "family": family,
        "rows": df.height,
        "cardinality": card_name,
        "cardinality_levels": cardinality,
        "shape": shape_name,
        "repeat_id": repeat_id,
        "generated_features": generated,
        "predicted_memory_gb": round(predicted_gb, 3),
        "memory_cap_gb": round(MEMORY_CAP_GB, 3),
        "output_columns": output_columns,
        "output_mb": output_mb,
        "elapsed_seconds": round(time.perf_counter() - start, 6),
        "status": status,
        "error": error,
    }
    append_row(row)
    if status != "success":
        append_skip_failure(row)
    with CHECKPOINT_FILE.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["case_id", "status", "error", "completed_at"])
        if fh.tell() == 0:
            writer.writeheader()
        writer.writerow({"case_id": row["case_id"], "status": row["status"], "error": row["error"], "completed_at": datetime.now().isoformat(timespec="seconds")})


def write_error_case(case: dict, error: str):
    row = {
        "case_id": case_id(case["engine"], case["family"], case["rows"], case["shape_name"], case["card_name"], case["repeat_id"]),
        "engine": case["engine"],
        "family": case["family"],
        "rows": case["rows"],
        "cardinality": case["card_name"],
        "cardinality_levels": case["cardinality"],
        "shape": case["shape_name"],
        "repeat_id": case["repeat_id"],
        "generated_features": case["generated"],
        "predicted_memory_gb": round(case["predicted_gb"], 3),
        "memory_cap_gb": round(MEMORY_CAP_GB, 3),
        "output_columns": "",
        "output_mb": "",
        "elapsed_seconds": "",
        "status": "error",
        "error": error,
    }
    append_row(row)
    append_skip_failure(row)
    with CHECKPOINT_FILE.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["case_id", "status", "error", "completed_at"])
        if fh.tell() == 0:
            writer.writeheader()
        writer.writerow({"case_id": row["case_id"], "status": row["status"], "error": row["error"], "completed_at": datetime.now().isoformat(timespec="seconds")})


def execute_case(case: dict):
    import polars as pl
    import PolarsFE

    n = int(case["rows"])
    shape = SHAPES[case["shape_name"]]
    cardinality = int(case["cardinality"])
    family = case["family"]
    engine = case["engine"]
    df = make_data(pl, n, shape, cardinality)
    if engine == "PolarsFE vNext":
        plan = make_plan(PolarsFE, family, shape, cardinality)
        record(engine, family, df, case["shape_name"], case["card_name"], cardinality, case["repeat_id"], case["generated"], case["predicted_gb"], lambda plan=plan: PolarsFE.polars_fit_transform_feature_plan(df, plan))
    elif engine == "Polars eager":
        record(engine, family, df, case["shape_name"], case["card_name"], cardinality, case["repeat_id"], case["generated"], case["predicted_gb"], lambda family=family: run_direct_polars(pl, df, family, shape, lazy=False))
    elif engine == "Polars lazy":
        record(engine, family, df, case["shape_name"], case["card_name"], cardinality, case["repeat_id"], case["generated"], case["predicted_gb"], lambda family=family: run_direct_polars(pl, df, family, shape, lazy=True))
    elif engine == "pandas":
        pdf = df.to_pandas()
        record(engine, family, df, case["shape_name"], case["card_name"], cardinality, case["repeat_id"], case["generated"], case["predicted_gb"], lambda pdf=pdf, shape=shape: pdf.assign(**{f"{col}_log1p": __import__("numpy").log1p(pdf[col]) for col in [f"num{i}" for i in range(1, shape["numeric"] + 1)]}))
    del df


def run_case_subprocess(case: dict):
    env = os.environ.copy()
    env["FE_BENCH_CHILD_CASE"] = json.dumps(case)
    completed_before = completed_cases()
    try:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve())],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=CASE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        write_error_case(case, f"case_timeout_seconds:{CASE_TIMEOUT_SECONDS}")
        return
    cid = case_id(case["engine"], case["family"], case["rows"], case["shape_name"], case["card_name"], case["repeat_id"])
    if result.returncode != 0 and cid not in completed_cases().difference(completed_before):
        detail = (result.stderr or result.stdout or "").strip().replace("\n", " | ")
        write_error_case(case, f"subprocess_exit:{result.returncode}; {detail[:500]}")


def main():
    child_case = os.environ.get("FE_BENCH_CHILD_CASE")
    if child_case:
        execute_case(json.loads(child_case))
        return

    done = completed_cases()
    for n in ROWS_GRID:
        for shape_name, shape in SHAPES.items():
            for card_name, cardinality in CARD_GRID.items():
                for family in FAMILIES:
                    generated = feature_estimate(family, shape, cardinality)
                    predicted_gb = estimate_memory_gb(n, shape, family, generated)
                    for engine in ENGINES:
                        for repeat_id in range(1, REPEATS + 1):
                            cid = case_id(engine, family, n, shape_name, card_name, repeat_id)
                            if cid in done:
                                continue
                            if n == 100_000_000 and not env_bool("FE_BENCH_ENABLE_100M"):
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "user_disabled_100m")
                                continue
                            if shape_name != "narrow" and not ALLOW_PYTHON_MEDIUM_WIDE:
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "python_medium_wide_disabled")
                                continue
                            if n > 1_000_000 and not ALLOW_PYTHON_LARGE:
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "python_large_disabled")
                                continue
                            if n >= 10_000_000 and not ALLOW_PYTHON_10M:
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "python_10m_disabled")
                                continue
                            if engine in {"PolarsFE vNext", "Polars eager", "Polars lazy"} and n > POLARS_MAX_ROWS:
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "engine_row_limit")
                                continue
                            if engine == "pandas" and n > PANDAS_MAX_ROWS:
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "engine_row_limit")
                                continue
                            if engine == "duckdb" and n > DUCKDB_MAX_ROWS:
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "engine_row_limit")
                                continue
                            if n * generated > MAX_GENERATED_CELLS:
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "generated_cell_guardrail")
                                continue
                            if predicted_gb > MEMORY_CAP_GB:
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "predicted_memory_exceeds_cap")
                                continue
                            if family == "text" and n > 1_000_000:
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "text_large_disabled_by_default")
                                continue
                            if engine == "pandas":
                                try:
                                    import pandas as pd  # noqa: F401
                                    import pyarrow  # noqa: F401
                                except Exception as exc:  # noqa: BLE001
                                    skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, f"dependency_missing: {exc}")
                                    continue
                                if family != "numeric":
                                    skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "pandas_runner_numeric_only")
                                    continue
                            if engine == "duckdb":
                                try:
                                    import duckdb  # noqa: F401
                                except Exception as exc:  # noqa: BLE001
                                    skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, f"dependency_missing: {exc}")
                                    continue
                                skip(engine, family, n, shape_name, card_name, cardinality, repeat_id, generated, predicted_gb, "duckdb_large_runner_placeholder")
                                continue

                            run_case_subprocess(
                                {
                                    "engine": engine,
                                    "family": family,
                                    "rows": n,
                                    "shape_name": shape_name,
                                    "card_name": card_name,
                                    "cardinality": cardinality,
                                    "repeat_id": repeat_id,
                                    "generated": generated,
                                    "predicted_gb": predicted_gb,
                                }
                            )

    (OUT_DIR / "large_benchmark_config_python.json").write_text(
        json.dumps(
            {
                "rows_grid": ROWS_GRID,
                "max_ram_gb": MAX_RAM_GB,
                "ram_fraction": RAM_FRACTION,
                "memory_cap_gb": MEMORY_CAP_GB,
                "max_generated_cells": MAX_GENERATED_CELLS,
                "polars_max_rows": POLARS_MAX_ROWS,
                "pandas_max_rows": PANDAS_MAX_ROWS,
                "duckdb_max_rows": DUCKDB_MAX_ROWS,
                "allow_python_large": ALLOW_PYTHON_LARGE,
                "allow_python_10m": ALLOW_PYTHON_10M,
                "allow_python_medium_wide": ALLOW_PYTHON_MEDIUM_WIDE,
                "case_timeout_seconds": CASE_TIMEOUT_SECONDS,
                "repeats": REPEATS,
                "resume": RESUME,
                "shapes": list(SHAPES.keys()),
                "cardinality": list(CARD_GRID.keys()),
                "families": FAMILIES,
                "engines": ENGINES,
                "python": sys.version,
                "platform": platform.platform(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(OUT_DIR.as_posix())


if __name__ == "__main__":
    main()
