"""PySpark implementations of the Benchmarks operations.

Input and result files default to the external directory used by the existing
benchmark scripts. Set BENCHMARK_DATA_PATH and BENCHMARK_RESULTS_PATH to
override it.
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


DEFAULT_DATA_PATH = Path("C:/Users/Bizon/Documents/GitHub/rappwd")
DATA_PATH = Path(os.environ.get("BENCHMARK_DATA_PATH", DEFAULT_DATA_PATH))
RESULT_PATH = Path(os.environ.get("BENCHMARK_RESULTS_PATH", DATA_PATH))
SIZES = ("1M", "10M", "100M")
KEYS = ["Date", "Customer", "Brand", "Category", "Beverage Flavor"]
GROUP_KEYS = ["Customer", "Brand", "Category", "Beverage Flavor"]
NUMERIC = ["Daily Liters", "Daily Units", "Daily Margin", "Daily Revenue"]

RESULT_NAMES = {
    "agg_sum": "BenchmarkResultsSpark.csv",
    "cast": "BenchmarkResultsSpark_Cast.csv",
    "filter": "BenchmarkResultsSpark_Filter.csv",
    "inner_join": "BenchmarkResultsSpark_InnerJoin.csv",
    "lags": "BenchmarkResultsSpark_Lags.csv",
    "left_join": "BenchmarkResultsSpark_LeftJoin.csv",
    "melt": "BenchmarkResultsSpark_Melt.csv",
    "rolling_join": "BenchmarkResultsSpark_RollingJoin.csv",
    "union": "BenchmarkResultsSpark_Union.csv",
}

METHODS = {
    "agg_sum": "sum aggregation",
    "cast": "cast",
    "filter": "filter",
    "inner_join": "inner join",
    "lags": "lags",
    "left_join": "left join",
    "melt": "melt",
    "rolling_join": "rolling join",
    "union": "union",
}


def load_data(spark: SparkSession, size: str) -> DataFrame:
    path = DATA_PATH / f"FakeBevData{size}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing {path}. Generate the benchmark data or set BENCHMARK_DATA_PATH."
        )
    data = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    return data.withColumn("Date", F.to_date("Date")).cache()


def build_operation(name: str, data: DataFrame) -> DataFrame:
    if name == "agg_sum":
        return data.groupBy(*KEYS).agg(
            F.sum("Daily Liters").alias("Daily Liters"),
            F.sum("Daily Units").alias("Daily Units"),
            F.sum("Daily Margin").alias("Daily Margin"),
        )

    if name == "melt":
        pairs = ", ".join(f"'{column}', `{column}`" for column in NUMERIC)
        return data.selectExpr(
            *[f"`{column}`" for column in KEYS],
            f"stack({len(NUMERIC)}, {pairs}) as (variable, value)",
        )

    if name == "cast":
        long = build_operation("melt", data).groupBy(*KEYS, "variable").agg(
            F.sum("value").alias("value")
        )
        return long.groupBy(*KEYS).pivot("variable", NUMERIC).sum("value")

    if name == "filter":
        location_number = F.regexp_extract("Customer", r"(\d+)$", 1).cast("int")
        return data.filter(
            (F.col("Date") > F.lit("2021-06-01").cast("date"))
            & (F.pmod(location_number, F.lit(2)) == 1)
            & F.col("Brand").isin("#N/A", "Cola-Generic", "Elves", "Sparkling", "Yellow-Yum", "Zingers")
            & F.col("Category").isin("Cocain", "Fuzzy", "Juicy")
            & F.col("Beverage Flavor").isin(
                "Angel", "Elves Crapple", "Elves Mint", "Florida-Grape Temperate",
                "Hot", "Limon", "Sparkling Berry", "Sparkling Curry", "Yuck",
                "Zappies Pulp Pooch",
            )
            & (F.col("Daily Liters") > 20)
            & (F.col("Daily Margin") < 100)
        )

    if name in {"inner_join", "left_join"}:
        left = data.select(*KEYS, "Daily Liters")
        right = data.select(*KEYS, "Daily Units", "Daily Margin", "Daily Revenue")
        if name == "left_join":
            right = right.filter(F.col("Brand") != "Zingers")
        return left.join(right, KEYS, "inner" if name == "inner_join" else "left")

    if name == "lags":
        window = Window.partitionBy(*GROUP_KEYS).orderBy("Date")
        result = data
        for column in NUMERIC[:3]:
            for lag in range(1, 6):
                result = result.withColumn(f"Lag {column} {lag}", F.lag(column, lag).over(window))
        return result

    if name == "union":
        selected = data.select(*KEYS, *NUMERIC[:3])
        return selected.unionByName(selected)

    if name == "rolling_join":
        # Spark 4.0+ SQL ASOF JOIN: latest right-side date not after the left date.
        left = data.select(*KEYS, "Daily Liters").alias("l")
        right = (
            data.filter(F.pmod(F.dayofyear("Date"), F.lit(7)) == 0)
            .groupBy(*KEYS)
            .agg(F.sum("Daily Revenue").alias("Reference Revenue"))
            .alias("r")
        )
        left.createOrReplaceTempView("rolling_left")
        right.createOrReplaceTempView("rolling_right")
        return data.sparkSession.sql(
            """
            SELECT l.*, r.`Reference Revenue`, r.Date AS `Reference Date`
            FROM rolling_left l ASOF LEFT JOIN rolling_right r
              ON l.Customer = r.Customer
             AND l.Brand = r.Brand
             AND l.Category = r.Category
             AND l.`Beverage Flavor` = r.`Beverage Flavor`
             AND l.Date >= r.Date
            """
        )

    raise ValueError(f"Unknown operation: {name}")


def materialize(frame: DataFrame) -> None:
    # Spark is lazy. The no-op JVM sink evaluates every output column without I/O.
    frame.write.format("noop").mode("overwrite").save()


def experiment_label(operation: str, size: str) -> str:
    if operation == "filter":
        return f"{size} 2N 1D 4G"
    if operation == "lags":
        return f"{size} 3N 1D 4G 5L"
    if operation in {"cast", "melt"}:
        return f"{size} 4N 1D 4G"
    if operation == "rolling_join":
        return f"{size} 1N 1D 4G"
    return f"{size} 3N 1D 4G"


def run(operation: str, repeats: int) -> Path:
    spark = (
        SparkSession.builder.appName(f"Benchmarks-{operation}")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    rows = []
    try:
        for size in SIZES:
            data = load_data(spark, size)
            data.count()  # Materialize input outside the measured region.
            timings = []
            for attempt in range(repeats):
                start = time.perf_counter()
                materialize(build_operation(operation, data))
                timings.append(time.perf_counter() - start)
                print(f"{size} attempt {attempt + 1}/{repeats}: {timings[-1]:.3f}s")
            rows.append(
                {
                    "Framework": "pyspark",
                    "Method": METHODS[operation],
                    "Experiment": experiment_label(operation, size),
                    "TimeInSeconds": statistics.median(timings),
                }
            )
            data.unpersist()
    finally:
        spark.stop()

    RESULT_PATH.mkdir(parents=True, exist_ok=True)
    output = RESULT_PATH / RESULT_NAMES[operation]
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return output


def main(default_operation: str | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", nargs="?", default=default_operation, choices=RESULT_NAMES)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.operation is None:
        parser.error("operation is required")
    print(run(args.operation, args.repeats))


if __name__ == "__main__":
    main()
