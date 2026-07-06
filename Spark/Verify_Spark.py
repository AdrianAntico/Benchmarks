"""Verify that Java and a local PySpark session work end to end."""

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("Benchmarks-Spark-Verification")
    .getOrCreate()
)

try:
    total = spark.range(10).groupBy().sum("id").first()[0]
    if total != 45:
        raise RuntimeError(f"Unexpected Spark result: {total}; expected 45")
    print(f"PySpark {spark.version} is ready (local smoke test: 45).")
finally:
    spark.stop()
