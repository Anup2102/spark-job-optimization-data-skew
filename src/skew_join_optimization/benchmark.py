"""Benchmark runner and reporting helpers."""

from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from skew_join_optimization.config import JobConfig, JoinResult
from skew_join_optimization.data_generation import (
    create_customer_dimension,
    create_skewed_fact_table,
    read_customer_dimension,
    read_fact_table,
)
from skew_join_optimization.joins import baseline_join, repartitioned_broadcast_join, salted_broadcast_join


def validate_config(config: JobConfig) -> None:
    if config.records <= 0:
        raise ValueError("records must be greater than 0")
    if config.keys <= 1:
        raise ValueError("keys must be greater than 1")
    if not 0 < config.hot_key_ratio < 1:
        raise ValueError("hot_key_ratio must be between 0 and 1")
    if config.partitions <= 0:
        raise ValueError("partitions must be greater than 0")
    if config.salt_buckets <= 1:
        raise ValueError("salt_buckets must be greater than 1")


def load_or_generate_tables(
    spark: SparkSession,
    config: JobConfig,
    fact_path: str | None = None,
    customers_path: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    if fact_path and customers_path:
        return read_fact_table(spark, fact_path), read_customer_dimension(spark, customers_path)

    if fact_path or customers_path:
        raise ValueError("fact_path and customers_path must be provided together")

    return (
        create_skewed_fact_table(spark, config.records, config.keys, config.hot_key_ratio),
        create_customer_dimension(spark, config.keys),
    )


def apply_incremental_window(
    fact_df: DataFrame,
    watermark_start: str | None = None,
    watermark_end: str | None = None,
    timestamp_column: str = "event_ts",
) -> DataFrame:
    filtered_df = fact_df

    if watermark_start:
        filtered_df = filtered_df.filter(F.col(timestamp_column) >= F.to_timestamp(F.lit(watermark_start)))
    if watermark_end:
        filtered_df = filtered_df.filter(F.col(timestamp_column) < F.to_timestamp(F.lit(watermark_end)))

    return filtered_df


def print_skew_profile(fact_df: DataFrame) -> None:
    print("\nTop customer_id counts before optimization")
    fact_df.groupBy("customer_id").count().orderBy(F.desc("count")).show(10, truncate=False)


def timed_count(name: str, joined_df: DataFrame) -> JoinResult:
    start = time.perf_counter()
    rows = joined_df.count()
    seconds = time.perf_counter() - start
    print(f"{name}: rows={rows:,}, runtime={seconds:,.2f}s")
    return JoinResult(name=name, rows=rows, seconds=seconds)


def run_benchmark(spark: SparkSession, config: JobConfig, fact_df: DataFrame, customer_df: DataFrame) -> list[JoinResult]:
    fact_df = fact_df.cache()
    customer_df = customer_df.cache()
    fact_df.count()
    customer_df.count()

    print_skew_profile(fact_df)

    return [
        timed_count("baseline shuffle join", baseline_join(fact_df, customer_df)),
        timed_count(
            "repartitioned broadcast join",
            repartitioned_broadcast_join(fact_df, customer_df, config.partitions),
        ),
        timed_count(
            "salted repartitioned broadcast join",
            salted_broadcast_join(
                spark,
                fact_df,
                customer_df,
                config.partitions,
                config.salt_buckets,
            ),
        ),
    ]


def write_optimized_output(
    spark: SparkSession,
    config: JobConfig,
    fact_df: DataFrame,
    customer_df: DataFrame,
    output_path: str,
    processing_date: str,
    output_format: str = "parquet",
) -> int:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    output_df = (
        salted_broadcast_join(
            spark,
            fact_df,
            customer_df,
            config.partitions,
            config.salt_buckets,
        )
        .withColumn("processing_date", F.lit(processing_date))
        .select("id", "customer_id", "customer_segment", "event_amount", "event_ts", "processing_date")
    )

    rows = output_df.count()
    (
        output_df.write.mode("overwrite")
        .format(output_format)
        .partitionBy("processing_date")
        .save(output_path)
    )
    print(f"\nWrote {rows:,} optimized rows to {output_path}/processing_date={processing_date}")
    return rows


def print_runtime_summary(results: list[JoinResult]) -> None:
    print("\nRuntime comparison")
    baseline = results[0].seconds
    for result in results:
        improvement = 0.0 if result.seconds == 0 else (baseline - result.seconds) / baseline * 100
        print(f"- {result.name}: {result.seconds:,.2f}s ({improvement:,.1f}% vs baseline)")
