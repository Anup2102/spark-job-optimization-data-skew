"""Data generation and loading helpers for skewed Spark join examples."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from skew_join_optimization.config import HOT_KEY


FACT_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=False),
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("event_amount", DoubleType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=True),
    ]
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("customer_segment", StringType(), nullable=True),
    ]
)


def create_skewed_fact_table(spark: SparkSession, records: int, keys: int, hot_key_ratio: float) -> DataFrame:
    """Create a large fact table where most rows use the same customer_id."""
    return (
        spark.range(records)
        .withColumn(
            "customer_id",
            F.when(F.rand(seed=7) < F.lit(hot_key_ratio), F.lit(HOT_KEY)).otherwise(
                ((F.col("id") % F.lit(keys - 1)) + F.lit(2)).cast(IntegerType())
            ),
        )
        .withColumn("event_amount", (F.rand(seed=11) * F.lit(500)).cast("double"))
        .withColumn("event_ts", F.current_timestamp())
        .select("id", "customer_id", "event_amount", "event_ts")
    )


def create_customer_dimension(spark: SparkSession, keys: int) -> DataFrame:
    """Create a small dimension table suitable for broadcast joins."""
    return (
        spark.range(1, keys + 1)
        .withColumnRenamed("id", "customer_id")
        .withColumn(
            "customer_segment",
            F.when(F.col("customer_id") == HOT_KEY, F.lit("enterprise_hot_key"))
            .when(F.col("customer_id") % 5 == 0, F.lit("enterprise"))
            .when(F.col("customer_id") % 3 == 0, F.lit("growth"))
            .otherwise(F.lit("standard")),
        )
    )


def read_fact_table(spark: SparkSession, path: str | Path) -> DataFrame:
    return spark.read.option("header", "true").schema(FACT_SCHEMA).csv(str(path))


def read_customer_dimension(spark: SparkSession, path: str | Path) -> DataFrame:
    return spark.read.option("header", "true").schema(CUSTOMER_SCHEMA).csv(str(path))


def write_sample_data(spark: SparkSession, output_dir: str | Path, records: int, keys: int, hot_key_ratio: float) -> None:
    output = Path(output_dir)
    fact_df = create_skewed_fact_table(spark, records, keys, hot_key_ratio)
    customer_df = create_customer_dimension(spark, keys)

    fact_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output / "fact_events"))
    customer_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output / "customer_dim"))
