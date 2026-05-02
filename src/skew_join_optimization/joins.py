"""Join strategies used by the skew optimization benchmark."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from skew_join_optimization.config import HOT_KEY


def baseline_join(fact_df: DataFrame, customer_df: DataFrame) -> DataFrame:
    return fact_df.join(customer_df, on="customer_id", how="inner")


def repartitioned_broadcast_join(fact_df: DataFrame, customer_df: DataFrame, partitions: int) -> DataFrame:
    repartitioned_fact = fact_df.repartition(partitions, "customer_id")
    return repartitioned_fact.join(F.broadcast(customer_df), on="customer_id", how="inner")


def add_salt_to_fact(fact_df: DataFrame, salt_buckets: int) -> DataFrame:
    return fact_df.withColumn(
        "salt",
        F.when(
            F.col("customer_id") == HOT_KEY,
            F.floor(F.rand(seed=23) * F.lit(salt_buckets)).cast(IntegerType()),
        ).otherwise(F.lit(0)),
    )


def expand_dimension_for_salt(spark: SparkSession, customer_df: DataFrame, salt_buckets: int) -> DataFrame:
    salts = spark.range(salt_buckets).withColumnRenamed("id", "salt").withColumn(
        "salt", F.col("salt").cast(IntegerType())
    )

    hot_dimension = customer_df.filter(F.col("customer_id") == HOT_KEY).crossJoin(salts)
    regular_dimension = customer_df.filter(F.col("customer_id") != HOT_KEY).withColumn("salt", F.lit(0))
    return hot_dimension.unionByName(regular_dimension)


def salted_broadcast_join(
    spark: SparkSession,
    fact_df: DataFrame,
    customer_df: DataFrame,
    partitions: int,
    salt_buckets: int,
) -> DataFrame:
    salted_fact = add_salt_to_fact(fact_df, salt_buckets)
    salted_dimension = expand_dimension_for_salt(spark, customer_df, salt_buckets)

    return (
        salted_fact.repartition(partitions, "customer_id", "salt")
        .join(F.broadcast(salted_dimension), on=["customer_id", "salt"], how="inner")
        .drop("salt")
    )
