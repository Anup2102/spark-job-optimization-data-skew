from __future__ import annotations

import pytest

from skew_join_optimization.config import HOT_KEY
from pyspark.sql import functions as F

from skew_join_optimization.benchmark import apply_incremental_window, write_optimized_output
from skew_join_optimization.config import JobConfig
from skew_join_optimization.data_generation import create_customer_dimension, create_skewed_fact_table
from skew_join_optimization.joins import add_salt_to_fact, expand_dimension_for_salt, salted_broadcast_join
from skew_join_optimization.spark import build_spark


@pytest.fixture(scope="session")
def spark():
    session = build_spark("skew-join-tests", partitions=4)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_skewed_fact_table_contains_hot_key(spark) -> None:
    fact_df = create_skewed_fact_table(spark, records=1000, keys=20, hot_key_ratio=0.75)
    top_key = fact_df.groupBy("customer_id").count().orderBy("count", ascending=False).first()

    assert top_key["customer_id"] == HOT_KEY
    assert top_key["count"] > 500


def test_salted_dimension_expands_only_hot_key(spark) -> None:
    customer_df = create_customer_dimension(spark, keys=10)
    salted_dimension = expand_dimension_for_salt(spark, customer_df, salt_buckets=4)

    assert salted_dimension.filter(f"customer_id = {HOT_KEY}").count() == 4
    assert salted_dimension.filter(f"customer_id != {HOT_KEY}").count() == 9


def test_salted_join_preserves_row_count(spark) -> None:
    fact_df = create_skewed_fact_table(spark, records=500, keys=20, hot_key_ratio=0.70)
    customer_df = create_customer_dimension(spark, keys=20)

    joined_df = salted_broadcast_join(spark, fact_df, customer_df, partitions=4, salt_buckets=4)

    assert joined_df.count() == fact_df.count()
    assert "salt" not in joined_df.columns


def test_hot_key_salt_is_spread_across_buckets(spark) -> None:
    fact_df = create_skewed_fact_table(spark, records=1000, keys=20, hot_key_ratio=0.90)
    salted_fact = add_salt_to_fact(fact_df, salt_buckets=4)

    salt_count = salted_fact.filter(f"customer_id = {HOT_KEY}").select("salt").distinct().count()

    assert salt_count > 1


def test_incremental_window_filters_event_ts(spark) -> None:
    fact_df = spark.createDataFrame(
        [
            (1, 1, 10.0, "2026-05-02 09:59:59"),
            (2, 1, 20.0, "2026-05-02 10:00:00"),
            (3, 2, 30.0, "2026-05-02 10:29:59"),
            (4, 3, 40.0, "2026-05-02 10:30:00"),
        ],
        ["id", "customer_id", "event_amount", "event_ts"],
    ).withColumn("event_ts", F.to_timestamp("event_ts"))

    filtered_df = apply_incremental_window(
        fact_df,
        watermark_start="2026-05-02 10:00:00",
        watermark_end="2026-05-02 10:30:00",
    )

    assert [row["id"] for row in filtered_df.orderBy("id").collect()] == [2, 3]


def test_optimized_output_is_idempotent_for_same_partition(spark, tmp_path) -> None:
    fact_df = create_skewed_fact_table(spark, records=50, keys=10, hot_key_ratio=0.70)
    customer_df = create_customer_dimension(spark, keys=10)
    output_path = str(tmp_path / "optimized_events")

    first_rows = write_optimized_output(
        spark,
        JobConfig(records=50, keys=10, partitions=4, salt_buckets=4),
        fact_df,
        customer_df,
        output_path,
        processing_date="2026-05-02",
    )
    second_rows = write_optimized_output(
        spark,
        JobConfig(records=50, keys=10, partitions=4, salt_buckets=4),
        fact_df,
        customer_df,
        output_path,
        processing_date="2026-05-02",
    )

    written_df = spark.read.parquet(output_path)

    assert first_rows == 50
    assert second_rows == 50
    assert written_df.count() == 50
