"""Spark session creation for local and cluster runs."""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(app_name: str, partitions: int, master: str | None = "local[*]") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", str(partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", str(100 * 1024 * 1024))
    )

    if master:
        builder = builder.master(master)

    return builder.getOrCreate()
