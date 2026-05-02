"""Command-line interface for the Spark skew optimization job."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from skew_join_optimization.benchmark import (
    apply_incremental_window,
    load_or_generate_tables,
    print_runtime_summary,
    run_benchmark,
    validate_config,
    write_optimized_output,
)
from skew_join_optimization.config import JobConfig
from skew_join_optimization.spark import build_spark


def parse_args(argv: Sequence[str] | None = None, ignore_unknown: bool = False) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Demonstrate Spark skew handling with repartition, broadcast, and salting."
    )
    parser.add_argument("--records", type=int, default=JobConfig.records, help="Number of generated fact rows.")
    parser.add_argument("--keys", type=int, default=JobConfig.keys, help="Number of generated customer keys.")
    parser.add_argument(
        "--hot-key-ratio",
        type=float,
        default=JobConfig.hot_key_ratio,
        help="Approximate fraction of generated fact records assigned to the hot key.",
    )
    parser.add_argument("--partitions", type=int, default=JobConfig.partitions, help="Shuffle partitions to use.")
    parser.add_argument("--salt-buckets", type=int, default=JobConfig.salt_buckets, help="Salt buckets for the hot key.")
    parser.add_argument("--app-name", default=JobConfig.app_name, help="Spark application name.")
    parser.add_argument("--master", default="local[*]", help="Spark master for local runs. Use empty string on clusters.")
    parser.add_argument("--fact-path", help="Optional CSV fact table path.")
    parser.add_argument("--customers-path", help="Optional CSV customer dimension path.")
    parser.add_argument("--watermark-start", help="Inclusive incremental lower bound for event_ts.")
    parser.add_argument("--watermark-end", help="Exclusive incremental upper bound for event_ts.")
    parser.add_argument("--output-path", help="Optional path for optimized joined output.")
    parser.add_argument(
        "--processing-date",
        default="1970-01-01",
        help="Output partition value. Reruns with the same value overwrite the same partition.",
    )
    parser.add_argument("--output-format", default="parquet", choices=["parquet", "csv", "json"], help="Sink format.")

    if ignore_unknown:
        args, _ = parser.parse_known_args(argv)
        return args

    return parser.parse_args(argv)


def run(args: argparse.Namespace, master_override: str | None = None) -> None:
    config = JobConfig(
        records=args.records,
        keys=args.keys,
        hot_key_ratio=args.hot_key_ratio,
        partitions=args.partitions,
        salt_buckets=args.salt_buckets,
        app_name=args.app_name,
    )
    validate_config(config)

    master = master_override if master_override is not None else args.master if args.master else None
    spark = build_spark(config.app_name, config.partitions, master=master)
    spark.sparkContext.setLogLevel("WARN")

    try:
        if args.fact_path:
            print(f"\nReading sample data from {args.fact_path} and {args.customers_path}")
        else:
            print(
                "\nGenerating skewed data "
                f"(records={config.records:,}, keys={config.keys:,}, hot_key_ratio={config.hot_key_ratio:.2f})"
            )

        fact_df, customer_df = load_or_generate_tables(spark, config, args.fact_path, args.customers_path)
        fact_df = apply_incremental_window(fact_df, args.watermark_start, args.watermark_end)
        results = run_benchmark(spark, config, fact_df, customer_df)
        print_runtime_summary(results)

        if args.output_path:
            write_optimized_output(
                spark,
                config,
                fact_df,
                customer_df,
                args.output_path,
                args.processing_date,
                args.output_format,
            )
    finally:
        spark.stop()


def main(argv: Sequence[str] | None = None, ignore_unknown: bool = False) -> None:
    args = parse_args(argv=argv, ignore_unknown=ignore_unknown)
    run(args)
