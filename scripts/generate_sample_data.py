#!/usr/bin/env python3
"""Generate CSV sample data for the Spark skew optimization project."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from skew_join_optimization.data_generation import write_sample_data
from skew_join_optimization.spark import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sample skewed CSV data.")
    parser.add_argument("--output-dir", default="data/generated", help="Output directory for generated CSV folders.")
    parser.add_argument("--records", type=int, default=10_000, help="Number of fact rows to generate.")
    parser.add_argument("--keys", type=int, default=100, help="Number of customer dimension keys.")
    parser.add_argument("--hot-key-ratio", type=float, default=0.70, help="Fraction of rows assigned to the hot key.")
    parser.add_argument("--partitions", type=int, default=8, help="Spark shuffle partitions.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark("generate-skew-sample-data", args.partitions)
    spark.sparkContext.setLogLevel("WARN")
    try:
        write_sample_data(spark, args.output_dir, args.records, args.keys, args.hot_key_ratio)
        print(f"Wrote sample data under {args.output_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
