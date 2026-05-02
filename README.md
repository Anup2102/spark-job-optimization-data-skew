# Spark Job Optimization: Data Skew and Join Tuning

Ready-to-run PySpark project that simulates a slow skewed join and demonstrates
optimization with repartitioning, broadcast joins, and salting.

The project mirrors a production optimization:

> Optimized a large-scale Spark job processing ~200M records that was running
> for over 3 hours. Identified bottlenecks caused by data skew and inefficient
> joins. Implemented repartitioning, broadcast joins, and skew-handling
> techniques. Reduced runtime to ~50 minutes, about a 70% improvement.

## What This Project Shows

- How skewed join keys create slow Spark tasks.
- How to identify the hot key distribution.
- How repartitioning improves shuffle parallelism.
- How broadcast joins avoid shuffling small dimension tables.
- How salting spreads one hot key across multiple join keys.
- How to structure a PySpark project with code, tests, sample data, and scripts.

## Project Structure

```text
.
├── data
│   └── sample
│       ├── customer_dim.csv
│       └── fact_events.csv
├── jobs
│   └── skew_join_optimization.py
├── scripts
│   └── generate_sample_data.py
├── src
│   └── skew_join_optimization
│       ├── __init__.py
│       ├── benchmark.py
│       ├── cli.py
│       ├── config.py
│       ├── data_generation.py
│       ├── joins.py
│       └── spark.py
├── tests
│   ├── test_config.py
│   └── test_joins.py
├── .gitignore
├── Makefile
├── pyproject.toml
├── README.md
└── requirements.txt
```

## Prerequisites

- Python 3.10 or newer.
- Java 17 or newer.
- Internet access for the first dependency install.

This repository uses PySpark 4.x so it works with newer local Python versions,
including Python 3.14. If your production cluster is pinned to Spark 3.x, use a
Python version supported by that Spark release and adjust `requirements.txt`.

## Quick Start

Create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the checked-in sample data:

```bash
make run-sample
```

Run an idempotent incremental sample load:

```bash
make run-incremental
```

Run a generated local benchmark:

```bash
make run
```

Run tests:

```bash
make test
```

## Run Without Make

Sample CSV run:

```bash
PYTHONPATH=src python3 jobs/skew_join_optimization.py \
  --fact-path data/sample/fact_events.csv \
  --customers-path data/sample/customer_dim.csv \
  --partitions 4 \
  --salt-buckets 4
```

Incremental load with deterministic output partition:

```bash
PYTHONPATH=src python3 jobs/skew_join_optimization.py \
  --fact-path data/sample/fact_events.csv \
  --customers-path data/sample/customer_dim.csv \
  --watermark-start "2026-05-02 10:00:00" \
  --watermark-end "2026-05-02 10:30:00" \
  --output-path data/generated/optimized_events \
  --processing-date 2026-05-02 \
  --partitions 4 \
  --salt-buckets 4
```

Generated data run:

```bash
PYTHONPATH=src python3 jobs/skew_join_optimization.py \
  --records 100000 \
  --keys 1000 \
  --hot-key-ratio 0.70 \
  --partitions 16 \
  --salt-buckets 8
```

Spark submit run:

```bash
spark-submit jobs/skew_join_optimization.py \
  --records 1000000 \
  --keys 10000 \
  --hot-key-ratio 0.70 \
  --partitions 64 \
  --salt-buckets 16
```

## Generate More Sample Data

Write generated CSV folders under `data/generated`:

```bash
make generate-sample
```

Or customize the generated data:

```bash
PYTHONPATH=src python3 scripts/generate_sample_data.py \
  --output-dir data/generated \
  --records 50000 \
  --keys 500 \
  --hot-key-ratio 0.80
```

The generated output is ignored by Git because Spark writes CSV data as folders
with part files.

## Cluster Example

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.skewJoin.enabled=true \
  --conf spark.sql.shuffle.partitions=800 \
  jobs/skew_join_optimization.py \
  --master "" \
  --records 200000000 \
  --keys 2000000 \
  --hot-key-ratio 0.70 \
  --partitions 800 \
  --salt-buckets 64
```

Use `--master ""` in the application arguments when the Spark master is already
provided by `spark-submit`.

## AWS Glue Deployment

AWS Glue usually expects:

1. A Glue job script in S3.
2. A `.zip` file containing extra Python modules.
3. Job parameters that point to S3 input paths and tuning values.

Build the Glue package:

```bash
make package-glue
```

This creates:

```text
dist/skew_join_optimization_glue.zip
```

Upload these files to S3:

```bash
aws s3 cp glue/skew_join_optimization_glue.py s3://YOUR_BUCKET/glue/scripts/skew_join_optimization_glue.py
aws s3 cp dist/skew_join_optimization_glue.zip s3://YOUR_BUCKET/glue/libs/skew_join_optimization_glue.zip
```

Use the uploaded script as the Glue job script. Add this job parameter:

```text
--extra-py-files s3://YOUR_BUCKET/glue/libs/skew_join_optimization_glue.zip
```

For an S3 input-data run, add arguments like:

```text
--fact-path s3://YOUR_BUCKET/data/fact_events/
--customers-path s3://YOUR_BUCKET/data/customer_dim/
--watermark-start 2026-05-02T00:00:00
--watermark-end 2026-05-03T00:00:00
--output-path s3://YOUR_BUCKET/curated/optimized_events/
--processing-date 2026-05-02
--partitions 800
--salt-buckets 64
```

For a generated-data benchmark run, omit `--fact-path` and `--customers-path`
and pass:

```text
--records 200000000
--keys 2000000
--hot-key-ratio 0.70
--partitions 800
--salt-buckets 64
```

Scheduling options:

- Use an AWS Glue Trigger for hourly, daily, weekly, or cron-style schedules.
- Use Amazon EventBridge Scheduler if you want centralized scheduling outside
  Glue workflows.
- Use a Glue Workflow when this job is one step in a larger pipeline.

## Idempotency And Incremental Loads

The benchmark-only mode is read-only, so it is naturally safe to rerun. For a
real scheduled pipeline, use these arguments:

```text
--watermark-start 2026-05-02T00:00:00
--watermark-end 2026-05-03T00:00:00
--output-path s3://YOUR_BUCKET/curated/optimized_events/
--processing-date 2026-05-02
```

Incremental behavior:

- `--watermark-start` is inclusive.
- `--watermark-end` is exclusive.
- The filter is applied to `event_ts`.
- For daily scheduling, pass yesterday's start and today's start as the window.

Idempotent behavior:

- The optimized output is written partitioned by `processing_date`.
- The writer uses `overwrite` mode with dynamic partition overwrite enabled.
- Rerunning the same window with the same `--processing-date` replaces that
  partition instead of appending duplicates.
- Different processing dates land in different partitions.

Example output layout:

```text
s3://YOUR_BUCKET/curated/optimized_events/
└── processing_date=2026-05-02/
    └── part-*.parquet
```

For AWS Glue, you can compute and pass watermarks from:

- Glue Trigger or EventBridge schedule parameters.
- A workflow parameter.
- A control table in DynamoDB, RDS, or S3.
- Glue Job Bookmarks for supported source types.

## How The Job Works

The fact table intentionally creates one hot key:

```text
customer_id = 1
```

By default, about 70% of generated fact records use that key. The remaining rows
are spread across many other keys. The customer dimension table is small, so it
is a good broadcast join candidate.

The benchmark runs three strategies:

1. **Baseline shuffle join:** joins directly on `customer_id`.
2. **Repartitioned broadcast join:** repartitions fact data by `customer_id` and
   broadcasts the customer dimension.
3. **Salted broadcast join:** adds a `salt` column to hot-key fact rows,
   duplicates the hot-key dimension row across salt buckets, repartitions by
   `customer_id` and `salt`, then broadcasts the salted dimension.

## Salting Strategy

Without salting, all hot-key records join on the same key:

```text
customer_id = 1
```

With salting, the hot key is split across artificial buckets:

```text
customer_id = 1, salt = 0
customer_id = 1, salt = 1
customer_id = 1, salt = 2
customer_id = 1, salt = 3
```

The dimension row for `customer_id = 1` is duplicated across all salt buckets.
Non-hot keys keep `salt = 0`, so the dimension table is only expanded where it
is needed.

## Performance Tuning Notes

Useful Spark settings for skewed joins:

```bash
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
--conf spark.sql.shuffle.partitions=800
--conf spark.sql.autoBroadcastJoinThreshold=104857600
```

Tuning guidance:

- Increase `--partitions` for larger datasets to create enough parallel tasks.
- Increase `--salt-buckets` when the hot key still creates straggler tasks.
- Keep broadcast joins for genuinely small dimension tables.
- Avoid over-salting because it duplicates dimension rows.
- Compare Spark UI task duration, shuffle read, spill, and skew metrics before
  and after changes.

## Expected Result

On real workloads, these techniques reduce long-tail tasks and shuffle pressure.
In the project scenario, repartitioning, broadcast joins, and salting reduced
runtime from more than 3 hours to about 50 minutes, about a 70% improvement.

On tiny sample data, salting can be slower because the overhead dominates. The
benefit appears when the hot key is large enough to create shuffle stragglers.
