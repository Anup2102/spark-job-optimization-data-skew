.PHONY: setup test run run-sample run-incremental generate-sample package-glue clean

setup:
	python3 -m pip install -r requirements.txt

test:
	PYTHONPATH=src python3 -m pytest -q

run:
	PYTHONPATH=src python3 jobs/skew_join_optimization.py --records 100000 --keys 1000 --partitions 16 --salt-buckets 8

run-sample:
	PYTHONPATH=src python3 jobs/skew_join_optimization.py --fact-path data/sample/fact_events.csv --customers-path data/sample/customer_dim.csv --partitions 4 --salt-buckets 4

run-incremental:
	PYTHONPATH=src python3 jobs/skew_join_optimization.py --fact-path data/sample/fact_events.csv --customers-path data/sample/customer_dim.csv --watermark-start "2026-05-02 10:00:00" --watermark-end "2026-05-02 10:30:00" --output-path data/generated/optimized_events --processing-date 2026-05-02 --partitions 4 --salt-buckets 4

generate-sample:
	PYTHONPATH=src python3 scripts/generate_sample_data.py

package-glue:
	python3 scripts/package_glue_zip.py

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
	find . -type d -name .pytest_cache -prune -exec rm -rf {} +
	rm -rf dist
