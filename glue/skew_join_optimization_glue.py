#!/usr/bin/env python3
"""AWS Glue entrypoint for the skew join optimization job.

Upload this file to S3 as the Glue job script and upload the package zip built
by scripts/package_glue_zip.py as --extra-py-files.
"""

from __future__ import annotations

from skew_join_optimization.cli import parse_args, run


def main() -> None:
    args = parse_args(ignore_unknown=True)
    run(args, master_override="")


if __name__ == "__main__":
    main()
