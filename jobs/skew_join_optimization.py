#!/usr/bin/env python3
"""Spark submit entrypoint for the skew join optimization demo."""

from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
JOB_DIR = Path(__file__).resolve().parent

sys.path = [str(SRC_DIR)] + [path for path in sys.path if Path(path or ".").resolve() != JOB_DIR]

from skew_join_optimization.cli import main


if __name__ == "__main__":
    main()
