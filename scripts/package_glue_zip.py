#!/usr/bin/env python3
"""Create a Glue-compatible zip with the project source package."""

from __future__ import annotations

import argparse
import zipfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
DEFAULT_OUTPUT = PROJECT_ROOT / "dist" / "skew_join_optimization_glue.zip"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Package src/ as an AWS Glue --extra-py-files zip.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output zip path.")
    return parser.parse_args()


def should_include(path: Path) -> bool:
    return path.is_file() and path.suffix == ".py" and "__pycache__" not in path.parts


def build_zip(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(output_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        for path in sorted(SRC_ROOT.rglob("*.py")):
            if should_include(path):
                zip_file.write(path, path.relative_to(SRC_ROOT))

    print(f"Wrote {output_path}")


def main() -> None:
    args = parse_args()
    build_zip(Path(args.output))


if __name__ == "__main__":
    main()
