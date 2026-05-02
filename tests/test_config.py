from __future__ import annotations

import pytest

from skew_join_optimization.benchmark import validate_config
from skew_join_optimization.config import JobConfig


def test_validate_config_accepts_defaults() -> None:
    validate_config(JobConfig())


def test_validate_config_rejects_invalid_hot_key_ratio() -> None:
    with pytest.raises(ValueError, match="hot_key_ratio"):
        validate_config(JobConfig(hot_key_ratio=1.2))
