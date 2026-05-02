"""Shared configuration objects for the Spark optimization demo."""

from __future__ import annotations

from dataclasses import dataclass


HOT_KEY = 1


@dataclass(frozen=True)
class JobConfig:
    records: int = 1_000_000
    keys: int = 10_000
    hot_key_ratio: float = 0.70
    partitions: int = 64
    salt_buckets: int = 16
    app_name: str = "skew-join-optimization"


@dataclass(frozen=True)
class JoinResult:
    name: str
    rows: int
    seconds: float
