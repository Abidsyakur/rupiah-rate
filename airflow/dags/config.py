"""
airflow/dags/config.py
========================
Centralised DAG configuration class. All tuneable parameters (timeouts,
batch sizes, quality thresholds, etc.) are read from environment variables
so the same Docker image can be deployed to dev/staging/prod by changing
only docker-compose env_file or Kubernetes secrets — no code changes.

Usage
-----
    from dags.config import PipelineDAGConfig
    cfg = PipelineDAGConfig()
    print(cfg.extract_timeout)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except ValueError:
        return default


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes")


@dataclass
class PipelineDAGConfig:
    """
    Runtime configuration for all rupiah-exchange-rate DAGs.

    Every attribute maps 1:1 to an environment variable so Docker /
    docker-compose can override any value at container startup.

    Docker usage (docker-compose.yml environment block):
        PIPELINE_BATCH_SIZE: "500"
        PIPELINE_VALIDATE_QUALITY_THRESHOLD: "0.8"
    """

    # ---- Extract stage -------------------------------------------------------
    extract_timeout: int       = field(default_factory=lambda: _env_int("PIPELINE_EXTRACT_TIMEOUT", 300))
    extract_retries: int       = field(default_factory=lambda: _env_int("PIPELINE_EXTRACT_RETRIES", 3))

    # ---- Validate stage ------------------------------------------------------
    validate_quality_threshold: float = field(
        default_factory=lambda: _env_float("PIPELINE_VALIDATE_QUALITY_THRESHOLD", 0.7)
    )
    fail_on_low_quality: bool = field(
        default_factory=lambda: _env_bool("PIPELINE_FAIL_ON_LOW_QUALITY", False)
    )

    # ---- Load stage ----------------------------------------------------------
    batch_size: int      = field(default_factory=lambda: _env_int("PIPELINE_BATCH_SIZE", 1000))
    load_retries: int    = field(default_factory=lambda: _env_int("PIPELINE_LOAD_RETRY_COUNT", 3))
    track_audit: bool    = field(default_factory=lambda: _env_bool("LOADER_TRACK_AUDIT", True))

    # ---- dbt (transform stage) -----------------------------------------------
    dbt_threads: int     = field(default_factory=lambda: _env_int("DBT_THREADS", 4))
    dbt_fail_fast: bool  = field(default_factory=lambda: _env_bool("DBT_FAIL_FAST", False))
    run_dbt_docs: bool   = field(default_factory=lambda: _env_bool("DBT_RUN_DOCS", False))

    # ---- Pipeline-wide -------------------------------------------------------
    stop_on_error: bool  = field(default_factory=lambda: _env_bool("PIPELINE_STOP_ON_ERROR", False))

    def __repr__(self) -> str:
        return (
            f"PipelineDAGConfig("
            f"batch_size={self.batch_size}, "
            f"quality_threshold={self.validate_quality_threshold}, "
            f"dbt_threads={self.dbt_threads}, "
            f"stop_on_error={self.stop_on_error})"
        )