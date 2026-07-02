"""
airflow/dags/utils/monitoring.py
==================================
Pipeline metrics tracking and monitoring utilities.

Tracks execution times, record counts, and error rates per DAG run.
Writes metrics to Airflow's XCom for cross-task visibility and optionally
pushes to a simple JSON log file for external monitoring tools.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

METRICS_LOG_DIR: str = os.getenv(
    "RUPIAH_METRICS_DIR",
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "metrics"),
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class StageMetrics:
    """Metrics for a single pipeline stage (extract / transform / load)."""

    stage_name:      str
    start_time:      float = field(default_factory=time.monotonic)
    end_time:        float = 0.0
    duration_seconds: float = 0.0
    records_in:      int = 0
    records_out:     int = 0
    records_failed:  int = 0
    error_count:     int = 0
    warning_count:   int = 0
    success:         bool = True
    extra:           dict[str, Any] = field(default_factory=dict)

    def finish(self, success: bool = True) -> "StageMetrics":
        self.end_time = time.monotonic()
        self.duration_seconds = round(self.end_time - self.start_time, 3)
        self.success = success
        return self

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PipelineRunMetrics:
    """Aggregated metrics for a complete pipeline run."""

    dag_id:           str
    run_id:           str
    execution_date:   str
    stages:           dict[str, StageMetrics] = field(default_factory=dict)
    pipeline_start:   float = field(default_factory=time.monotonic)
    pipeline_end:     float = 0.0
    total_duration:   float = 0.0
    total_records:    int = 0
    processed_records: int = 0
    failed_records:   int = 0
    success:          bool = True
    errors:           list[str] = field(default_factory=list)

    def add_stage(self, metrics: StageMetrics) -> None:
        self.stages[metrics.stage_name] = metrics
        if not metrics.success:
            self.success = False

    def finish(self) -> "PipelineRunMetrics":
        self.pipeline_end = time.monotonic()
        self.total_duration = round(self.pipeline_end - self.pipeline_start, 3)
        self.total_records = sum(s.records_in for s in self.stages.values())
        self.processed_records = sum(s.records_out for s in self.stages.values())
        self.failed_records = sum(s.records_failed for s in self.stages.values())
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "dag_id":            self.dag_id,
            "run_id":            self.run_id,
            "execution_date":    self.execution_date,
            "total_duration_s":  self.total_duration,
            "total_records":     self.total_records,
            "processed_records": self.processed_records,
            "failed_records":    self.failed_records,
            "success":           self.success,
            "errors":            self.errors,
            "stages": {
                name: s.to_dict() for name, s in self.stages.items()
            },
        }

    def summary(self) -> dict[str, Any]:
        """Compact summary suitable for XCom and Slack."""
        return {
            "success":           self.success,
            "total_records":     self.total_records,
            "processed_records": self.processed_records,
            "failed_records":    self.failed_records,
            "duration_seconds":  self.total_duration,
        }


# ---------------------------------------------------------------------------
# Monitoring functions
# ---------------------------------------------------------------------------

def log_stage_start(stage_name: str, context: dict[str, Any] | None = None) -> StageMetrics:
    """
    Log the start of a pipeline stage and return a StageMetrics object.

    Call :meth:`StageMetrics.finish` when the stage completes and pass
    the result to :func:`log_stage_end`.

    Example
    -------
    >>> metrics = log_stage_start("extract")
    >>> # ... do work ...
    >>> log_stage_end(metrics.finish(success=True), context=context)
    """
    logger.info("[monitoring] Stage '%s' started at %s", stage_name, datetime.now(timezone.utc).isoformat())
    return StageMetrics(stage_name=stage_name)


def log_stage_end(metrics: StageMetrics, context: dict[str, Any] | None = None) -> None:
    """Log stage completion and push metrics to XCom if context is provided."""
    status = "SUCCESS" if metrics.success else "FAILED"
    logger.info(
        "[monitoring] Stage '%s' %s in %.3fs | "
        "records_in=%d records_out=%d failed=%d errors=%d",
        metrics.stage_name,
        status,
        metrics.duration_seconds,
        metrics.records_in,
        metrics.records_out,
        metrics.records_failed,
        metrics.error_count,
    )

    if context and (ti := context.get("task_instance")):
        ti.xcom_push(
            key=f"metrics_{metrics.stage_name}",
            value=metrics.to_dict(),
        )


def log_pipeline_metrics(run_metrics: PipelineRunMetrics) -> None:
    """
    Log and persist full pipeline run metrics.

    Writes a JSON file to RUPIAH_METRICS_DIR (mounted volume in Docker)
    so external monitoring tools (Grafana, Datadog, etc.) can ingest it.
    """
    run_metrics.finish()
    d = run_metrics.to_dict()

    # Console log
    logger.info(
        "[monitoring] Pipeline '%s' run '%s' finished | "
        "success=%s duration=%.1fs processed=%d failed=%d",
        run_metrics.dag_id,
        run_metrics.run_id,
        run_metrics.success,
        run_metrics.total_duration,
        run_metrics.processed_records,
        run_metrics.failed_records,
    )

    # JSON file (best-effort — never fail the DAG because of metrics I/O)
    try:
        os.makedirs(METRICS_LOG_DIR, exist_ok=True)
        filename = (
            f"{run_metrics.dag_id}__{run_metrics.run_id.replace(':', '_')}.json"
        )
        path = os.path.join(METRICS_LOG_DIR, filename)
        with open(path, "w") as f:
            json.dump(d, f, indent=2, default=str)
        logger.debug("[monitoring] Metrics written to %s", path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[monitoring] Could not write metrics file: %s", exc)


def build_sla_miss_callback(dag_id: str):
    """
    Return a SLA-miss callback function bound to the given dag_id.
    Logs the SLA miss and delegates to Slack (if configured).

    Usage:
        DAG(..., sla_miss_callback=build_sla_miss_callback("extract_dag"))
    """
    def _sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
        logger.warning(
            "[monitoring] SLA MISS in DAG '%s' | "
            "tasks=%s blocking=%s",
            dag_id, task_list, blocking_task_list,
        )
        try:
            from dags.utils.slack_alerts import slack_sla_miss_callback
            slack_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis)
        except Exception as exc:  # noqa: BLE001
            logger.error("[monitoring] Could not send Slack SLA alert: %s", exc)

    return _sla_miss