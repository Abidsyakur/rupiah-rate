"""
airflow/dags/transform_dag.py
==============================
DAG 2 — Transform data with dbt (staging → intermediate → marts).

Schedule : Daily at 03:00 UTC  (0 3 * * *)
Tasks    : dbt_deps → dbt_run_staging → dbt_run_intermediate →
           dbt_run_marts → dbt_test → dbt_docs (optional)
Depends  : extract_dag must complete successfully first
Retries  : 3 with exponential backoff
Alerts   : Email + Slack on failure
SLA      : 45 minutes
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from dags.config import PipelineDAGConfig
from dags.constants import (
    DAG_ID_EXTRACT,
    DAG_ID_TRANSFORM,
    SLA_TRANSFORM,
    TRANSFORM_SUMMARY_XCOM_KEY,
)
from dags.utils.helpers import on_failure_callback, utcnow_iso, xcom_push_summary
from dags.utils.monitoring import build_sla_miss_callback, log_stage_end, log_stage_start

logger = logging.getLogger(__name__)
cfg = PipelineDAGConfig()

# ---------------------------------------------------------------------------
# dbt command builder
# ---------------------------------------------------------------------------
_DBT_BASE = (
    f"cd {os.getenv('RUPIAH_PROJECT_ROOT', '/opt/airflow/project')}/dbt && "
    f"dbt {{cmd}} "
    f"--project-dir . "
    f"--profiles-dir . "
    f"--target {os.getenv('DBT_TARGET', 'prod')} "
    f"--threads {cfg.dbt_threads}"
)

_DEFAULT_ARGS = {
    "owner":                     "rupiah-pipeline",
    "depends_on_past":           False,
    "email_on_failure":          True,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=30),
    "on_failure_callback":       on_failure_callback,
    "sla":                       SLA_TRANSFORM,
}


@dag(
    dag_id=DAG_ID_TRANSFORM,
    description="Run dbt transformations: staging → intermediate → marts → test → docs.",
    default_args=_DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rupiah", "transform", "dbt"],
    sla_miss_callback=build_sla_miss_callback(DAG_ID_TRANSFORM),
    doc_md=__doc__,
)
def transform_dag():

    # ------------------------------------------------------------------ #
    # Sensor: wait for extract_dag to finish today's run
    # ------------------------------------------------------------------ #
    wait_for_extract = ExternalTaskSensor(
        task_id="wait_for_extract_dag",
        external_dag_id=DAG_ID_EXTRACT,
        external_task_id=None,          # wait for entire DAG, not a specific task
        allowed_states=["success"],
        execution_delta=timedelta(hours=1),   # extract_dag runs at 02:00, we run at 03:00
        timeout=1800,
        poke_interval=60,
        mode="reschedule",
        doc_md="Wait for today's extract_dag run to complete before transforming.",
    )

    # ------------------------------------------------------------------ #
    # Task 1 — dbt deps (install/refresh packages)
    # ------------------------------------------------------------------ #
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=_DBT_BASE.format(cmd="deps"),
        doc_md="Install/update dbt packages (dbt_utils, dbt_expectations).",
    )

    # ------------------------------------------------------------------ #
    # Task 2 — dbt run: staging layer
    # ------------------------------------------------------------------ #
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=_DBT_BASE.format(cmd="run") + " --select staging",
        doc_md=(
            "Materialise all staging views: stg_currencies, stg_api_sources, "
            "stg_exchange_rates, stg_api_calls, stg_data_quality_metrics."
        ),
    )

    # ------------------------------------------------------------------ #
    # Task 3 — dbt run: intermediate layer
    # ------------------------------------------------------------------ #
    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=_DBT_BASE.format(cmd="run") + " --select intermediate",
        doc_md=(
            "Materialise intermediate tables: int_daily_exchange_rates, "
            "int_exchange_rate_statistics, int_api_performance, int_quality_summary."
        ),
    )

    # ------------------------------------------------------------------ #
    # Task 4 — dbt run: marts layer
    # ------------------------------------------------------------------ #
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=_DBT_BASE.format(cmd="run") + " --select marts",
        doc_md=(
            "Materialise mart tables: dim_currencies, dim_api_sources, "
            "dim_quality_metrics, fct_exchange_rates, fct_daily_snapshots."
        ),
    )

    # ------------------------------------------------------------------ #
    # Task 5 — dbt test (all layers)
    # ------------------------------------------------------------------ #
    fail_fast_flag = "--fail-fast" if cfg.dbt_fail_fast else ""
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{_DBT_BASE.format(cmd='test')} {fail_fast_flag}".strip(),
        doc_md=(
            "Run all dbt tests: generic (unique, not_null, accepted_values, "
            "relationships) + singular (test_no_duplicate_rates, "
            "test_rate_consistency, test_quality_scores)."
        ),
    )

    # ------------------------------------------------------------------ #
    # Task 6 — dbt docs generate (optional, controlled by config)
    # ------------------------------------------------------------------ #
    @task.branch(task_id="should_generate_docs")
    def should_generate_docs() -> str:
        return "dbt_docs_generate" if cfg.run_dbt_docs else "record_transform_summary"

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=_DBT_BASE.format(cmd="docs generate"),
        doc_md="Generate dbt documentation artifacts (catalog.json, manifest.json).",
    )

    # ------------------------------------------------------------------ #
    # Task 7 — Record transform summary to XCom
    # ------------------------------------------------------------------ #
    @task(task_id="record_transform_summary", trigger_rule="none_failed_min_one_success")
    def record_transform_summary(**context) -> dict:
        """Push a transform summary dict to XCom for load_dag and full_etl_dag."""
        stage = log_stage_start("transform_summary")
        summary = {
            TRANSFORM_SUMMARY_XCOM_KEY: {
                "dbt_stages_run":  ["staging", "intermediate", "marts"],
                "tests_run":       True,
                "docs_generated":  cfg.run_dbt_docs,
                "completed_at":    utcnow_iso(),
            }
        }
        xcom_push_summary(context, TRANSFORM_SUMMARY_XCOM_KEY, summary)
        stage.finish(success=True)
        log_stage_end(stage, context)
        logger.info("[transform_dag] Transform summary recorded.")
        return summary

    # ------------------------------------------------------------------ #
    # Task wiring
    # ------------------------------------------------------------------ #
    branch      = should_generate_docs()
    summary     = record_transform_summary()

    (
        wait_for_extract
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test
        >> branch
    )
    branch >> dbt_docs_generate >> summary
    branch >> summary


transform_dag()