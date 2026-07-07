"""
airflow/dags/transform_dag.py
==============================
DAG 3 (in ELT order) — Transform data with dbt (staging → intermediate → marts).

Schedule : Daily at 04:00 UTC  (0 4 * * *)
Tasks    : dbt_deps → dbt_seed → dbt_run_staging → dbt_run_intermediate →
           dbt_run_marts → dbt_test → dbt_docs (optional)
Depends  : load_dag must complete successfully first (dbt transforms
           raw tables that load_dag populates — see ELT note below)
Retries  : 3 with exponential backoff
Alerts   : Email + Slack on failure
SLA      : 45 minutes

ELT, not ETL
------------
dbt is fundamentally an ELT tool: it transforms data that is ALREADY
loaded into the warehouse, it does not extract or load raw data itself.
This project's pipeline is therefore Extract -> Load -> Transform, NOT
Extract -> Transform -> Load. transform_dag depends on load_dag
(not extract_dag), because dbt's staging models
(e.g. stg_exchange_rates.sql) read from {{ source('raw', 'exchange_rates') }}
tables, which load_dag's ExchangeRateLoader populates.

See airflow/dags/full_elt_dag.py for the end-to-end orchestration order.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from dags.config import PipelineDAGConfig
from dags.constants import (
    DAG_ID_LOAD,
    DAG_ID_TRANSFORM,
    SLA_TRANSFORM,
    TRANSFORM_SUMMARY_XCOM_KEY,
)
from dags.utils.helpers import (
    on_failure_callback,
    utcnow_iso,
    wait_for_recent_success,
    xcom_push_summary,
)
from dags.utils.monitoring import build_sla_miss_callback, log_stage_end, log_stage_start

logger = logging.getLogger(__name__)
cfg = PipelineDAGConfig()

# ---------------------------------------------------------------------------
# dbt command builder
# ---------------------------------------------------------------------------
_DBT_PROJECT_ROOT = os.getenv('RUPIAH_PROJECT_ROOT', '/opt/airflow/project')

# `dbt deps` only installs packages from packages.yml — it never connects
# to the database, so it does NOT accept --target or --threads (both are
# rejected with "No such option"). Give it its own minimal command.
_DBT_DEPS_CMD = (
    f"cd {_DBT_PROJECT_ROOT}/dbt && "
    f"dbt deps "
    f"--project-dir . "
    f"--profiles-dir ."
)

# All other commands (run/test/docs generate/etc.) do connect to the
# database and accept --target/--threads.
_DBT_BASE = (
    f"cd {_DBT_PROJECT_ROOT}/dbt && "
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
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rupiah", "transform", "dbt"],
    sla_miss_callback=build_sla_miss_callback(DAG_ID_TRANSFORM),
    doc_md=__doc__,
)
def transform_dag():

    # ------------------------------------------------------------------ #
    # Sensor: wait for load_dag to have a recent successful run.
    #
    # ELT ORDER FIX: transform_dag now depends on load_dag (not
    # extract_dag directly). dbt's staging models
    # (stg_exchange_rates.sql etc.) read from {{ source('raw', ...) }}
    # tables — which are populated by load_dag's ExchangeRateLoader, not
    # by extract_dag itself (extract only fetches + validates in-memory,
    # it never writes to the database). Running dbt before load_dag
    # means dbt transforms EMPTY or STALE raw tables, which is exactly
    # what was happening before this fix and why downstream tasks
    # (including load_dag's own sensor waiting on transform_dag) kept
    # timing out — the correct order is Extract -> Load -> Transform.
    # ------------------------------------------------------------------ #
    wait_for_load = PythonSensor(
        task_id="wait_for_load_dag",
        python_callable=wait_for_recent_success(DAG_ID_LOAD, within_hours=6.0),
        poke_interval=60,
        timeout=1800,
        mode="reschedule",
        doc_md="Wait for a recent successful load_dag run (skipped if chained from full_elt_dag).",
    )

    # ------------------------------------------------------------------ #
    # Task 1 — dbt deps (install/refresh packages)
    # ------------------------------------------------------------------ #
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=_DBT_DEPS_CMD,
        doc_md="Install/update dbt packages (dbt_utils, dbt_expectations).",
    )

    # ------------------------------------------------------------------ #
    # Task 1b — dbt seed (materialise reference data, e.g. seeds/currencies.csv)
    #
    # BUG FIX: dbt_test was failing with "relation analytics.currencies
    # does not exist" for every test defined in seeds/schema.yml. Those
    # tests target the currencies SEED table, which only gets created in
    # the database when `dbt seed` runs — this DAG previously never ran
    # it, only `dbt run` (which only builds models, not seeds).
    # ------------------------------------------------------------------ #
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=_DBT_BASE.format(cmd="seed"),
        doc_md=(
            "Materialise seed files (dbt/seeds/currencies.csv) into the "
            "database so seeds/schema.yml tests have a table to run against."
        ),
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
    # Task 6b — Export mart tables to staging area (CSV snapshots)
    #
    # MOVED HERE from load_dag.py: this task queries marts.* tables,
    # which dbt_run_marts (above) just built/refreshed. It cannot run
    # before dbt has materialised the marts.
    # ------------------------------------------------------------------ #
    @task(task_id="export_marts")
    def export_marts(**context) -> dict:
        """
        Export key mart tables to CSV files in the staging directory so
        downstream consumers (data warehouse, BI tools) can pick them up
        independently of the pipeline's DB connection.
        """
        import csv
        import os
        from dags.constants import STAGING_DIR

        stage = log_stage_start("export_marts")
        os.makedirs(STAGING_DIR, exist_ok=True)

        exported: dict[str, int] = {}

        try:
            from src.utils.database import get_engine, get_session
            from sqlalchemy import text

            engine = get_engine()
            mart_queries = {
                "fct_daily_snapshots": "SELECT * FROM marts.fct_daily_snapshots WHERE rate_date = current_date - 1",
                "fct_exchange_rates":  "SELECT * FROM marts.fct_exchange_rates WHERE rate_date = current_date - 1",
                "dim_currencies":      "SELECT * FROM marts.dim_currencies",
            }

            with get_session(engine) as session:
                for table, query in mart_queries.items():
                    try:
                        rows = session.execute(text(query)).fetchall()
                        if not rows:
                            logger.info("[export_marts] %s: no rows for yesterday.", table)
                            exported[table] = 0
                            continue

                        filepath = os.path.join(STAGING_DIR, f"{table}.csv")
                        with open(filepath, "w", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow(rows[0]._fields)
                            writer.writerows(rows)

                        exported[table] = len(rows)
                        logger.info("[export_marts] %s: %d rows exported to %s", table, len(rows), filepath)
                    except Exception as exc:
                        logger.warning("[export_marts] Could not export %s: %s", table, exc)
                        exported[table] = -1

        except Exception as exc:
            logger.warning("[export_marts] DB export skipped (non-fatal): %s", exc)

        stage.records_out = sum(v for v in exported.values() if v > 0)
        stage.finish(success=True)
        log_stage_end(stage, context)

        summary = {"exported_tables": exported, "exported_at": utcnow_iso(), "staging_dir": STAGING_DIR}
        xcom_push_summary(context, "export_summary", summary)
        return summary

    # ------------------------------------------------------------------ #
    # Task 6c — Refresh materialized views / caches
    #
    # MOVED HERE from load_dag.py: these views are built on top of
    # marts.* tables, so they must refresh AFTER dbt_run_marts, not before.
    # ------------------------------------------------------------------ #
    @task(task_id="refresh_cache")
    def refresh_cache(**context) -> dict:
        """
        Refresh PostgreSQL materialized views (if any) so BI tools see the
        latest mart data immediately. Extend this task to call your BI
        tool's API (e.g. Metabase, Superset) if needed.
        """
        stage = log_stage_start("refresh_cache")
        refreshed: list[str] = []

        try:
            from src.utils.database import get_engine, get_session
            from sqlalchemy import text

            engine = get_engine()
            mat_views = ["marts.mv_latest_rates", "marts.mv_daily_summary"]
            with get_session(engine) as session:
                for view in mat_views:
                    try:
                        session.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
                        refreshed.append(view)
                        logger.info("[refresh_cache] Refreshed %s", view)
                    except Exception:
                        logger.debug("[refresh_cache] %s does not exist — skipping.", view)

        except Exception as exc:
            logger.warning("[refresh_cache] Cache refresh skipped (non-fatal): %s", exc)

        stage.finish(success=True)
        log_stage_end(stage, context)

        summary = {"refreshed_views": refreshed, "refreshed_at": utcnow_iso()}
        xcom_push_summary(context, "refresh_cache_summary", summary)
        return summary

    # ------------------------------------------------------------------ #
    # Task 7 — Record transform summary to XCom
    # ------------------------------------------------------------------ #
    @task(task_id="record_transform_summary", trigger_rule="none_failed_min_one_success")
    def record_transform_summary(**context) -> dict:
        """Push a transform summary dict to XCom for full_elt_dag."""
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
    #
    # dbt_test fans out into two parallel paths that both converge on
    # record_transform_summary:
    #   1. should_generate_docs -> [dbt_docs_generate | skip]
    #   2. export_marts -> refresh_cache   (needs marts to already exist)
    # ------------------------------------------------------------------ #
    branch       = should_generate_docs()
    exports      = export_marts()
    cache        = refresh_cache()
    summary      = record_transform_summary()

    (
        wait_for_load
        >> dbt_deps
        >> dbt_seed
        >> dbt_run_staging
        >> dbt_run_intermediate
        >> dbt_run_marts
        >> dbt_test
        >> branch
    )
    dbt_test >> exports >> cache >> summary
    branch >> dbt_docs_generate >> summary
    branch >> summary


transform_dag()