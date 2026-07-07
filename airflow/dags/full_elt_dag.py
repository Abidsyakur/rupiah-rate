"""
airflow/dags/full_elt_dag.py
==============================
DAG 4 — Complete ELT orchestration: extract_dag → load_dag → transform_dag.

Schedule : Daily at 02:00 UTC  (0 2 * * *)  — same time as extract_dag,
           since this DAG triggers extract_dag itself rather than running
           in parallel with it.
Tasks    : trigger_extract → trigger_load → trigger_transform → notify_summary
Alerts   : SLA alerts + Slack summary on completion
SLA      : 2 hours (covers all three sub-pipelines end to end)

ELT, not ETL (renamed from full_etl_dag.py)
--------------------------------------------
This DAG was originally full_etl_dag.py with order
extract -> transform -> load. That order is WRONG for a dbt-based
pipeline: dbt (transform_dag) transforms data that must ALREADY be
loaded into the warehouse — dbt is the "T" in ELT, not a data-loading
tool itself. Running transform before load meant dbt operated on
empty/stale raw tables, which produced incorrect mart data and caused
load_dag's own dependency sensor to time out waiting for a transform
run that could never meaningfully "complete" the pipeline correctly.

The corrected order is:
    extract_dag  (fetch + validate, writes staging JSON)
    -> load_dag    (writes raw rates into exchange_rates table)
    -> transform_dag (dbt: staging -> intermediate -> marts, using the
                       raw data load_dag just wrote)

Design note
-----------
This DAG uses TriggerDagRunOperator with wait_for_completion=True so it
behaves as a single sequential "meta-pipeline" — useful for manual re-runs
("run the whole thing now") and for a single Airflow UI view showing
overall pipeline health, without duplicating the extract/load/transform
task logic itself (that stays in the three dedicated DAGs).

Each TriggerDagRunOperator passes conf={"triggered_by": "full_elt_dag"}
so the sub-DAGs' own dependency sensors (see utils/helpers.py's
wait_for_recent_success) skip their "wait for a recent successful
upstream run" check entirely — wait_for_completion=True here already
guarantees strict ordering, so the sub-DAG sensors would otherwise be
redundant (and, worse, could race/timeout if the upstream run's success
timestamp hasn't propagated yet).

If extract_dag, load_dag, and transform_dag are also independently
scheduled (02:00 / 03:00 / 04:00), disable their own schedules
(schedule_interval=None) when using full_elt_dag as the sole entry point,
to avoid double-running the pipeline.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from dags.config import PipelineDAGConfig
from dags.constants import (
    DAG_ID_EXTRACT,
    DAG_ID_FULL_ELT,
    DAG_ID_LOAD,
    DAG_ID_TRANSFORM,
    LOAD_SUMMARY_XCOM_KEY,
    TRANSFORM_SUMMARY_XCOM_KEY,
    SLA_FULL_ELT,
)
from dags.utils.helpers import on_failure_callback, utcnow_iso
from dags.utils.monitoring import build_sla_miss_callback, log_stage_end, log_stage_start
from dags.utils.slack_alerts import send_pipeline_summary

logger = logging.getLogger(__name__)
cfg = PipelineDAGConfig()

_DEFAULT_ARGS = {
    "owner":                     "rupiah-pipeline",
    "depends_on_past":           False,
    "email_on_failure":          True,
    "retries":                   1,           # sub-DAGs already retry internally
    "retry_delay":               timedelta(minutes=10),
    "on_failure_callback":       on_failure_callback,
    "sla":                       SLA_FULL_ELT,
}


@dag(
    dag_id=DAG_ID_FULL_ELT,
    description="Complete ELT orchestration: extract → load → transform, run sequentially.",
    default_args=_DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rupiah", "orchestration", "full-pipeline", "elt"],
    sla_miss_callback=build_sla_miss_callback(DAG_ID_FULL_ELT),
    doc_md=__doc__,
)
def full_elt_dag():

    log_stage_start("full_elt")

    # ------------------------------------------------------------------ #
    # Step 1 — Trigger extract_dag and wait for completion
    #
    # conf={"triggered_by": "full_elt_dag"} lets load_dag/transform_dag's
    # sensors (see utils/helpers.wait_for_recent_success) skip their own
    # "recent success" check entirely, since wait_for_completion=True here
    # already guarantees strict ordering — avoids redundant/confusing
    # double-waiting.
    # ------------------------------------------------------------------ #
    trigger_extract = TriggerDagRunOperator(
        task_id="trigger_extract_dag",
        trigger_dag_id=DAG_ID_EXTRACT,
        conf={"triggered_by": "full_elt_dag"},
        wait_for_completion=True,
        poke_interval=30,
        execution_timeout=timedelta(minutes=35),
        failed_states=["failed"],
        reset_dag_run=True,
        doc_md="Triggers extract_dag and blocks until it succeeds or fails.",
    )

    # ------------------------------------------------------------------ #
    # Step 2 — Trigger load_dag and wait for completion
    #
    # ELT ORDER: load runs BEFORE transform — dbt needs the raw rows
    # load_dag writes into exchange_rates before it can transform them.
    # ------------------------------------------------------------------ #
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id=DAG_ID_LOAD,
        conf={"triggered_by": "full_elt_dag"},
        wait_for_completion=True,
        poke_interval=30,
        execution_timeout=timedelta(minutes=25),
        failed_states=["failed"],
        reset_dag_run=True,
        doc_md="Triggers load_dag and blocks until it succeeds or fails.",
    )

    # ------------------------------------------------------------------ #
    # Step 3 — Trigger transform_dag and wait for completion
    # ------------------------------------------------------------------ #
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id=DAG_ID_TRANSFORM,
        conf={"triggered_by": "full_elt_dag"},
        wait_for_completion=True,
        poke_interval=30,
        execution_timeout=timedelta(minutes=50),
        failed_states=["failed"],
        reset_dag_run=True,
        doc_md="Triggers transform_dag and blocks until it succeeds or fails.",
    )

    # ------------------------------------------------------------------ #
    # Step 4 — Notify summary (runs regardless of upstream success/failure)
    # ------------------------------------------------------------------ #
    @task(task_id="notify_summary", trigger_rule=TriggerRule.ALL_DONE)
    def notify_summary(**context) -> dict:
        """
        Post an end-to-end pipeline summary to Slack (and log it),
        regardless of whether every upstream DAG succeeded — this task
        always runs (TriggerRule.ALL_DONE) so failures are also reported.
        """
        stage = log_stage_start("notify_summary")

        ti = context.get("task_instance")
        dag_run = context.get("dag_run")

        # Determine overall success by inspecting upstream task states
        upstream_task_ids = ["trigger_extract_dag", "trigger_load_dag", "trigger_transform_dag"]
        upstream_states = {}
        if dag_run:
            for tid in upstream_task_ids:
                task_instance = dag_run.get_task_instance(tid)
                upstream_states[tid] = task_instance.state if task_instance else "unknown"

        overall_success = all(state == "success" for state in upstream_states.values())

        # Pull load_dag's and transform_dag's summaries via cross-DAG XCom
        # (best-effort only — these are for the Slack notification's
        # detail fields; a missing summary here does not affect
        # overall_success, which is derived from task STATE above).
        load_summary = transform_summary = None
        if ti:
            load_summary = ti.xcom_pull(
                dag_id=DAG_ID_LOAD,
                task_ids="record_load_summary",
                key=LOAD_SUMMARY_XCOM_KEY,
                include_prior_dates=True,
            )
            transform_summary = ti.xcom_pull(
                dag_id=DAG_ID_TRANSFORM,
                task_ids="record_transform_summary",
                key=TRANSFORM_SUMMARY_XCOM_KEY,
                include_prior_dates=True,
            )

        summary = {
            "success":           overall_success,
            "upstream_states":   upstream_states,
            "total_records":     0,
            "processed_records": 0,
            "failed_records":    0,
            "duration_seconds":  0.0,
        }

        if load_summary:
            summary.update(load_summary.get(LOAD_SUMMARY_XCOM_KEY, {}))
        if transform_summary:
            summary["transform"] = transform_summary.get(TRANSFORM_SUMMARY_XCOM_KEY, {})

        stage.finish(success=overall_success)
        log_stage_end(stage, context)

        logger.info(
            "[full_elt_dag] Pipeline complete: success=%s upstream_states=%s",
            overall_success, upstream_states,
        )

        # Send Slack summary (no-op if SLACK_WEBHOOK_URL is unset)
        send_pipeline_summary(
            dag_id=DAG_ID_FULL_ELT,
            run_id=context.get("run_id", "unknown"),
            summary=summary,
        )

        if not overall_success:
            raise RuntimeError(
                f"[full_elt_dag] One or more sub-pipelines failed: {upstream_states}"
            )

        return summary

    # ------------------------------------------------------------------ #
    # Wiring — strictly sequential, ELT order: extract -> load -> transform
    # ------------------------------------------------------------------ #
    trigger_extract >> trigger_load >> trigger_transform >> notify_summary()


full_elt_dag()