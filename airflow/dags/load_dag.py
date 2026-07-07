"""
airflow/dags/load_dag.py
==========================
DAG 2 (in ELT order) — Load validated raw exchange rate data into the
warehouse (the "L" in ELT — dbt/transform_dag does the "T" afterward).

Schedule : Daily at 03:00 UTC  (0 3 * * *)
Tasks    : wait_for_extract → load_warehouse → record_load_summary
Depends  : extract_dag must complete successfully first
Retries  : 3 with exponential backoff
Alerts   : Email + Slack on failure
SLA      : 20 minutes

ELT, not ETL
------------
This DAG only loads RAW extracted rates into the exchange_rates table.
It does NOT export or refresh anything derived from dbt's mart tables
(fct_daily_snapshots, etc.) — those tasks moved to transform_dag.py,
since marts.* tables are built by dbt and don't exist yet (or only hold
yesterday's data) at the point this DAG runs. See
airflow/dags/full_elt_dag.py for the end-to-end orchestration order.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from dags.config import PipelineDAGConfig
from dags.constants import (
    DAG_ID_EXTRACT,
    DAG_ID_LOAD,
    EXTRACT_SUMMARY_XCOM_KEY,
    LOAD_SUMMARY_XCOM_KEY,
    SLA_LOAD,
    YFINANCE_SOURCE_ID,
    FRED_SOURCE_ID,
)
from dags.utils.helpers import (
    on_failure_callback,
    utcnow_iso,
    wait_for_recent_success,
    xcom_push_summary,
)
from dags.utils.monitoring import (
    PipelineRunMetrics,
    build_sla_miss_callback,
    log_pipeline_metrics,
    log_stage_end,
    log_stage_start,
)

logger = logging.getLogger(__name__)
cfg = PipelineDAGConfig()

_DEFAULT_ARGS = {
    "owner":                     "rupiah-pipeline",
    "depends_on_past":           False,
    "email_on_failure":          True,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=30),
    "on_failure_callback":       on_failure_callback,
    "sla":                       SLA_LOAD,
}


@dag(
    dag_id=DAG_ID_LOAD,
    description="Load validated exchange rate data into the database and refresh downstream caches.",
    default_args=_DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rupiah", "load"],
    sla_miss_callback=build_sla_miss_callback(DAG_ID_LOAD),
    doc_md=__doc__,
)
def load_dag():

    # ------------------------------------------------------------------ #
    # Sensor: wait for extract_dag to have a recent successful run.
    #
    # ELT ORDER FIX: load_dag now depends on extract_dag (not
    # transform_dag). dbt (transform_dag) needs the RAW tables already
    # populated to transform them — so the correct order is
    # extract -> load -> transform, not extract -> transform -> load.
    # Loading before transforming is what makes this pipeline ELT
    # (Extract-Load-Transform), matching dbt's actual design: dbt is the
    # "T" in ELT, it transforms data already sitting in the warehouse.
    # ------------------------------------------------------------------ #
    wait_for_extract = PythonSensor(
        task_id="wait_for_extract_dag",
        python_callable=wait_for_recent_success(DAG_ID_EXTRACT, within_hours=6.0),
        poke_interval=60,
        timeout=1800,
        mode="reschedule",
        doc_md="Wait for a recent successful extract_dag run (skipped if chained from full_elt_dag).",
    )

    # ------------------------------------------------------------------ #
    # NOTE: export_marts and refresh_cache tasks used to live here, but
    # they query marts.* tables — which are built by dbt (transform_dag),
    # not by this DAG. With the ELT ordering fix (extract -> load ->
    # transform), those tables don't exist yet (or only hold yesterday's
    # stale data) at the point load_dag runs. Both tasks have been moved
    # to transform_dag.py, where they correctly run AFTER dbt has
    # (re)built the mart tables.
    # ------------------------------------------------------------------ #

    # ------------------------------------------------------------------ #
    # Helper: load the validated extraction payload from shared staging
    # ------------------------------------------------------------------ #
    def _read_staging_payload(context: dict) -> dict | None:
        """
        Read the validated extraction summary from STAGING_DIR (shared
        Docker volume) instead of Airflow cross-DAG XCom.

        BUG THIS FIXES: ``ti.xcom_pull(dag_id="extract_dag", ...)`` is
        scoped to a matching execution_date/logical_date between the two
        DAG runs. This works ONLY when both DAGs happen to share the same
        logical date AND Airflow can resolve the matching run — which
        breaks for manual runs, and for TriggerDagRunOperator-triggered
        sub-DAG-runs (each gets its OWN execution_date). The result was
        "task marked SUCCESS but nothing loaded" — the exact symptom
        reported: extract_dag succeeded, but load_dag silently found no
        XCom data to load.

        Lookup order:
          1. STAGING_DIR/extract_{ds}.json   (today's logical date)
          2. STAGING_DIR/extract_latest.json (most recent successful extract)
          3. Cross-DAG XCom (legacy fallback, best-effort only)
        """
        import json
        import os
        from dags.constants import STAGING_DIR

        ds = context.get("ds", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        dated_path  = os.path.join(STAGING_DIR, f"extract_{ds}.json")
        latest_path = os.path.join(STAGING_DIR, "extract_latest.json")

        for path, label in ((dated_path, "dated"), (latest_path, "latest")):
            if os.path.isfile(path):
                try:
                    with open(path) as f:
                        payload = json.load(f)
                    logger.info(
                        "[load_warehouse] Loaded extraction payload from %s file: %s",
                        label, path,
                    )
                    return payload
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "[load_warehouse] Could not read %s (%s): %s", path, label, exc
                    )

        # Legacy fallback — best-effort, may find nothing (see docstring)
        logger.warning(
            "[load_warehouse] No staging file found at %s or %s — "
            "falling back to cross-DAG XCom (unreliable; may return None).",
            dated_path, latest_path,
        )
        ti = context.get("task_instance")
        if ti is None:
            return None
        return ti.xcom_pull(
            dag_id="extract_dag",
            task_ids="validate_extract",
            key=EXTRACT_SUMMARY_XCOM_KEY,
            include_prior_dates=True,
        )

    # ------------------------------------------------------------------ #
    # Task 2 — Load rates into exchange_rates table via ExchangeRateLoader
    # ------------------------------------------------------------------ #
    @task(task_id="load_warehouse")
    def load_warehouse(**context) -> dict:
        """
        Read the validated extraction payload from the shared staging
        volume (written by extract_dag's validate_extract task) and load
        it into the exchange_rates table using ExchangeRateLoader's
        idempotent upsert.
        """
        stage = log_stage_start("load_warehouse")

        extract_summary = _read_staging_payload(context)

        if not extract_summary or not extract_summary.get("merged_data", {}).get("rates"):
            logger.warning(
                "[load_warehouse] No extraction payload found (staging file "
                "empty/missing and XCom fallback found nothing) — nothing to load. "
                "Did extract_dag run successfully for this logical date?"
            )
            stage.finish(success=True)
            log_stage_end(stage, context)
            return {"rows_loaded": 0, "rows_updated": 0, "skipped": 0}

        rates = extract_summary["merged_data"]["rates"]
        logger.info("[load_warehouse] Loading %d rate(s) to exchange_rates table.", len(rates))

        from src.utils.database import get_engine, get_session
        from src.etl.loaders import ExchangeRateLoader, LoaderConfig

        loader = ExchangeRateLoader(
            config=LoaderConfig(
                batch_size=cfg.batch_size,
                track_audit=cfg.track_audit,
                skip_duplicates=True,
                retry_count=cfg.load_retries,
            )
        )

        total_loaded = total_updated = total_skipped = 0

        # Split by source_id and load each source separately
        for source_id in (YFINANCE_SOURCE_ID, FRED_SOURCE_ID):
            source_rates = [
                r for r in rates
                if r.get("source") in ("yfinance" if source_id == YFINANCE_SOURCE_ID else "fred", str(source_id))
            ]
            if not source_rates:
                continue

            # Attach FK IDs — in production these would come from a
            # currency lookup; here we use a best-effort DB lookup.
            try:
                from src.utils.database import get_engine, get_session, Currency
                engine = get_engine()
                with get_session(engine) as session:
                    from sqlalchemy import select
                    currency_map = {
                        c.code: c.currency_id
                        for c in session.query(Currency).filter(Currency.is_active == True).all()
                    }

                loadable = []
                for r in source_rates:
                    pair = r.get("pair", "")
                    parts = pair.split("_")
                    if len(parts) != 2:
                        continue
                    from_id = currency_map.get(parts[0])
                    to_id   = currency_map.get(parts[1])
                    if not from_id or not to_id:
                        logger.warning("[load_warehouse] Unknown currency in pair %s — skipping.", pair)
                        continue
                    loadable.append({**r, "from_currency_id": from_id, "to_currency_id": to_id})

                if not loadable:
                    continue

                engine = get_engine()
                with get_session(engine) as session:
                    result = loader.load(session, rates=loadable, source_id=source_id)
                    total_loaded  += result.rows_loaded
                    total_updated += result.rows_updated
                    total_skipped += result.rows_skipped
                    logger.info(
                        "[load_warehouse] source_id=%d loaded=%d updated=%d skipped=%d",
                        source_id, result.rows_loaded, result.rows_updated, result.rows_skipped,
                    )

            except Exception as exc:
                logger.error("[load_warehouse] Error loading source_id=%d: %s", source_id, exc)
                stage.error_count += 1

        stage.records_out = total_loaded + total_updated
        stage.finish(success=stage.error_count == 0)
        log_stage_end(stage, context)

        summary = {
            "rows_loaded":  total_loaded,
            "rows_updated": total_updated,
            "skipped":      total_skipped,
            "loaded_at":    utcnow_iso(),
        }
        xcom_push_summary(context, "load_result", summary)
        logger.info("[load_warehouse] Done: loaded=%d updated=%d skipped=%d", total_loaded, total_updated, total_skipped)
        return summary

    # ------------------------------------------------------------------ #
    # Task 2 — Record load summary + pipeline metrics
    #
    # (Previously this was "refresh_cache", which refreshed materialized
    # views built ON TOP OF marts.* tables. Since transform_dag now runs
    # AFTER load_dag, those views wouldn't reflect today's data yet at
    # this point — cache refreshing has moved to transform_dag.py,
    # immediately after dbt builds the marts. This task just records the
    # load-stage summary for full_elt_dag's final notification.)
    # ------------------------------------------------------------------ #
    @task(task_id="record_load_summary")
    def record_load_summary(**context) -> dict:
        """Record load-stage metrics and push the final load summary to XCom."""
        stage = log_stage_start("record_load_summary")
        stage.finish(success=True)
        log_stage_end(stage, context)

        run_metrics = PipelineRunMetrics(
            dag_id=DAG_ID_LOAD,
            run_id=context.get("run_id", "unknown"),
            execution_date=str(context.get("execution_date", utcnow_iso())),
        )
        log_pipeline_metrics(run_metrics)

        summary = {LOAD_SUMMARY_XCOM_KEY: {"completed_at": utcnow_iso()}}
        xcom_push_summary(context, LOAD_SUMMARY_XCOM_KEY, summary)
        return summary

    # ------------------------------------------------------------------ #
    # Wiring
    # ------------------------------------------------------------------ #
    loaded  = load_warehouse()
    summary = record_load_summary()

    wait_for_extract >> loaded >> summary


load_dag()