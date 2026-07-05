"""
airflow/dags/load_dag.py
==========================
DAG 3 — Load transformed mart data to warehouse and refresh cache.

Schedule : Daily at 04:00 UTC  (0 4 * * *)
Tasks    : wait_transform → export_marts → load_warehouse → refresh_cache
Depends  : transform_dag must complete successfully first
Retries  : 3 with exponential backoff
Alerts   : Email + Slack on failure
SLA      : 20 minutes
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import timedelta

from airflow.utils.state import DagRunState
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from dags.config import PipelineDAGConfig
from dags.constants import (
    DAG_ID_LOAD,
    DAG_ID_TRANSFORM,
    EXTRACT_SUMMARY_XCOM_KEY,
    LOAD_SUMMARY_XCOM_KEY,
    SLA_LOAD,
    YFINANCE_SOURCE_ID,
    FRED_SOURCE_ID,
)
from dags.utils.helpers import on_failure_callback, utcnow_iso, xcom_push_summary
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
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rupiah", "load"],
    sla_miss_callback=build_sla_miss_callback(DAG_ID_LOAD),
    doc_md=__doc__,
)
def load_dag():

    # ------------------------------------------------------------------ #
    # Sensor: wait for transform_dag
    # ------------------------------------------------------------------ #
    wait_for_transform = ExternalTaskSensor(
        task_id="wait_for_transform_dag",
        external_dag_id=DAG_ID_TRANSFORM,
        external_task_id=None,
        allowed_states=["success"],
        execution_delta=timedelta(hours=1),
        timeout=1800,
        poke_interval=60,
        mode="reschedule",
    )

    # ------------------------------------------------------------------ #
    # Task 1 — Export mart tables to staging area (CSV snapshots)
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
    # Task 2 — Load rates into exchange_rates table via ExchangeRateLoader
    # ------------------------------------------------------------------ #
    @task(task_id="load_warehouse")
    def load_warehouse(**context) -> dict:
        """
        Pull the validated extraction payload from XCom (pushed by
        extract_dag's validate_extract task) and load it into the
        exchange_rates table using ExchangeRateLoader's idempotent upsert.
        """
        stage = log_stage_start("load_warehouse")

        # Pull extraction summary from extract_dag via XCom
        ti = context.get("task_instance")
        extract_summary = ti.xcom_pull(
            dag_id="extract_dag",
            task_ids="validate_extract",
            key=EXTRACT_SUMMARY_XCOM_KEY,
        ) if ti else None

        if not extract_summary or not extract_summary.get("merged_data", {}).get("rates"):
            logger.warning("[load_warehouse] No extraction payload in XCom — nothing to load.")
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
    # Task 3 — Refresh materialized views / caches
    # ------------------------------------------------------------------ #
    @task(task_id="refresh_cache")
    def refresh_cache(**context) -> dict:
        """
        Refresh PostgreSQL materialized views or trigger downstream cache
        invalidation so BI tools see the latest mart data immediately.
        Extend this task to call your BI tool's API (e.g. Metabase, Superset).
        """
        stage = log_stage_start("refresh_cache")
        refreshed: list[str] = []

        try:
            from src.utils.database import get_engine, get_session
            from sqlalchemy import text

            engine = get_engine()
            # Refresh any materialized views that exist (non-fatal if absent)
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

        run_metrics = PipelineRunMetrics(
            dag_id=DAG_ID_LOAD,
            run_id=context.get("run_id", "unknown"),
            execution_date=str(context.get("execution_date", utcnow_iso())),
        )
        log_pipeline_metrics(run_metrics)

        summary = {LOAD_SUMMARY_XCOM_KEY: {"refreshed_views": refreshed, "completed_at": utcnow_iso()}}
        xcom_push_summary(context, LOAD_SUMMARY_XCOM_KEY, summary)
        return summary

    # ------------------------------------------------------------------ #
    # Wiring
    # ------------------------------------------------------------------ #
    exports = export_marts()
    loaded  = load_warehouse()
    cache   = refresh_cache()

    wait_for_transform >> exports >> loaded >> cache


load_dag()