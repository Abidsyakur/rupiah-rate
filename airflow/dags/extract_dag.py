"""
airflow/dags/extract_dag.py
=============================
DAG 1 — Extract exchange rate data from yfinance and FRED API.

Schedule : Daily at 02:00 UTC  (0 2 * * *)
Tasks    : check_apis → extract_yfinance → extract_fred → merge → validate
Retries  : 3 with exponential backoff (5min → 10min → 20min)
Alerts   : Email on failure (Slack if SLACK_WEBHOOK_URL is set)
SLA      : 30 minutes
"""

from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Bootstrap src/ onto sys.path before any project imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from dags.config import PipelineDAGConfig
from dags.constants import (
    DAG_ID_EXTRACT,
    EXTRACT_SUMMARY_XCOM_KEY,
    FRED_PAIRS,
    FRED_SOURCE_ID,
    SLA_EXTRACT,
    YFINANCE_PAIRS,
    YFINANCE_SOURCE_ID,
)
from dags.utils.helpers import (
    check_env_vars,
    format_duration,
    on_failure_callback,
    utcnow_iso,
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

# ---------------------------------------------------------------------------
# Default args applied to every task in this DAG
# ---------------------------------------------------------------------------
_DEFAULT_ARGS = {
    "owner":                     "rupiah-pipeline",
    "depends_on_past":           False,
    "email_on_failure":          True,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=30),
    "on_failure_callback":       on_failure_callback,
    "sla":                       SLA_EXTRACT,
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
@dag(
    dag_id=DAG_ID_EXTRACT,
    description="Extract daily exchange rates from yfinance (all pairs) and FRED API (USD_IDR monthly).",
    default_args=_DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rupiah", "extract", "yfinance", "fred"],
    sla_miss_callback=build_sla_miss_callback(DAG_ID_EXTRACT),
    doc_md=__doc__,
)
def extract_dag():

    # ------------------------------------------------------------------ #
    # Task 1 — Pre-flight check: env vars + API reachability
    # ------------------------------------------------------------------ #
    @task(task_id="check_apis")
    def check_apis(**context) -> dict:
        """
        Verify required environment variables are present and both APIs
        are reachable before committing to a full extraction run.
        """
        stage = log_stage_start("check_apis")
        logger.info("[check_apis] Starting pre-flight checks.")

        # 1. Required env vars
        missing = check_env_vars("DATABASE_URL", "FRED_API_KEY")
        if missing:
            stage.finish(success=False)
            log_stage_end(stage, context)
            raise EnvironmentError(
                f"[check_apis] Missing required environment variables: {missing}. "
                f"Ensure they are set in docker-compose.yml or your .env file."
            )

        # 2. yfinance reachability (lightweight ticker fetch)
        try:
            from src.etl.extractors import YFinanceExtractor
            extractor = YFinanceExtractor()
            probe = extractor.fetch_rates(["USD_IDR"])
            if not probe["rates"]:
                raise ConnectionError("yfinance returned no rates for USD_IDR probe.")
            logger.info("[check_apis] yfinance: OK (USD_IDR = %s)", probe["rates"][0]["rate"])
        except Exception as exc:
            stage.finish(success=False)
            log_stage_end(stage, context)
            raise RuntimeError(f"[check_apis] yfinance connectivity check failed: {exc}") from exc

        # 3. FRED API reachability
        try:
            import os, requests
            params = {
                "series_id": "DEXINUS",
                "api_key": os.getenv("FRED_API_KEY"),
                "file_type": "json",
                "limit": 1,
            }
            resp = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params=params,
                timeout=(10, 30),
            )
            resp.raise_for_status()
            logger.info("[check_apis] FRED API: OK (HTTP %d)", resp.status_code)
        except Exception as exc:
            # FRED failure is a warning, not a hard stop — we can still
            # extract yfinance data and skip FRED gracefully.
            logger.warning("[check_apis] FRED API check failed (non-fatal): %s", exc)

        stage.error_count = len(missing)
        stage.finish(success=True)
        log_stage_end(stage, context)
        logger.info("[check_apis] Pre-flight checks passed.")
        return {"checked_at": utcnow_iso(), "env_vars_ok": True}

    # ------------------------------------------------------------------ #
    # Task 2 — Extract from yfinance (all 6 IDR pairs, daily)
    # ------------------------------------------------------------------ #
    @task(task_id="extract_yfinance")
    def extract_yfinance(**context) -> dict:
        """
        Fetch the latest tick-level exchange rates for all 6 IDR pairs
        from Yahoo Finance using the yfinance library.

        Returns an extraction summary dict pushed to XCom for the merge task.
        """
        stage = log_stage_start("extract_yfinance")
        logger.info("[extract_yfinance] Fetching pairs: %s", YFINANCE_PAIRS)

        from src.etl.extractors import YFinanceExtractor
        extractor = YFinanceExtractor()
        result = extractor.fetch_rates(YFINANCE_PAIRS)

        rates     = result.get("rates", [])
        errors    = result.get("errors", [])

        stage.records_out  = len(rates)
        stage.error_count  = len(errors)

        if errors:
            logger.warning(
                "[extract_yfinance] %d pair(s) had errors: %s", len(errors), errors
            )

        if not rates:
            stage.finish(success=False)
            log_stage_end(stage, context)
            raise RuntimeError(
                "[extract_yfinance] No rates returned from yfinance. "
                f"Errors: {errors}"
            )

        stage.finish(success=True)
        log_stage_end(stage, context)

        summary = {
            "source":          "yfinance",
            "source_id":       YFINANCE_SOURCE_ID,
            "pairs_requested": YFINANCE_PAIRS,
            "pairs_fetched":   [r["pair"] for r in rates],
            "record_count":    len(rates),
            "errors":          errors,
            "fetched_at":      utcnow_iso(),
            "rates":           rates,          # full payload for merge task
        }
        xcom_push_summary(context, "yfinance_result", summary)
        logger.info(
            "[extract_yfinance] Done: %d record(s) fetched, %d error(s).",
            len(rates), len(errors),
        )
        return summary

    # ------------------------------------------------------------------ #
    # Task 3 — Extract from FRED API (USD_IDR monthly aggregate)
    # ------------------------------------------------------------------ #
    @task(task_id="extract_fred")
    def extract_fred(**context) -> dict:
        """
        Fetch the most recent monthly USD/IDR aggregate from the FRED API
        (series DEXINUS, frequency=monthly, aggregation=avg).

        FRED is used for monthly trend data — the current incomplete month
        is skipped automatically (the extractor walks back to the last
        closed period).
        """
        import os
        stage = log_stage_start("extract_fred")
        logger.info("[extract_fred] Fetching pairs: %s (monthly)", FRED_PAIRS)

        try:
            from src.etl.extractors import FREDExtractor
            extractor = FREDExtractor(
                api_key=os.getenv("FRED_API_KEY"),
                frequency="m",
                aggregation_method="avg",
            )
            result = extractor.fetch_rates(FRED_PAIRS)
        except EnvironmentError as exc:
            # FRED_API_KEY missing — non-critical, log and continue
            logger.warning("[extract_fred] FRED API key not set: %s", exc)
            stage.finish(success=True)   # don't fail the DAG for FRED alone
            log_stage_end(stage, context)
            return {"source": "fred", "source_id": FRED_SOURCE_ID, "record_count": 0, "rates": [], "errors": [str(exc)]}

        rates  = result.get("rates", [])
        errors = result.get("errors", [])

        stage.records_out = len(rates)
        stage.error_count = len(errors)

        if errors:
            logger.warning("[extract_fred] Errors: %s", errors)

        stage.finish(success=True)
        log_stage_end(stage, context)

        summary = {
            "source":          "fred",
            "source_id":       FRED_SOURCE_ID,
            "pairs_requested": FRED_PAIRS,
            "pairs_fetched":   [r["pair"] for r in rates],
            "record_count":    len(rates),
            "errors":          errors,
            "fetched_at":      utcnow_iso(),
            "rates":           rates,
        }
        xcom_push_summary(context, "fred_result", summary)
        logger.info(
            "[extract_fred] Done: %d record(s) fetched, %d error(s).",
            len(rates), len(errors),
        )
        return summary

    # ------------------------------------------------------------------ #
    # Task 4 — Merge results from both sources
    # ------------------------------------------------------------------ #
    @task(task_id="merge_results")
    def merge_results(yfinance_result: dict, fred_result: dict, **context) -> dict:
        """
        Combine yfinance and FRED extraction results into a single
        consolidated summary. The merged payload is pushed to XCom for
        the downstream extract_validate task and, later, load_dag.
        """
        stage = log_stage_start("merge")

        all_rates  = yfinance_result.get("rates", []) + fred_result.get("rates", [])
        all_errors = yfinance_result.get("errors", []) + fred_result.get("errors", [])

        merged = {
            "total_records":  len(all_rates),
            "yfinance_count": yfinance_result.get("record_count", 0),
            "fred_count":     fred_result.get("record_count", 0),
            "all_errors":     all_errors,
            "merged_at":      utcnow_iso(),
            "rates":          all_rates,
        }

        stage.records_in  = len(all_rates)
        stage.records_out = len(all_rates)
        stage.error_count = len(all_errors)
        stage.finish(success=True)
        log_stage_end(stage, context)

        logger.info(
            "[merge] Merged: yfinance=%d fred=%d total=%d errors=%d",
            yfinance_result.get("record_count", 0),
            fred_result.get("record_count", 0),
            len(all_rates),
            len(all_errors),
        )
        return merged

    # ------------------------------------------------------------------ #
    # Task 5 — Validate merged data
    # ------------------------------------------------------------------ #
    @task(task_id="validate_extract")
    def validate_extract(merged: dict, **context) -> dict:
        """
        Run ExchangeRateValidator over the merged extraction payload to
        produce a per-pair quality report. Does NOT load to the DB — that
        is load_dag's responsibility. Only hard failures abort the pipeline.
        """
        import pandas as pd
        stage = log_stage_start("validate")

        from src.etl.validators import ExchangeRateValidator, ValidatorConfig
        validator = ExchangeRateValidator(
            config=ValidatorConfig(
                freshness_hours=cfg.extract_timeout / 3600,
                null_threshold=0.05,
                fail_on_null=True,
                fail_on_anomaly=False,
            )
        )

        rates = merged.get("rates", [])
        if not rates:
            logger.warning("[validate] No rates to validate.")
            stage.finish(success=True)
            log_stage_end(stage, context)
            summary = {EXTRACT_SUMMARY_XCOM_KEY: merged, "validation_passed": True, "quality_scores": {}}
            xcom_push_summary(context, EXTRACT_SUMMARY_XCOM_KEY, summary)
            _write_staging_payload(summary, context)
            return summary

        # ------------------------------------------------------------------
        # BUG FIX: group by (pair, source) — NOT pair alone.
        #
        # yfinance provides daily ticks; FRED provides monthly aggregates.
        # The same pair (e.g. USD_IDR) can appear from BOTH sources with
        # wildly different timestamps/frequencies. Grouping by pair alone
        # merges these into one heterogeneous "time series", which false-
        # positives the date_consistency check (see incident: USD_IDR
        # failing with "1 unparseable/out-of-order date value" because a
        # July 2026 daily tick and a May 2026 monthly average were
        # compared as if they were the same series).
        # ------------------------------------------------------------------
        by_pair_source: dict[str, list] = {}
        for r in rates:
            group_key = f"{r.get('pair', 'UNKNOWN')}|{r.get('source', 'unknown')}"
            by_pair_source.setdefault(group_key, []).append(r)

        quality_scores: dict[str, float] = {}
        validation_errors: list[str] = []

        for group_key, group_rates in by_pair_source.items():
            pair, source = group_key.split("|", 1)
            df = pd.DataFrame(group_rates).rename(columns={"rate": "rate_close"})
            result = validator.validate(df)
            # Key quality_scores by pair only for downstream consumers,
            # but if a pair has multiple sources, keep the worse score.
            existing = quality_scores.get(pair)
            quality_scores[pair] = (
                round(result.quality_score, 4)
                if existing is None
                else round(min(existing, result.quality_score), 4)
            )
            if not result.is_valid:
                validation_errors.extend([f"[{pair}/{source}] {e}" for e in result.errors])
                logger.warning("[validate] %s (%s) FAILED validation: %s", pair, source, result.errors)
            else:
                logger.info("[validate] %s (%s) OK (quality=%.3f)", pair, source, result.quality_score)

        avg_quality = round(sum(quality_scores.values()) / len(quality_scores), 4) if quality_scores else 0.0

        stage.records_in  = len(rates)
        stage.records_out = len(rates) - len(validation_errors)
        stage.error_count = len(validation_errors)
        stage.finish(success=len(validation_errors) == 0 or not cfg.fail_on_low_quality)
        log_stage_end(stage, context)

        if validation_errors and cfg.fail_on_low_quality:
            raise ValueError(
                f"[validate] {len(validation_errors)} validation error(s): {validation_errors}"
            )

        summary = {
            "merged_data":      merged,
            "quality_scores":   quality_scores,
            "avg_quality":      avg_quality,
            "validation_errors": validation_errors,
            "validation_passed": len(validation_errors) == 0,
            "validated_at":     utcnow_iso(),
        }
        xcom_push_summary(context, EXTRACT_SUMMARY_XCOM_KEY, summary)

        # ------------------------------------------------------------------
        # BUG FIX: persist to a shared-volume JSON file so load_dag can
        # read it reliably. Cross-DAG XCom (ti.xcom_pull(dag_id=...)) is
        # scoped to a matching execution_date/logical_date, which breaks
        # for manual runs and for TriggerDagRunOperator-triggered sub-DAGs
        # (each gets its own execution_date) — exactly the situation that
        # caused "task succeeded but nothing landed in the database".
        # ------------------------------------------------------------------
        _write_staging_payload(summary, context)

        logger.info(
            "[validate] Done: avg_quality=%.3f pairs=%d errors=%d",
            avg_quality, len(quality_scores), len(validation_errors),
        )
        return summary

    # ------------------------------------------------------------------ #
    # Helper: persist validated payload to shared staging volume
    # ------------------------------------------------------------------ #
    def _write_staging_payload(summary: dict, context: dict) -> None:
        """
        Write the validated extraction summary to two files in STAGING_DIR
        (a Docker volume mounted into every Airflow container):

          - extract_latest.json      always overwritten — the canonical
                                     "most recent successful extract" file
                                     load_dag reads by default.
          - extract_{ds}.json        dated snapshot, keyed by Airflow's
                                     logical date (context['ds']), useful
                                     for backfills / audit trail.

        This sidesteps Airflow's cross-DAG XCom execution_date-matching
        requirement entirely, which is what silently caused load_dag to
        find no data despite extract_dag succeeding.
        """
        import json
        import os
        from dags.constants import STAGING_DIR

        try:
            os.makedirs(STAGING_DIR, exist_ok=True)
            ds = context.get("ds", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

            payload = {**summary, "written_at": utcnow_iso(), "logical_date": ds}

            latest_path = os.path.join(STAGING_DIR, "extract_latest.json")
            dated_path  = os.path.join(STAGING_DIR, f"extract_{ds}.json")

            for path in (latest_path, dated_path):
                with open(path, "w") as f:
                    json.dump(payload, f, indent=2, default=str)

            logger.info(
                "[validate] Staging payload written to %s and %s",
                latest_path, dated_path,
            )
        except Exception as exc:  # noqa: BLE001
            # Never let a staging-file write failure fail the whole DAG —
            # XCom still has the data for same-run consumers; this is a
            # best-effort convenience for cross-DAG consumption.
            logger.error(
                "[validate] Could not write staging payload (non-fatal): %s", exc
            )

    # ------------------------------------------------------------------ #
    # Task wiring
    # ------------------------------------------------------------------ #
    pre_flight = check_apis()
    yf_result  = extract_yfinance()
    fred_result = extract_fred()
    merged     = merge_results(yf_result, fred_result)
    validated  = validate_extract(merged)

    pre_flight >> [yf_result, fred_result] >> merged >> validated


extract_dag()