"""
airflow/plugins/custom_operators.py
======================================
Custom Airflow operators for the Rupiah Exchange Rate Intelligence pipeline.

Currently provides:
    - DataQualityCheckOperator: runs an arbitrary SQL query and fails the
      task if the returned row count exceeds a threshold (used to wrap
      dbt singular tests as an Airflow-native check outside dbt itself,
      e.g. for pre-dbt sanity checks on raw tables).
    - ExchangeRateSensorOperator: polls the exchange_rates table for
      fresh data before allowing downstream tasks to proceed, without
      requiring a full ExternalTaskSensor cross-DAG dependency.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


class DataQualityCheckOperator(BaseOperator):
    """
    Runs a SQL query against the pipeline database and fails the task if
    the row count returned exceeds ``max_allowed_rows``.

    Designed to mirror the "singular test passes when 0 rows returned"
    convention used in dbt/tests/singular/*.sql, so the same style of
    check can run as a native Airflow task (e.g. before dbt even runs,
    to catch raw-table issues early).

    Parameters
    ----------
    sql:
        The SQL query to execute. Should return the OFFENDING rows
        (i.e. rows that indicate a problem), matching dbt singular test
        conventions.
    max_allowed_rows:
        Maximum number of offending rows tolerated before failing.
        Default 0 (any offending row fails the check).
    conn_getter:
        Optional callable returning a SQLAlchemy engine. Defaults to
        ``utils.database.get_engine`` from the project's src/ package.

    Example
    -------
        DataQualityCheckOperator(
            task_id="check_no_negative_rates",
            sql="SELECT rate_id FROM exchange_rates WHERE rate <= 0",
            max_allowed_rows=0,
        )
    """

    template_fields = ("sql",)
    ui_color = "#e6f2ff"

    @apply_defaults
    def __init__(
        self,
        sql: str,
        max_allowed_rows: int = 0,
        conn_getter: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.max_allowed_rows = max_allowed_rows
        self.conn_getter = conn_getter

    def execute(self, context: Context) -> int:
        from sqlalchemy import text

        if self.conn_getter is not None:
            engine = self.conn_getter()
        else:
            import sys, os
            sys.path.insert(0, os.getenv("RUPIAH_PROJECT_ROOT", "/opt/airflow/project") + "/src")
            from src.utils.database import get_engine
            engine = get_engine()

        logger.info("[DataQualityCheckOperator] Running: %s", self.sql)

        with engine.connect() as conn:
            result = conn.execute(text(self.sql))
            rows = result.fetchall()

        row_count = len(rows)
        logger.info(
            "[DataQualityCheckOperator] %d offending row(s) found (max allowed: %d)",
            row_count, self.max_allowed_rows,
        )

        if row_count > self.max_allowed_rows:
            sample = rows[:5]
            raise AirflowException(
                f"Data quality check failed: {row_count} offending row(s) "
                f"found (max allowed: {self.max_allowed_rows}). "
                f"Sample: {sample}"
            )

        return row_count


class ExchangeRateSensorOperator(BaseSensorOperator):
    """
    Polls the exchange_rates table for rows fresher than ``max_age_hours``.

    Useful as a lightweight in-DAG freshness gate before running
    transform_dag, as an alternative to (or in combination with)
    ExternalTaskSensor when you specifically care about DATA freshness
    rather than DAG-run success/failure state.

    Parameters
    ----------
    max_age_hours:
        Maximum acceptable age (in hours) of the most recent exchange
        rate row. Default 26 hours (covers the daily 02:00 extract with
        margin for delays).
    source_id:
        Optional — restrict the freshness check to a specific API source.

    Example
    -------
        ExchangeRateSensorOperator(
            task_id="wait_for_fresh_rates",
            max_age_hours=26,
            poke_interval=60,
            timeout=1800,
        )
    """

    template_fields = ()
    ui_color = "#fff2e6"

    @apply_defaults
    def __init__(
        self,
        max_age_hours: float = 26.0,
        source_id: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.max_age_hours = max_age_hours
        self.source_id = source_id

    def poke(self, context: Context) -> bool:
        from sqlalchemy import text
        import sys, os

        sys.path.insert(0, os.getenv("RUPIAH_PROJECT_ROOT", "/opt/airflow/project") + "/src")
        from src.utils.database import get_engine

        engine = get_engine()
        query = "SELECT MAX(timestamp) AS latest FROM exchange_rates"
        params: dict[str, Any] = {}
        if self.source_id is not None:
            query += " WHERE source_id = :source_id"
            params["source_id"] = self.source_id

        with engine.connect() as conn:
            result = conn.execute(text(query), params).fetchone()

        if not result or not result[0]:
            logger.info("[ExchangeRateSensorOperator] No exchange rate rows found yet.")
            return False

        latest_ts = result[0]
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)

        age_hours = (datetime.now(timezone.utc) - latest_ts).total_seconds() / 3600
        is_fresh = age_hours <= self.max_age_hours

        logger.info(
            "[ExchangeRateSensorOperator] Latest rate is %.1fh old (threshold: %.1fh) — fresh=%s",
            age_hours, self.max_age_hours, is_fresh,
        )
        return is_fresh