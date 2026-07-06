"""
airflow/dags/utils/helpers.py
===============================
Shared helper functions used across all DAGs. Keeps DAG files lean by
extracting reusable logic (email building, retry decorators, XCom wrappers,
environment checks) into one testable module.
"""

from __future__ import annotations

import functools
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# sys.path bootstrap — ensures src/ is importable inside the container
# ---------------------------------------------------------------------------

def ensure_src_on_path() -> None:
    """
    Add the project's ``src/`` directory to ``sys.path`` so that
    ``from etl.extractors import ...`` works from inside DAG task functions.

    Called once at module import time by each DAG file.
    In Docker, RUPIAH_PROJECT_ROOT is set explicitly in docker-compose.yml.
    """
    project_root = os.getenv(
        "RUPIAH_PROJECT_ROOT",
        os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..")
        ),
    )
    src_path = os.path.join(project_root, "src")
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
        logger.debug("Added %s to sys.path", src_path)


# ---------------------------------------------------------------------------
# Email alert builder
# ---------------------------------------------------------------------------

def build_failure_email(context: dict[str, Any]) -> dict[str, str]:
    """
    Build an email dict from an Airflow task context dict.

    Returns a dict with keys ``subject`` and ``html_content``, suitable
    for passing to ``EmailOperator`` or Airflow's built-in email alerts.

    Parameters
    ----------
    context:
        Airflow task context passed to ``on_failure_callback``.

    Returns
    -------
    dict[str, str]
    """
    dag    = context.get("dag")
    ti     = context.get("task_instance")
    exc    = context.get("exception", "Unknown error")
    run_id = context.get("run_id", "N/A")
    exec_date = str(context.get("execution_date", datetime.now(timezone.utc)))

    dag_id  = getattr(dag, "dag_id", "unknown")
    task_id = getattr(ti, "task_id", "unknown")
    log_url = getattr(ti, "log_url", "#")

    subject = f"[AIRFLOW FAILURE] {dag_id}.{task_id}"
    html_content = f"""
    <html><body>
    <h2 style="color:red;">&#x274C; Airflow Task Failed</h2>
    <table border="1" cellpadding="6" cellspacing="0">
      <tr><th>DAG</th><td>{dag_id}</td></tr>
      <tr><th>Task</th><td>{task_id}</td></tr>
      <tr><th>Run ID</th><td>{run_id}</td></tr>
      <tr><th>Execution Date</th><td>{exec_date}</td></tr>
      <tr><th>Error</th><td><pre>{str(exc)[:1000]}</pre></td></tr>
      <tr><th>Logs</th><td><a href="{log_url}">View Task Logs</a></td></tr>
    </table>
    <p>Please investigate and resolve before the next scheduled run.</p>
    </body></html>
    """
    return {"subject": subject, "html_content": html_content}


def send_failure_email(context: dict[str, Any]) -> None:
    """
    Airflow ``on_failure_callback`` that sends an email alert.

    Uses Airflow's built-in ``send_email`` utility (configured via
    ``AIRFLOW__SMTP__*`` environment variables in docker-compose.yml).
    Falls back to logging if email is not configured.
    """
    from dags.constants import ALERT_EMAIL_TO

    if not ALERT_EMAIL_TO:
        logger.warning(
            "RUPIAH_ALERT_EMAILS is not set — skipping email alert for "
            "%s.%s",
            getattr(context.get("dag"), "dag_id", "?"),
            getattr(context.get("task_instance"), "task_id", "?"),
        )
        return

    email_data = build_failure_email(context)
    try:
        from airflow.utils.email import send_email
        send_email(
            to=ALERT_EMAIL_TO,
            subject=email_data["subject"],
            html_content=email_data["html_content"],
        )
        logger.info("Failure alert email sent to %s", ALERT_EMAIL_TO)
    except Exception as exc:  # noqa: BLE001
        logger.error("Could not send failure email: %s", exc)


# ---------------------------------------------------------------------------
# Combined callback: email (always) + Slack (if configured)
# ---------------------------------------------------------------------------

def on_failure_callback(context: dict[str, Any]) -> None:
    """
    Universal on_failure_callback used by all DAGs.

    Sends email alert (if RUPIAH_ALERT_EMAILS is set) and Slack alert
    (if SLACK_WEBHOOK_URL is set). Either or both can be absent —
    the callback degrades gracefully.
    """
    send_failure_email(context)
    try:
        from dags.utils.slack_alerts import slack_failure_callback
        slack_failure_callback(context)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Slack callback error (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# XCom helpers
# ---------------------------------------------------------------------------

def xcom_push_summary(context: dict[str, Any], key: str, value: Any) -> None:
    """Push a value to XCom, logging the key and value for traceability."""
    ti = context.get("task_instance")
    if ti is None:
        logger.warning("xcom_push_summary: no task_instance in context.")
        return
    ti.xcom_push(key=key, value=value)
    logger.info("XCom push: key=%r value_type=%s", key, type(value).__name__)


def xcom_pull_summary(
    context: dict[str, Any],
    task_id: str,
    key: str,
    dag_id: str | None = None,
) -> Any:
    """
    Pull a value from XCom, returning None if the key is missing.

    Parameters
    ----------
    context:
        Airflow task context.
    task_id:
        The task that pushed the value.
    key:
        XCom key string.
    dag_id:
        Optional — cross-DAG XCom pull target.
    """
    ti = context.get("task_instance")
    if ti is None:
        return None
    value = ti.xcom_pull(task_ids=task_id, key=key, dag_id=dag_id)
    logger.debug("XCom pull: task_id=%r key=%r value=%r", task_id, key, value)
    return value


# ---------------------------------------------------------------------------
# Retry decorator (used outside Airflow's built-in retry — e.g. in a
# @task function where you need fine-grained control inside the task body)
# ---------------------------------------------------------------------------

def retry_with_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """
    Decorator that retries a function with exponential backoff.

    Mirrors the ``@with_retry`` decorator in ``src/etl/extractors.py``
    but lives here so DAG utility functions can use it without importing
    from ``src/`` (which requires ``ensure_src_on_path()`` to have run).

    Parameters
    ----------
    max_attempts:
        Maximum number of total attempts (first call + retries).
    base_delay:
        Initial wait time in seconds before the first retry.
    max_delay:
        Upper cap on wait time between retries.
    exceptions:
        Exception types to catch and retry on.

    Example
    -------
    >>> @retry_with_backoff(max_attempts=3, exceptions=(ConnectionError,))
    ... def check_api():
    ...     ...
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        logger.error(
                            "retry_with_backoff: %s failed after %d attempt(s): %s",
                            fn.__name__, max_attempts, exc,
                        )
                        raise
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    logger.warning(
                        "retry_with_backoff: %s attempt %d/%d failed (%s). "
                        "Retrying in %.1fs...",
                        fn.__name__, attempt, max_attempts, exc, delay,
                    )
                    time.sleep(delay)
            raise last_exc  # type: ignore[misc]
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Environment / health-check helpers
# ---------------------------------------------------------------------------

def check_env_vars(*required_vars: str) -> list[str]:
    """
    Return a list of required environment variable names that are missing.

    Usage in a DAG task:
        missing = check_env_vars("DATABASE_URL", "FRED_API_KEY")
        if missing:
            raise EnvironmentError(f"Missing env vars: {missing}")
    """
    return [v for v in required_vars if not os.getenv(v)]


def format_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins}m {secs}s"


def utcnow_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Robust cross-DAG "did it run recently" check
# ---------------------------------------------------------------------------

def has_recent_successful_run(dag_id: str, within_hours: float = 6.0) -> bool:
    """
    Return True if ``dag_id`` had at least one successful DagRun within the
    last ``within_hours`` hours.

    This replaces ``ExternalTaskSensor(execution_delta=timedelta(hours=N))``,
    which assumes the upstream DAG ran at a fixed, predictable offset before
    the downstream DAG's own execution_date. That assumption breaks for:
      - Manually-triggered runs (execution_date doesn't line up)
      - DAGs triggered via TriggerDagRunOperator (e.g. from full_etl_dag),
        where each sub-DAG gets its own independent execution_date

    Using "was there a recent success" instead of "was there a success
    exactly N hours before MY execution_date" makes the dependency check
    correct regardless of how either DAG was triggered.

    Parameters
    ----------
    dag_id:
        The upstream DAG to check (e.g. "extract_dag").
    within_hours:
        How far back to look for a successful run. Default 6 hours —
        generous enough to cover manual re-runs and retries, tight enough
        to catch a genuinely stale/missing upstream run.

    Example
    -------
    >>> has_recent_successful_run("extract_dag", within_hours=6)
    True
    """
    from airflow.models import DagRun

    cutoff = datetime.now(timezone.utc) - timedelta(hours=within_hours)

    runs = DagRun.find(dag_id=dag_id, state="success")
    recent_runs = [
        r for r in runs
        if r.execution_date is not None
        and (
            r.execution_date.replace(tzinfo=timezone.utc)
            if r.execution_date.tzinfo is None
            else r.execution_date
        ) >= cutoff
    ]

    logger.info(
        "[has_recent_successful_run] dag_id=%s within_hours=%.1f -> found %d recent successful run(s)",
        dag_id, within_hours, len(recent_runs),
    )
    return len(recent_runs) > 0


def wait_for_recent_success(
    dag_id: str,
    within_hours: float = 6.0,
    poke_interval: int = 60,
    timeout: int = 1800,
):
    """
    Build a PythonSensor-compatible callable that waits for a recent
    successful run of ``dag_id``, checked via :func:`has_recent_successful_run`.

    Usage in a DAG (with the @task.sensor decorator or PythonSensor):
        from airflow.sensors.python import PythonSensor
        wait_for_extract = PythonSensor(
            task_id="wait_for_extract_dag",
            python_callable=wait_for_recent_success("extract_dag"),
            poke_interval=60,
            timeout=1800,
            mode="reschedule",
        )
    """
    def _check(**context) -> bool:
        # Chained runs from full_etl_dag already guarantee ordering via
        # TriggerDagRunOperator(wait_for_completion=True) — skip the
        # sensor entirely in that case to avoid redundant waiting.
        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        if conf.get("triggered_by") == "full_etl_dag":
            logger.info(
                "[wait_for_recent_success] Triggered by full_etl_dag — "
                "upstream ordering already guaranteed, skipping wait for %s.",
                dag_id,
            )
            return True
        return has_recent_successful_run(dag_id, within_hours=within_hours)

    return _check