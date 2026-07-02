"""
airflow/dags/utils/slack_alerts.py
=====================================
Slack notification utilities for DAG failure/success/SLA-miss alerts.

Design principles
-----------------
- Graceful degradation: if SLACK_WEBHOOK_URL is not set, every function
  logs a warning and returns without raising. This means the project
  works out-of-the-box with email-only alerting, and Slack can be
  activated at any time by setting the env var — no code changes needed.
- All functions follow the Airflow callback signature so they can be
  passed directly to ``on_failure_callback``, ``on_success_callback``,
  and ``sla_miss_callback`` DAG/task parameters.

Activation
----------
Set SLACK_WEBHOOK_URL in docker-compose.yml or your .env file:

    SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../xxx
    SLACK_ALERT_CHANNEL=#rupiah-pipeline-alerts  # optional, has a default

Usage in a DAG
--------------
    from dags.utils.slack_alerts import slack_failure_callback

    with DAG(..., on_failure_callback=slack_failure_callback):
        ...
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

import requests

from dags.constants import SLACK_CHANNEL, SLACK_WEBHOOK_URL

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_configured() -> bool:
    """Return True if a Slack webhook URL is available."""
    if not SLACK_WEBHOOK_URL:
        logger.debug(
            "SLACK_WEBHOOK_URL is not set — Slack notifications are disabled. "
            "Set it in your docker-compose.yml or .env to enable."
        )
        return False
    return True


def _post_message(payload: dict[str, Any]) -> bool:
    """
    POST a Slack message payload to the configured webhook.

    Parameters
    ----------
    payload:
        Slack Block Kit message dict.

    Returns
    -------
    bool
        True if the message was delivered (HTTP 200), False otherwise.
    """
    if not _is_configured():
        return False

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,       # type: ignore[arg-type]
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if response.status_code != 200:
            logger.error(
                "Slack webhook returned HTTP %d: %s",
                response.status_code,
                response.text,
            )
            return False
        return True

    except requests.exceptions.RequestException as exc:
        logger.error("Failed to send Slack notification: %s", exc)
        return False


def _context_to_meta(context: dict[str, Any]) -> dict[str, str]:
    """Extract common metadata from an Airflow task/dag context dict."""
    dag    = context.get("dag")
    ti     = context.get("task_instance")
    run_id = context.get("run_id", "unknown")

    return {
        "dag_id":       getattr(dag, "dag_id", "unknown"),
        "task_id":      getattr(ti, "task_id", "N/A"),
        "run_id":       run_id,
        "log_url":      getattr(ti, "log_url", "#"),
        "execution_date": str(context.get("execution_date", datetime.utcnow())),
    }


# ---------------------------------------------------------------------------
# Airflow callback functions
# ---------------------------------------------------------------------------

def slack_failure_callback(context: dict[str, Any]) -> None:
    """
    Airflow on_failure_callback — posts a RED alert to Slack.

    Attach to a DAG or individual task:
        DAG(on_failure_callback=slack_failure_callback, ...)
        PythonOperator(on_failure_callback=slack_failure_callback, ...)
    """
    meta = _context_to_meta(context)
    exception = context.get("exception", "Unknown error")

    payload = {
        "channel": SLACK_CHANNEL,
        "attachments": [
            {
                "color": "#FF0000",
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": "FAILURE — Rupiah Pipeline"},
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*DAG:*\n{meta['dag_id']}"},
                            {"type": "mrkdwn", "text": f"*Task:*\n{meta['task_id']}"},
                            {"type": "mrkdwn", "text": f"*Run ID:*\n{meta['run_id']}"},
                            {"type": "mrkdwn", "text": f"*Execution:*\n{meta['execution_date']}"},
                        ],
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"*Error:*\n```{str(exception)[:500]}```"},
                    },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "View Logs"},
                                "url": meta["log_url"],
                            }
                        ],
                    },
                ],
            }
        ],
    }
    _post_message(payload)


def slack_success_callback(context: dict[str, Any]) -> None:
    """
    Airflow on_success_callback — posts a GREEN notification to Slack.

    Typically attached at the DAG level for summary-on-completion:
        DAG(on_success_callback=slack_success_callback, ...)
    """
    meta = _context_to_meta(context)
    payload = {
        "channel": SLACK_CHANNEL,
        "attachments": [
            {
                "color": "#36A64F",
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": "SUCCESS — Rupiah Pipeline"},
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*DAG:*\n{meta['dag_id']}"},
                            {"type": "mrkdwn", "text": f"*Run ID:*\n{meta['run_id']}"},
                            {"type": "mrkdwn", "text": f"*Execution:*\n{meta['execution_date']}"},
                        ],
                    },
                ],
            }
        ],
    }
    _post_message(payload)


def slack_sla_miss_callback(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: Any,
    blocking_tis: Any,
) -> None:
    """
    Airflow sla_miss_callback — posts an ORANGE SLA-miss alert to Slack.

    Attach at DAG level:
        DAG(sla_miss_callback=slack_sla_miss_callback, ...)
    """
    if not _is_configured():
        return

    dag_id = getattr(dag, "dag_id", "unknown")
    payload = {
        "channel": SLACK_CHANNEL,
        "attachments": [
            {
                "color": "#FFA500",
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": "SLA MISS — Rupiah Pipeline"},
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                            {"type": "mrkdwn", "text": f"*Tasks breaching SLA:*\n{task_list}"},
                            {"type": "mrkdwn", "text": f"*Blocking tasks:*\n{blocking_task_list}"},
                        ],
                    },
                ],
            }
        ],
    }
    _post_message(payload)


def send_pipeline_summary(
    dag_id: str,
    run_id: str,
    summary: dict[str, Any],
) -> None:
    """
    Send a custom pipeline summary message (not an Airflow callback).

    Called manually from full_etl_dag.py after all stages complete to
    post a structured metrics summary.

    Parameters
    ----------
    dag_id:   DAG identifier for the title.
    run_id:   Airflow run_id for correlation.
    summary:  Dict with keys: success, total_records, processed_records,
              failed_records, duration_seconds.
    """
    if not _is_configured():
        return

    status_emoji = "✅" if summary.get("success") else "❌"
    color = "#36A64F" if summary.get("success") else "#FF0000"

    payload = {
        "channel": SLACK_CHANNEL,
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{status_emoji} Pipeline Summary — {dag_id}",
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Run ID:*\n{run_id}"},
                            {"type": "mrkdwn", "text": f"*Total records:*\n{summary.get('total_records', 0)}"},
                            {"type": "mrkdwn", "text": f"*Processed:*\n{summary.get('processed_records', 0)}"},
                            {"type": "mrkdwn", "text": f"*Failed:*\n{summary.get('failed_records', 0)}"},
                            {"type": "mrkdwn", "text": f"*Duration:*\n{summary.get('duration_seconds', 0):.1f}s"},
                        ],
                    },
                ],
            }
        ],
    }
    _post_message(payload)