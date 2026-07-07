"""
airflow/dags/constants.py
==========================
Centralised constants and default values shared across all DAGs.
Path resolution supports both Docker (env-var driven) and local dev
(auto-detect from file location).
"""

from __future__ import annotations

import os
from datetime import timedelta

# ---------------------------------------------------------------------------
# Path resolution — Docker sets RUPIAH_PROJECT_ROOT explicitly via
# docker-compose.yml; local dev falls back to relative path detection.
# ---------------------------------------------------------------------------

PROJECT_ROOT: str = os.getenv(
    "RUPIAH_PROJECT_ROOT",
    # In Docker the working dir is /opt/airflow; code is mounted at
    # /opt/airflow/project. Locally it's two levels above dags/.
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")),
)

DBT_PROJECT_DIR: str = os.path.join(PROJECT_ROOT, "dbt")
DBT_PROFILES_DIR: str = os.getenv("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_TARGET: str = os.getenv("DBT_TARGET", "prod")

STAGING_DIR: str = os.getenv(
    "RUPIAH_STAGING_DIR",
    os.path.join(PROJECT_ROOT, "data", "staging"),
)

# ---------------------------------------------------------------------------
# DAG identifiers
# ---------------------------------------------------------------------------

DAG_ID_EXTRACT   = "extract_dag"
DAG_ID_TRANSFORM = "transform_dag"
DAG_ID_LOAD      = "load_dag"
DAG_ID_FULL_ELT  = "full_elt_dag"

# ---------------------------------------------------------------------------
# Currency pairs & source IDs
# ---------------------------------------------------------------------------

YFINANCE_PAIRS:    list[str] = ["USD_IDR", "EUR_IDR", "GBP_IDR", "JPY_IDR", "SGD_IDR", "AUD_IDR"]
FRED_PAIRS:        list[str] = ["USD_IDR"]
YFINANCE_SOURCE_ID: int = int(os.getenv("YFINANCE_SOURCE_ID", "1"))
FRED_SOURCE_ID:     int = int(os.getenv("FRED_SOURCE_ID",    "2"))

# ---------------------------------------------------------------------------
# Retry / SLA defaults
# ---------------------------------------------------------------------------

DEFAULT_RETRIES:        int       = 3
DEFAULT_RETRY_DELAY:    timedelta = timedelta(minutes=5)
DEFAULT_MAX_RETRY_DELAY: timedelta = timedelta(minutes=30)

SLA_EXTRACT:   timedelta = timedelta(minutes=30)
SLA_TRANSFORM: timedelta = timedelta(minutes=45)
SLA_LOAD:      timedelta = timedelta(minutes=20)
SLA_FULL_ELT:  timedelta = timedelta(hours=2)

# ---------------------------------------------------------------------------
# Notifications
# ---------------------------------------------------------------------------

ALERT_EMAIL_TO: list[str] = [
    e.strip()
    for e in os.getenv("RUPIAH_ALERT_EMAILS", "").split(",")
    if e.strip()
]

SLACK_WEBHOOK_URL: str | None = os.getenv("SLACK_WEBHOOK_URL")
SLACK_CHANNEL:     str        = os.getenv("SLACK_ALERT_CHANNEL", "#rupiah-pipeline-alerts")

# ---------------------------------------------------------------------------
# XCom keys
# ---------------------------------------------------------------------------

EXTRACT_SUMMARY_XCOM_KEY   = "extract_summary"
TRANSFORM_SUMMARY_XCOM_KEY = "transform_summary"
LOAD_SUMMARY_XCOM_KEY      = "load_summary"