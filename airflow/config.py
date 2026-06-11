"""Airflow configuration"""

# Airflow Home
import os
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/app/airflow")

# Database
SQLALCHEMY_DATABASE_URI = os.getenv(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
)

# Executor
EXECUTOR = os.getenv("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")

# DAG configuration
DAG_DEFAULT_VIEW = "tree"
DAG_ORIENTATION = "LR"

# Task configuration
TASK_CONCURRENCY = 3
MAX_ACTIVE_RUNS_PER_DAG = 1

# Logging
LOG_LEVEL = os.getenv("AIRFLOW__LOGGING__LOGGING_LEVEL", "INFO")
