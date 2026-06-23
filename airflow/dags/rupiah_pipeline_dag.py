"""
airflow/dags/rupiah_pipeline_dag.py
=====================================
Airflow 3.x DAGs for the Rupiah Exchange Rate Intelligence pipeline.

Architecture
------------
Airflow 3.x and the pipeline share one venv. SQLAlchemy is pinned to
1.4.51 project-wide (apache-airflow requires sqlalchemy<2.0 in every
released version) — src/utils/database.py is written in 1.4 style to match.

DAGs
----
1. rupiah_yfinance_hourly  — extract → validate → load → dbt (every hour)
2. rupiah_fred_daily       — extract → validate → load → dbt (06:00 UTC daily)
3. rupiah_health_check     — DB + API connectivity check (every 30 min)

Airflow 3.x notes vs 2.x
--------------------------
- ``schedule`` replaces ``schedule_interval``
- ``BashOperator`` still works (imported from airflow.operators.bash)
- ``@dag`` / ``@task`` decorators are the preferred TaskFlow style
- ``start_date`` is still required
- ``catchup=False`` default is unchanged

Environment variables expected in .env / Airflow Variables
-----------------------------------------------------------
    PROJECT_ROOT        /opt/rupiah-rate  (or D:\\rupiah-rate on Windows)
    DATABASE_URL        postgresql://user:pass@localhost:5432/rupiah_exchange
    FRED_API_KEY        your_fred_api_key
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# ---------------------------------------------------------------------------
# Path setup — allow importing src/ directly since same venv
# ---------------------------------------------------------------------------
PROJECT_ROOT = Variable.get(
    "RUPIAH_PROJECT_ROOT",
    default_var=str(Path(__file__).parents[2]),
)
sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# Shared default args
# ---------------------------------------------------------------------------
_DEFAULT_ARGS = {
    "owner":                     "rupiah-pipeline",
    "depends_on_past":           False,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=30),
}

_DBT_CMD = (
    f"dbt run "
    f"--project-dir {PROJECT_ROOT}/dbt "
    f"--profiles-dir {PROJECT_ROOT}/dbt "
    f"--target prod"
)

# ===========================================================================
# DAG 1 — Hourly yfinance  (data harian)
# ===========================================================================

@dag(
    dag_id="rupiah_yfinance_hourly",
    description="Hourly yfinance extraction → validation → load for USD/EUR/SGD/JPY to IDR",
    default_args=_DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",      # every hour  (Airflow 3.x: use 'schedule' not 'schedule_interval')
    catchup=False,
    max_active_runs=1,
    tags=["rupiah", "yfinance", "hourly"],
)
def rupiah_yfinance_hourly():

    @task(task_id="extract_yfinance")
    def extract_yfinance():
        """
        Fetch latest rates for USD_IDR, EUR_IDR, SGD_IDR, JPY_IDR
        from Yahoo Finance using the yfinance library.
        Returns a dict of ExtractionResult for downstream tasks.
        """
        from src.etl.extractors import YFinanceExtractor

        extractor = YFinanceExtractor()
        result = extractor.fetch_rates(extractor.SUPPORTED_PAIRS)

        if result["errors"]:
            raise ValueError(
                f"yfinance extraction errors: {result['errors']}"
            )

        pairs_fetched = [r["pair"] for r in result["rates"]]
        print(f"[extract_yfinance] Fetched: {pairs_fetched}")
        return result

    @task(task_id="validate_yfinance")
    def validate_yfinance(extraction_result: dict):
        """
        Validate extracted yfinance data using ExchangeRateValidator.
        Hard-fails the task if is_valid=False.
        """
        import pandas as pd
        from src.etl.validators import ExchangeRateValidator

        rates = extraction_result.get("rates", [])
        if not rates:
            raise ValueError("No rates to validate.")

        df = pd.DataFrame(rates)
        df = df.rename(columns={"rate": "rate_close", "timestamp": "timestamp"})

        validator = ExchangeRateValidator()
        result = validator.validate(df)

        validator.log_results(result, source="yfinance")
        print(f"[validate_yfinance] quality_score={result.quality_score:.3f}")

        if not result.is_valid:
            raise ValueError(
                f"Validation failed: {result.errors}"
            )

        return {
            "extraction_result": extraction_result,
            "quality_score": result.quality_score,
            "warnings": result.warnings,
        }

    @task(task_id="load_yfinance")
    def load_yfinance(validated: dict):
        """
        Upsert validated rates into exchange_rates table.
        Logs an ApiCall audit row regardless of outcome.
        Placeholder — implement with loaders.py when ready.
        """
        rates = validated["extraction_result"].get("rates", [])
        print(
            f"[load_yfinance] Loading {len(rates)} rate(s) "
            f"(quality={validated['quality_score']:.3f})"
        )
        # TODO: replace with actual loader once loaders.py is implemented
        # from src.etl.loaders import ExchangeRateLoader
        # loader = ExchangeRateLoader()
        # loader.upsert(rates, source="yfinance")
        return {"loaded": len(rates)}

    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{_DBT_CMD} --select marts",
        doc_md="Refresh dbt mart models after successful load.",
    )

    # Task wiring (TaskFlow handles XCom automatically)
    extracted  = extract_yfinance()
    validated  = validate_yfinance(extracted)
    loaded     = load_yfinance(validated)
    loaded >> dbt_marts


rupiah_yfinance_hourly()


# ===========================================================================
# DAG 2 — Daily FRED  (data bulanan/tahunan)
# ===========================================================================

@dag(
    dag_id="rupiah_fred_daily",
    description="Daily FRED extraction → validation → load for USD_IDR monthly aggregate",
    default_args=_DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",      # 06:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["rupiah", "fred", "daily"],
)
def rupiah_fred_daily():

    @task(task_id="extract_fred")
    def extract_fred():
        """
        Fetch monthly USD/IDR aggregate from FRED API.
        Series: DEXINUS, frequency=monthly, aggregation=avg.
        Skips current incomplete month automatically.
        """
        import os
        from src.etl.extractors import FREDExtractor

        extractor = FREDExtractor(
            api_key=os.getenv("FRED_API_KEY"),
            frequency="m",
            aggregation_method="avg",
        )
        result = extractor.fetch_rates(["USD_IDR"])

        if result["errors"]:
            raise ValueError(f"FRED extraction errors: {result['errors']}")

        print(f"[extract_fred] Fetched: {[r['pair'] for r in result['rates']]}")
        return result

    @task(task_id="validate_fred")
    def validate_fred(extraction_result: dict):
        """
        Validate FRED data.
        Note: VALIDATOR_FRESHNESS_HOURS should be ~720 (30 days) for monthly data.
        Override in .env: VALIDATOR_FRESHNESS_HOURS=720
        """
        import pandas as pd
        from src.etl.validators import ExchangeRateValidator, ValidatorConfig

        rates = extraction_result.get("rates", [])
        if not rates:
            raise ValueError("No FRED rates to validate.")

        df = pd.DataFrame(rates)
        df = df.rename(columns={"rate": "rate_close"})

        # Monthly data: relax freshness threshold to 30 days
        cfg = ValidatorConfig(freshness_hours=720)
        validator = ExchangeRateValidator(config=cfg)
        result = validator.validate(df)

        validator.log_results(result, source="fred")
        print(f"[validate_fred] quality_score={result.quality_score:.3f}")

        if not result.is_valid:
            raise ValueError(f"FRED validation failed: {result.errors}")

        return {
            "extraction_result": extraction_result,
            "quality_score": result.quality_score,
        }

    @task(task_id="load_fred")
    def load_fred(validated: dict):
        """Upsert validated FRED rates into exchange_rates table."""
        rates = validated["extraction_result"].get("rates", [])
        print(f"[load_fred] Loading {len(rates)} FRED rate(s)")
        # TODO: from src.etl.loaders import ExchangeRateLoader; loader.upsert(...)
        return {"loaded": len(rates)}

    dbt_monthly = BashOperator(
        task_id="dbt_run_monthly_marts",
        bash_command=f"{_DBT_CMD} --select marts.monthly",
    )

    extracted = extract_fred()
    validated = validate_fred(extracted)
    loaded    = load_fred(validated)
    loaded >> dbt_monthly


rupiah_fred_daily()


# ===========================================================================
# DAG 3 — Health check  (every 30 min)
# ===========================================================================

@dag(
    dag_id="rupiah_health_check",
    description="DB connectivity and API reachability check every 30 minutes",
    default_args={**_DEFAULT_ARGS, "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=datetime(2025, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["rupiah", "monitoring"],
)
def rupiah_health_check():

    @task(task_id="check_db_connection")
    def check_db():
        """Probe PostgreSQL with SELECT 1."""
        from src.utils.database import get_engine, check_connection

        engine = get_engine()
        ok = check_connection(engine)
        if not ok:
            raise ConnectionError("Database connection check failed.")
        print("[health] DB connection OK.")

    @task(task_id="check_yfinance_api")
    def check_yfinance():
        """Verify yfinance can fetch at least one rate."""
        from src.etl.extractors import YFinanceExtractor

        extractor = YFinanceExtractor()
        result = extractor.fetch_rates(["USD_IDR"])
        if not result["rates"]:
            raise ConnectionError(
                f"yfinance health check failed: {result['errors']}"
            )
        rate = result["rates"][0]["rate"]
        print(f"[health] yfinance OK — USD_IDR={rate}")

    @task(task_id="check_fred_api", trigger_rule=TriggerRule.ALL_DONE)
    def check_fred():
        """Verify FRED API key is valid and endpoint is reachable."""
        import os
        from src.etl.extractors import FREDExtractor

        key = os.getenv("FRED_API_KEY")
        if not key:
            print("[health] FRED_API_KEY not set — skipping FRED check.")
            return

        extractor = FREDExtractor(api_key=key)
        result = extractor.fetch_rates(["USD_IDR"])
        if not result["rates"]:
            raise ConnectionError(
                f"FRED health check failed: {result['errors']}"
            )
        rate = result["rates"][0]["rate"]
        print(f"[health] FRED OK — USD_IDR monthly avg={rate}")

    db     = check_db()
    yf     = check_yfinance()
    fred   = check_fred()

    db >> [yf, fred]


rupiah_health_check()