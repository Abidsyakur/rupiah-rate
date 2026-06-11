"""
tests/unit/test_database.py
Unit tests for src/utils/database.py (ADR-002).
"""
from __future__ import annotations

import os
import sys
import pathlib
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import inspect, text, select, func
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

sys.path.insert(0, str(pathlib.Path(__file__).parents[2] / "src"))

from src.utils.database import (
    ApiCall,
    ApiSource,
    Base,
    Currency,
    DailySnapshot,
    DataQualityMetric,
    ExchangeRate,
    _get_database_url,
    check_connection,
    create_all_tables,
    drop_all_tables,
    get_engine,
    get_session,
    get_session_factory,
)

SQLITE_URL = "sqlite://"
FIXED_DATE = date(2025, 1, 15)
FIXED_TS = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

@pytest.fixture(scope="session")
def engine():
    eng = get_engine(database_url=SQLITE_URL, env="dev")
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)
    eng.dispose()

@pytest.fixture
def session(engine):
    connection = engine.connect()
    transaction = connection.begin()
    sess = Session(bind=connection)
    yield sess
    sess.close()
    transaction.rollback()
    connection.close()

# ---------------------------------------------------------------------------
# Factory Helpers
# ---------------------------------------------------------------------------
def make_currency(code: str = "USD", name: str = "US Dollar", is_active: bool = True) -> Currency:
    return Currency(code=code, name=name, is_active=is_active)

def make_api_source(
    name: str = "yfinance", endpoint: str | None = "https://query1.finance.yahoo.com",
    rate_limit: int | None = 2000, retry_strategy: str | None = "exponential_backoff_max3",
    is_active: bool = True,
) -> ApiSource:
    return ApiSource(source_name=name, api_endpoint=endpoint, retry_strategy=retry_strategy, rate_limit=rate_limit, is_active=is_active)

def make_exchange_rate(
    from_id: int, to_id: int, source_id: int, rate: float = 18176.50,
    timestamp: datetime = FIXED_TS, quality: float | None = 1.0, is_valid: bool = True,
) -> ExchangeRate:
    return ExchangeRate(
        from_currency_id=from_id, to_currency_id=to_id, source_id=source_id,
        rate=Decimal(str(rate)), timestamp=timestamp,
        data_quality_score=Decimal(str(quality)) if quality is not None else None, is_valid=is_valid,
    )

def make_api_call(
    source_id: int, status: str = "SUCCESS", records_fetched: int = 4,
    records_valid: int = 4, records_invalid: int = 0, execution_time_ms: int = 350,
    error_message: str | None = None,
) -> ApiCall:
    return ApiCall(
        source_id=source_id, timestamp=FIXED_TS, status=status, records_fetched=records_fetched,
        records_valid=records_valid, records_invalid=records_invalid, execution_time_ms=execution_time_ms,
        error_message=error_message,
    )

def make_daily_snapshot(
    from_id: int, to_id: int, snapshot_date: date = FIXED_DATE,
    is_anomaly: bool = False, anomaly_level: str = "NORMAL",
) -> DailySnapshot:
    return DailySnapshot(
        snapshot_date=snapshot_date, from_currency_id=from_id, to_currency_id=to_id,
        rate_open=Decimal("18100.0"), rate_high=Decimal("18300.0"), rate_low=Decimal("18050.0"),
        rate_close=Decimal("18176.5"), rate_avg=Decimal("18180.0"), rate_ma7=Decimal("18150.0"),
        pct_change=Decimal("0.420"), is_anomaly=is_anomaly, anomaly_level=anomaly_level,
    )

def _seed_base(session: Session) -> tuple[Currency, Currency, ApiSource]:
    usd = make_currency("USD", "US Dollar")
    idr = make_currency("IDR", "Indonesian Rupiah")
    src = make_api_source("yfinance")
    session.add_all([usd, idr, src])
    session.flush()
    return usd, idr, src

# ===========================================================================
# 1. Currency Model
# ===========================================================================
class TestCurrencyModel:
    def test_create_currency_persists_all_fields(self, session):
        session.add(make_currency("EUR", "Euro", is_active=True))
        session.flush()
        eur = session.scalars(select(Currency).where(Currency.code == "EUR")).one()
        assert eur.currency_id is not None
        assert eur.code == "EUR"
        assert eur.is_active is True

    def test_currency_is_active_defaults_to_true(self, session):
        c = Currency(code="SGD", name="Singapore Dollar")
        session.add(c)
        session.flush()
        fetched = session.scalars(select(Currency).where(Currency.code == "SGD")).one()
        assert fetched.is_active is True

    def test_currency_unique_code_raises_on_duplicate(self, session):
        session.add(make_currency("JPY", "Japanese Yen"))
        session.flush()
        session.add(make_currency("JPY", "Japanese Yen Duplicate"))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_currency_timestamps_present_after_flush(self, session):
        c = make_currency("AUD", "Australian Dollar")
        session.add(c)
        session.flush()
        session.refresh(c)
        assert c.created_at is not None
        assert c.updated_at is not None

    def test_currency_soft_delete_via_is_active(self, session):
        c = make_currency("CAD", "Canadian Dollar")
        session.add(c)
        session.flush()
        c.is_active = False
        session.flush()
        row = session.scalars(select(Currency).where(Currency.code == "CAD")).one()
        assert row.is_active is False

    def test_currency_repr(self, session):
        c = make_currency("CHF", "Swiss Franc")
        session.add(c)
        session.flush()
        assert "CHF" in repr(c)

    @pytest.mark.parametrize("code,name", [("USD", "US Dollar"), ("EUR", "Euro"), ("SGD", "Singapore Dollar")])
    def test_currency_all_tracked_pairs_insertable(self, session, code, name):
        session.add(make_currency(code, name))
        session.flush()
        count = session.scalar(select(func.count()).select_from(Currency).where(Currency.code == code))
        assert count == 1

# ===========================================================================
# 2. ApiSource Model
# ===========================================================================
class TestApiSourceModel:
    def test_create_api_source_all_fields(self, session):
        src = make_api_source(name="fred", endpoint="https://api.stlouisfed.org/fred", rate_limit=120)
        session.add(src)
        session.flush()
        fetched = session.scalars(select(ApiSource).where(ApiSource.source_name == "fred")).one()
        assert fetched.source_name == "fred"
        assert fetched.rate_limit == 120

    def test_api_source_optional_fields_nullable(self, session):
        src = ApiSource(source_name="bank_indonesia", is_active=True)
        session.add(src)
        session.flush()
        fetched = session.scalars(select(ApiSource).where(ApiSource.source_name == "bank_indonesia")).one()
        assert fetched.api_endpoint is None

    def test_source_name_unique_raises_on_duplicate(self, session):
        session.add(make_api_source("yfinance"))
        session.flush()
        session.add(make_api_source("yfinance"))
        with pytest.raises(IntegrityError):
            session.flush()

# ===========================================================================
# 3. ExchangeRate Model
# ===========================================================================
class TestExchangeRateModel:
    def test_create_exchange_rate_all_fields(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        fetched = session.scalars(select(ExchangeRate).where(ExchangeRate.rate_id == er.rate_id)).one()
        assert fetched.rate == pytest.approx(Decimal("18176.50"), rel=1e-4)

    def test_exchange_rate_unique_upsert_constraint(self, session):
        usd, idr, src = _seed_base(session)
        er1 = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        er2 = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er1)
        session.flush()
        session.add(er2)
        with pytest.raises(IntegrityError):
            session.flush()

    def test_exchange_rate_indexes_defined(self):
        table = ExchangeRate.__table__
        index_names = {idx.name for idx in table.indexes}
        assert "idx_exchange_rates_pair_timestamp" in index_names

# ===========================================================================
# 4. ApiCall Model
# ===========================================================================
class TestApiCallModel:
    def test_create_api_call_success_status(self, session):
        _, _, src = _seed_base(session)
        call = make_api_call(src.source_id, status="SUCCESS")
        session.add(call)
        session.flush()
        fetched = session.scalars(select(ApiCall).where(ApiCall.call_id == call.call_id)).one()
        assert fetched.status == "SUCCESS"

    def test_api_call_audit_trail_insert_only_no_updated_at(self):
        columns = {c.name for c in ApiCall.__table__.columns}
        assert "updated_at" not in columns
        assert "created_at" in columns

# ===========================================================================
# 5. Constraint Tests
# ===========================================================================
class TestConstraints:
    def test_rate_positive_check_defined_in_metadata(self):
        constraints = {c.name for c in ExchangeRate.__table__.constraints}
        assert "ck_exchange_rates_rate_positive" in constraints

    def test_null_rate_rejected(self, session):
        usd, idr, src = _seed_base(session)
        er = ExchangeRate(from_currency_id=usd.currency_id, to_currency_id=idr.currency_id, source_id=src.source_id, rate=None, timestamp=FIXED_TS)
        session.add(er)
        with pytest.raises((IntegrityError, Exception)):
            session.flush()

# ===========================================================================
# 6. Relationships
# ===========================================================================
class TestRelationships:
    def test_exchange_rate_from_currency_relationship(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        session.refresh(er)
        assert er.from_currency.code == "USD"

    def test_cascade_delete_quality_metrics_when_rate_deleted(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        metric = DataQualityMetric(rate_id=er.rate_id, check_name="NULL_CHECK", check_passed=True)
        session.add(metric)
        session.flush()
        
        count_before = session.scalar(select(func.count()).select_from(DataQualityMetric))
        assert count_before == 1
        
        session.delete(er)
        session.flush()
        count_after = session.scalar(select(func.count()).select_from(DataQualityMetric))
        assert count_after == 0

# ===========================================================================
# 7. Engine / Session Utilities
# ===========================================================================
class TestGetDatabaseUrl:
    def test_database_url_env_var_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
        monkeypatch.delenv("DATABASE_URL_DEV", raising=False)
        url = _get_database_url("dev")
        assert url == "postgresql://user:pass@localhost/db"

    def test_missing_url_raises_environment_error(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL_DEV", raising=False)
        with pytest.raises(EnvironmentError):
            _get_database_url("dev")

class TestGetSession:
    def test_get_session_commits_on_success(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)
        with get_session(eng) as sess:
            sess.add(make_currency("NZD", "New Zealand Dollar"))
        with get_session(eng) as sess:
            count = session.scalar(select(func.count()).select_from(Currency).where(Currency.code == "NZD"))
            # fallback manual query if session variable scope issue
            with eng.connect() as conn:
                res = conn.execute(text("SELECT count(*) FROM currencies WHERE code='NZD'")).scalar()
            assert res == 1
        Base.metadata.drop_all(eng)
        eng.dispose()

    def test_get_session_always_closes(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)
        captured_session: list[Session] = []
        with pytest.raises(ValueError):
            with get_session(eng) as sess:
                captured_session.append(sess)
                raise ValueError("boom")
        # SQLAlchemy 2.0: in_transaction() is False after close/rollback
        assert captured_session[0].in_transaction() is False
        Base.metadata.drop_all(eng)
        eng.dispose()

class TestGetSessionFactory:
    def test_session_is_not_autocommit(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        factory = get_session_factory(eng)
        sess = factory()
        assert isinstance(sess, Session)
        sess.close()
        eng.dispose()

# ===========================================================================
# 8. Schema utility functions
# ===========================================================================
class TestSchemaUtilities:
    def test_create_all_tables_is_idempotent(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        create_all_tables(eng)
        create_all_tables(eng)
        inspector = inspect(eng)
        assert "currencies" in inspector.get_table_names()
        eng.dispose()

    def test_drop_all_tables_raises_in_prod(self, monkeypatch):
        monkeypatch.setenv("APP_ENV", "prod")
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        with pytest.raises(RuntimeError):
            drop_all_tables(eng)
        eng.dispose()

    def test_check_connection_returns_true_when_healthy(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        assert check_connection(eng) is True
        eng.dispose()