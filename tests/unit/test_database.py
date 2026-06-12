"""
tests/unit/test_database.py
============================
Unit tests for src/models/database.py (ADR-002).

SQLAlchemy 2.0 patterns used in this test file
-----------------------------------------------
- ``session.scalars(select(...))``     instead of ``session.query(...)``
- ``select(Model).where(...)``         instead of ``filter_by(...)``
- ``select(func.count()).select_from`` for COUNT queries
- ``Session(bind=connection)``         replaced by ``Session(connection)``  (2.0)
- Relationships are plain ``list``     — no more ``.all()`` on dynamic proxies
- ``session.get(Model, pk)``           for PK lookups (2.0 preferred API)

SQLite notes
------------
- CHECK constraints are NOT enforced by SQLite by default.
  Constraint existence is verified via ORM metadata; enforcement tests
  that need the DB to reject a value are skipped with a marker or test
  the metadata expression directly.
- ENUM types are stored as VARCHAR in SQLite.
- ``SET TIME ZONE`` in the connect event is silently ignored (expected).

Coverage targets (>85%)
-----------------------
  Currency            – create, unique code, timestamps, is_active default, repr
  ApiSource           – create, unique name, nullable optionals, repr
  ExchangeRate        – create, FK relationships, quality_score, is_valid default,
                        unique upsert constraint, index definitions, repr
  ApiCall             – create, all statuses, insert-only (no updated_at),
                        audit fields, repr
  DataQualityMetric   – create, cascade delete, check names, repr
  DailySnapshot       – create, OHLCV fields, anomaly fields, unique date+pair, repr
  Constraints         – metadata-level CHECK verification + NOT NULL + UNIQUE
  Relationships       – FK navigation, back_populates, cascade delete-orphan
  Enums               – ApiCallStatusEnum, AnomalyLevelEnum values
  get_engine()        – SQLite path, env detection, missing URL raises
  get_session()       – commit, rollback on SQLAlchemyError, rollback on Exception,
                        session always closed
  get_session_factory() – returns bound sessionmaker, autocommit=False
  _get_database_url() – DATABASE_URL precedence, env-specific var, missing raises
  create_all_tables() – idempotent
  drop_all_tables()   – non-prod ok, prod raises RuntimeError
  check_connection()  – healthy True, broken False
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy import func, inspect, select, text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from src.utils.database import (
    ApiCall,
    ApiCallStatusEnum,
    AnomalyLevelEnum,
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SQLITE_URL = "sqlite://"
FIXED_DATE = date(2025, 1, 15)
FIXED_TS   = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fixtures: `engine` and `session` are provided by tests/conftest.py
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Builder helpers  (plain functions, not fixtures)
# ---------------------------------------------------------------------------

def make_currency(
    code: str = "USD",
    name: str = "US Dollar",
    is_active: bool = True,
) -> Currency:
    return Currency(code=code, name=name, is_active=is_active)


def make_api_source(
    name: str = "yfinance",
    endpoint: str | None = "https://query1.finance.yahoo.com",
    rate_limit: int | None = 2000,
    retry_strategy: str | None = "exponential_backoff_max3",
    is_active: bool = True,
) -> ApiSource:
    return ApiSource(
        source_name=name,
        api_endpoint=endpoint,
        retry_strategy=retry_strategy,
        rate_limit=rate_limit,
        is_active=is_active,
    )


def make_exchange_rate(
    from_id: int,
    to_id: int,
    source_id: int,
    rate: float = 18_176.50,
    timestamp: datetime = FIXED_TS,
    quality: float | None = 1.0,
    is_valid: bool = True,
) -> ExchangeRate:
    return ExchangeRate(
        from_currency_id=from_id,
        to_currency_id=to_id,
        source_id=source_id,
        rate=Decimal(str(rate)),
        timestamp=timestamp,
        data_quality_score=Decimal(str(quality)) if quality is not None else None,
        is_valid=is_valid,
    )


def make_api_call(
    source_id: int,
    status: str = "SUCCESS",
    records_fetched: int = 4,
    records_valid: int = 4,
    records_invalid: int = 0,
    execution_time_ms: int = 350,
    error_message: str | None = None,
) -> ApiCall:
    return ApiCall(
        source_id=source_id,
        timestamp=FIXED_TS,
        status=status,
        records_fetched=records_fetched,
        records_valid=records_valid,
        records_invalid=records_invalid,
        execution_time_ms=execution_time_ms,
        error_message=error_message,
    )


def make_daily_snapshot(
    from_id: int,
    to_id: int,
    snapshot_date: date = FIXED_DATE,
    is_anomaly: bool = False,
    anomaly_level: str = "NORMAL",
) -> DailySnapshot:
    return DailySnapshot(
        snapshot_date=snapshot_date,
        from_currency_id=from_id,
        to_currency_id=to_id,
        rate_open=Decimal("18100.0"),
        rate_high=Decimal("18300.0"),
        rate_low=Decimal("18050.0"),
        rate_close=Decimal("18176.5"),
        rate_avg=Decimal("18180.0"),
        rate_ma7=Decimal("18150.0"),
        pct_change=Decimal("0.420"),
        is_anomaly=is_anomaly,
        anomaly_level=anomaly_level,
    )


# ---------------------------------------------------------------------------
# Shared setup: seed USD + IDR + yfinance source
# ---------------------------------------------------------------------------

def _seed_base(session: Session) -> tuple[Currency, Currency, ApiSource]:
    """Insert USD, IDR, yfinance and flush so PKs are assigned."""
    usd = make_currency("USD", "US Dollar")
    idr = make_currency("IDR", "Indonesian Rupiah")
    src = make_api_source("yfinance")
    session.add_all([usd, idr, src])
    session.flush()
    return usd, idr, src


# ---------------------------------------------------------------------------
# 2.0 query helpers (keeps test bodies clean)
# ---------------------------------------------------------------------------

def _one(session: Session, model, **kwargs):
    """Return the single row matching the given kwargs (2.0 select style)."""
    stmt = select(model)
    for col, val in kwargs.items():
        stmt = stmt.where(getattr(model, col) == val)
    return session.scalars(stmt).one()


def _count(session: Session, model) -> int:
    """Return the total number of rows in the table."""
    return session.scalar(select(func.count()).select_from(model))


# ===========================================================================
# 1. Currency Model
# ===========================================================================

class TestCurrencyModel:

    def test_create_currency_persists_all_fields(self, session):
        session.add(make_currency("EUR", "Euro", is_active=True))
        session.flush()

        eur = _one(session, Currency, code="EUR")
        assert eur.currency_id is not None
        assert eur.code == "EUR"
        assert eur.name == "Euro"
        assert eur.is_active is True

    def test_currency_is_active_defaults_to_true(self, session):
        c = Currency(code="SGD", name="Singapore Dollar")
        session.add(c)
        session.flush()

        fetched = _one(session, Currency, code="SGD")
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

        row = _one(session, Currency, code="CAD")
        assert row.is_active is False
        assert row.currency_id is not None

    def test_currency_repr(self, session):
        c = make_currency("CHF", "Swiss Franc")
        session.add(c)
        session.flush()
        assert "CHF" in repr(c)
        assert "Currency" in repr(c)

    @pytest.mark.parametrize("code,name", [
        ("USD", "US Dollar"),
        ("EUR", "Euro"),
        ("SGD", "Singapore Dollar"),
        ("JPY", "Japanese Yen"),
        ("IDR", "Indonesian Rupiah"),
    ])
    def test_all_tracked_currencies_insertable(self, session, code, name):
        session.add(make_currency(code, name))
        session.flush()
        assert _count(session, Currency) >= 1


# ===========================================================================
# 2. ApiSource Model
# ===========================================================================

class TestApiSourceModel:

    def test_create_api_source_all_fields(self, session):
        session.add(make_api_source(
            name="fred",
            endpoint="https://api.stlouisfed.org/fred",
            rate_limit=120,
            retry_strategy="exponential_backoff_max3",
        ))
        session.flush()

        fetched = _one(session, ApiSource, source_name="fred")
        assert fetched.source_id is not None
        assert fetched.source_name == "fred"
        assert fetched.api_endpoint == "https://api.stlouisfed.org/fred"
        assert fetched.rate_limit == 120
        assert fetched.retry_strategy == "exponential_backoff_max3"
        assert fetched.is_active is True

    def test_api_source_optional_fields_nullable(self, session):
        session.add(ApiSource(source_name="bank_indonesia", is_active=True))
        session.flush()

        fetched = _one(session, ApiSource, source_name="bank_indonesia")
        assert fetched.api_endpoint is None
        assert fetched.retry_strategy is None
        assert fetched.rate_limit is None

    def test_source_name_unique_raises_on_duplicate(self, session):
        session.add(make_api_source("yfinance"))
        session.flush()

        session.add(make_api_source("yfinance"))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_api_source_timestamps(self, session):
        src = make_api_source("fred_test")
        session.add(src)
        session.flush()
        session.refresh(src)
        assert src.created_at is not None

    def test_api_source_repr(self, session):
        src = make_api_source("yfinance_repr")
        session.add(src)
        session.flush()
        assert "yfinance_repr" in repr(src)
        assert "ApiSource" in repr(src)

    @pytest.mark.parametrize("source_name", ["yfinance", "fred", "bank_indonesia"])
    def test_all_pipeline_sources_insertable(self, session, source_name):
        session.add(ApiSource(source_name=source_name, is_active=True))
        session.flush()
        result = session.scalars(
            select(ApiSource).where(ApiSource.source_name == source_name)
        ).one()
        assert result.source_name == source_name


# ===========================================================================
# 3. ExchangeRate Model
# ===========================================================================

class TestExchangeRateModel:

    def test_create_exchange_rate_all_fields(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()

        fetched = session.get(ExchangeRate, er.rate_id)
        assert fetched.rate == pytest.approx(Decimal("18176.50"), rel=1e-4)
        assert fetched.from_currency_id == usd.currency_id
        assert fetched.to_currency_id == idr.currency_id
        assert fetched.source_id == src.source_id
        assert fetched.data_quality_score == Decimal("1.00")
        assert fetched.is_valid is True

    def test_exchange_rate_is_valid_defaults_true(self, session):
        usd, idr, src = _seed_base(session)
        er = ExchangeRate(
            from_currency_id=usd.currency_id,
            to_currency_id=idr.currency_id,
            source_id=src.source_id,
            rate=Decimal("18000.0"),
            timestamp=FIXED_TS,
        )
        session.add(er)
        session.flush()
        assert er.is_valid is True

    def test_exchange_rate_quality_score_nullable(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(
            usd.currency_id, idr.currency_id, src.source_id, quality=None
        )
        session.add(er)
        session.flush()

        fetched = session.get(ExchangeRate, er.rate_id)
        assert fetched.data_quality_score is None

    def test_exchange_rate_future_timestamp_accepted(self, session):
        usd, idr, src = _seed_base(session)
        future_ts = datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        er = make_exchange_rate(
            usd.currency_id, idr.currency_id, src.source_id, timestamp=future_ts
        )
        session.add(er)
        session.flush()
        assert er.rate_id is not None

    def test_exchange_rate_repr(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        r = repr(er)
        assert "ExchangeRate" in r
        assert "rate" in r

    def test_exchange_rate_unique_upsert_constraint(self, session):
        usd, idr, src = _seed_base(session)
        session.add(make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id))
        session.flush()

        session.add(make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_exchange_rate_different_timestamp_is_distinct_row(self, session):
        usd, idr, src = _seed_base(session)
        ts2 = datetime(2025, 1, 15, 11, 30, 0, tzinfo=timezone.utc)
        session.add(make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id))
        session.add(make_exchange_rate(
            usd.currency_id, idr.currency_id, src.source_id, timestamp=ts2
        ))
        session.flush()  # must not raise

        assert _count(session, ExchangeRate) == 2

    def test_exchange_rate_indexes_defined(self):
        index_names = {idx.name for idx in ExchangeRate.__table__.indexes}
        assert "idx_exchange_rates_pair_timestamp" in index_names
        assert "idx_exchange_rates_timestamp" in index_names
        assert "idx_exchange_rates_source_id" in index_names
        assert "idx_exchange_rates_is_valid" in index_names


# ===========================================================================
# 4. ApiCall Model
# ===========================================================================

class TestApiCallModel:

    def test_create_api_call_success_status(self, session):
        _, _, src = _seed_base(session)
        call = make_api_call(src.source_id, status="SUCCESS")
        session.add(call)
        session.flush()

        fetched = session.get(ApiCall, call.call_id)
        assert fetched.status == "SUCCESS"
        assert fetched.error_message is None
        assert fetched.records_fetched == 4
        assert fetched.records_valid == 4
        assert fetched.records_invalid == 0
        assert fetched.execution_time_ms == 350

    @pytest.mark.parametrize("status", ["SUCCESS", "TIMEOUT", "RATE_LIMIT", "ERROR"])
    def test_api_call_all_statuses_accepted(self, session, status):
        _, _, src = _seed_base(session)
        call = make_api_call(src.source_id, status=status)
        session.add(call)
        session.flush()

        fetched = session.get(ApiCall, call.call_id)
        assert fetched.status == status

    def test_api_call_error_message_populated_on_failure(self, session):
        _, _, src = _seed_base(session)
        call = make_api_call(
            src.source_id,
            status="ERROR",
            records_fetched=0,
            records_valid=0,
            records_invalid=0,
            error_message="ConnectionError: timed out after 30s",
        )
        session.add(call)
        session.flush()

        fetched = session.get(ApiCall, call.call_id)
        assert "ConnectionError" in fetched.error_message

    def test_api_call_insert_only_no_updated_at(self):
        """ApiCall is insert-only — updated_at must be absent."""
        columns = {c.name for c in ApiCall.__table__.columns}
        assert "updated_at" not in columns
        assert "created_at" in columns

    def test_api_call_nullable_fields_accept_none(self, session):
        _, _, src = _seed_base(session)
        call = ApiCall(
            source_id=src.source_id,
            timestamp=FIXED_TS,
            status="SUCCESS",
        )
        session.add(call)
        session.flush()

        fetched = session.get(ApiCall, call.call_id)
        assert fetched.records_fetched is None
        assert fetched.records_valid is None
        assert fetched.records_invalid is None
        assert fetched.execution_time_ms is None

    def test_api_call_repr(self, session):
        _, _, src = _seed_base(session)
        call = make_api_call(src.source_id)
        session.add(call)
        session.flush()
        assert "ApiCall" in repr(call)
        assert "SUCCESS" in repr(call)

    def test_api_call_indexes_defined(self):
        index_names = {idx.name for idx in ApiCall.__table__.indexes}
        assert "idx_api_calls_source_timestamp" in index_names
        assert "idx_api_calls_status" in index_names


# ===========================================================================
# 5. Constraints
# ===========================================================================

class TestConstraints:
    """
    SQLite does NOT enforce CHECK constraints, so:
      - NOT NULL and UNIQUE violations → raise IntegrityError (SQLite enforces)
      - CHECK constraint presence     → verified via ORM metadata
      - CHECK expression content      → asserted on sqltext string
    """

    # ---- CHECK metadata presence ----

    def test_rate_positive_check_defined(self):
        names = {c.name for c in ExchangeRate.__table__.constraints}
        assert "ck_exchange_rates_rate_positive" in names

    def test_different_currencies_check_defined(self):
        names = {c.name for c in ExchangeRate.__table__.constraints}
        assert "ck_exchange_rates_different_currencies" in names

    def test_quality_score_range_check_defined(self):
        names = {c.name for c in ExchangeRate.__table__.constraints}
        assert "ck_exchange_rates_quality_score_range" in names

    def test_daily_snapshot_high_gte_low_check_defined(self):
        names = {c.name for c in DailySnapshot.__table__.constraints}
        assert "ck_daily_snapshots_high_gte_low" in names

    def test_daily_snapshot_rates_positive_check_defined(self):
        names = {c.name for c in DailySnapshot.__table__.constraints}
        assert "ck_daily_snapshots_rates_positive" in names

    def test_data_quality_anomaly_score_range_check_defined(self):
        names = {c.name for c in DataQualityMetric.__table__.constraints}
        assert "ck_data_quality_anomaly_score_range" in names

    # ---- CHECK expression content ----

    def test_rate_positive_sqltext(self):
        exprs = [
            str(c.sqltext)
            for c in ExchangeRate.__table__.constraints
            if hasattr(c, "sqltext")
        ]
        assert any("rate > 0" in e for e in exprs)

    def test_different_currencies_sqltext(self):
        exprs = [
            str(c.sqltext)
            for c in ExchangeRate.__table__.constraints
            if hasattr(c, "sqltext")
        ]
        assert any("from_currency_id != to_currency_id" in e for e in exprs)

    # ---- NOT NULL enforcement ----

    def test_null_rate_rejected(self, session):
        usd, idr, src = _seed_base(session)
        er = ExchangeRate(
            from_currency_id=usd.currency_id,
            to_currency_id=idr.currency_id,
            source_id=src.source_id,
            rate=None,
            timestamp=FIXED_TS,
        )
        session.add(er)
        with pytest.raises((IntegrityError, Exception)):
            session.flush()

    def test_null_timestamp_rejected(self, session):
        usd, idr, src = _seed_base(session)
        er = ExchangeRate(
            from_currency_id=usd.currency_id,
            to_currency_id=idr.currency_id,
            source_id=src.source_id,
            rate=Decimal("18176.50"),
            timestamp=None,
        )
        session.add(er)
        with pytest.raises((IntegrityError, Exception)):
            session.flush()

    def test_null_currency_code_rejected(self, session):
        session.add(Currency(code=None, name="No Code"))
        with pytest.raises((IntegrityError, Exception)):
            session.flush()

    # ---- UNIQUE enforcement ----

    def test_duplicate_currency_code_rejected(self, session):
        session.add(make_currency("DUP", "Dup One"))
        session.flush()
        session.add(make_currency("DUP", "Dup Two"))
        with pytest.raises(IntegrityError):
            session.flush()

    # ---- Future timestamp ----

    def test_future_timestamp_accepted(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(
            usd.currency_id, idr.currency_id, src.source_id,
            timestamp=datetime(2099, 6, 15, tzinfo=timezone.utc),
        )
        session.add(er)
        session.flush()
        assert er.rate_id is not None


# ===========================================================================
# 6. Relationships  (2.0: plain list, no .all())
# ===========================================================================

class TestRelationships:

    def test_exchange_rate_from_currency(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        session.refresh(er)

        assert er.from_currency.code == "USD"

    def test_exchange_rate_to_currency(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        session.refresh(er)

        assert er.to_currency.code == "IDR"

    def test_exchange_rate_source(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        session.refresh(er)

        assert er.source.source_name == "yfinance"

    def test_currency_rates_as_base_back_populates(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        session.refresh(usd)

        # 2.0: rates_as_base is a plain list, not a dynamic proxy
        assert len(usd.rates_as_base) == 1
        assert usd.rates_as_base[0].rate_id == er.rate_id

    def test_currency_rates_as_quote_back_populates(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        session.refresh(idr)

        assert len(idr.rates_as_quote) == 1

    def test_api_call_source_fk(self, session):
        _, _, src = _seed_base(session)
        call = make_api_call(src.source_id)
        session.add(call)
        session.flush()
        session.refresh(call)

        assert call.source.source_name == "yfinance"

    def test_api_source_api_calls_back_populates(self, session):
        _, _, src = _seed_base(session)
        session.add_all([
            make_api_call(src.source_id, status="SUCCESS"),
            make_api_call(src.source_id, status="ERROR"),
        ])
        session.flush()
        session.refresh(src)

        # 2.0: api_calls is a plain list
        assert len(src.api_calls) == 2

    def test_cascade_delete_removes_quality_metrics(self, session):
        """
        DataQualityMetric rows are CASCADE-deleted with their parent
        ExchangeRate (ondelete=CASCADE + cascade='all, delete-orphan').
        """
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()

        session.add(DataQualityMetric(
            rate_id=er.rate_id,
            check_name="NULL_CHECK",
            check_passed=True,
        ))
        session.flush()
        assert _count(session, DataQualityMetric) == 1

        session.delete(er)
        session.flush()
        assert _count(session, DataQualityMetric) == 0

    def test_exchange_rate_missing_fk_raises(self, session):
        _, _, src = _seed_base(session)
        er = ExchangeRate(
            from_currency_id=99999,
            to_currency_id=99998,
            source_id=src.source_id,
            rate=Decimal("18176.50"),
            timestamp=FIXED_TS,
        )
        session.add(er)
        with pytest.raises((IntegrityError, Exception)):
            session.flush()


# ===========================================================================
# 7. DataQualityMetric Model
# ===========================================================================

class TestDataQualityMetricModel:

    def test_create_metric_null_check(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()

        m = DataQualityMetric(
            rate_id=er.rate_id,
            check_name="NULL_CHECK",
            check_passed=True,
        )
        session.add(m)
        session.flush()

        fetched = session.get(DataQualityMetric, m.metric_id)
        assert fetched.check_name == "NULL_CHECK"
        assert fetched.check_passed is True
        assert fetched.anomaly_score is None

    @pytest.mark.parametrize("check_name", ["NULL_CHECK", "RANGE_CHECK", "ANOMALY_CHECK"])
    def test_all_check_names_accepted(self, session, check_name):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()

        m = DataQualityMetric(
            rate_id=er.rate_id,
            check_name=check_name,
            check_passed=True,
        )
        session.add(m)
        session.flush()
        assert m.metric_id is not None

    def test_metric_with_anomaly_score(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()

        m = DataQualityMetric(
            rate_id=er.rate_id,
            check_name="ANOMALY_CHECK",
            check_passed=False,
            anomaly_score=Decimal("0.85"),
        )
        session.add(m)
        session.flush()

        fetched = session.get(DataQualityMetric, m.metric_id)
        assert fetched.anomaly_score == Decimal("0.85")

    def test_metric_repr(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        m = DataQualityMetric(rate_id=er.rate_id, check_name="RANGE_CHECK", check_passed=True)
        session.add(m)
        session.flush()
        assert "DataQualityMetric" in repr(m)


# ===========================================================================
# 8. DailySnapshot Model
# ===========================================================================

class TestDailySnapshotModel:

    def test_create_daily_snapshot_all_fields(self, session):
        usd, idr, _ = _seed_base(session)
        snap = make_daily_snapshot(usd.currency_id, idr.currency_id)
        session.add(snap)
        session.flush()

        fetched = session.get(DailySnapshot, snap.snapshot_id)
        assert fetched.snapshot_date == FIXED_DATE
        assert fetched.rate_open  == Decimal("18100.0")
        assert fetched.rate_high  == Decimal("18300.0")
        assert fetched.rate_low   == Decimal("18050.0")
        assert fetched.rate_close == Decimal("18176.5")
        assert fetched.is_anomaly is False
        assert fetched.anomaly_level == "NORMAL"

    def test_daily_snapshot_anomaly_warning(self, session):
        usd, idr, _ = _seed_base(session)
        snap = make_daily_snapshot(
            usd.currency_id, idr.currency_id,
            is_anomaly=True, anomaly_level="WARNING",
        )
        session.add(snap)
        session.flush()

        fetched = session.get(DailySnapshot, snap.snapshot_id)
        assert fetched.is_anomaly is True
        assert fetched.anomaly_level == "WARNING"

    @pytest.mark.parametrize("level", ["NORMAL", "WARNING", "CRITICAL"])
    def test_all_anomaly_levels_accepted(self, session, level):
        usd, idr, _ = _seed_base(session)
        # Use a deterministic date per level to avoid UNIQUE violation
        level_date = {"NORMAL": date(2025, 2, 1),
                      "WARNING": date(2025, 2, 2),
                      "CRITICAL": date(2025, 2, 3)}[level]
        snap = make_daily_snapshot(
            usd.currency_id, idr.currency_id,
            snapshot_date=level_date,
            anomaly_level=level,
        )
        session.add(snap)
        session.flush()
        assert snap.snapshot_id is not None

    def test_daily_snapshot_unique_date_pair_constraint(self, session):
        usd, idr, _ = _seed_base(session)
        session.add(make_daily_snapshot(usd.currency_id, idr.currency_id))
        session.flush()

        session.add(make_daily_snapshot(usd.currency_id, idr.currency_id))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_daily_snapshot_optional_fields_accept_none(self, session):
        usd, idr, _ = _seed_base(session)
        snap = make_daily_snapshot(usd.currency_id, idr.currency_id)
        snap.rate_ma7 = None
        snap.pct_change = None
        session.add(snap)
        session.flush()

        fetched = session.get(DailySnapshot, snap.snapshot_id)
        assert fetched.rate_ma7 is None
        assert fetched.pct_change is None

    def test_daily_snapshot_repr(self, session):
        usd, idr, _ = _seed_base(session)
        snap = make_daily_snapshot(usd.currency_id, idr.currency_id)
        session.add(snap)
        session.flush()
        assert "DailySnapshot" in repr(snap)
        assert str(FIXED_DATE) in repr(snap)


# ===========================================================================
# 9. Python Enum values
# ===========================================================================

class TestEnumValues:

    def test_api_call_status_enum_values(self):
        assert ApiCallStatusEnum.SUCCESS.value    == "SUCCESS"
        assert ApiCallStatusEnum.TIMEOUT.value    == "TIMEOUT"
        assert ApiCallStatusEnum.RATE_LIMIT.value == "RATE_LIMIT"
        assert ApiCallStatusEnum.ERROR.value      == "ERROR"

    def test_anomaly_level_enum_values(self):
        assert AnomalyLevelEnum.NORMAL.value   == "NORMAL"
        assert AnomalyLevelEnum.WARNING.value  == "WARNING"
        assert AnomalyLevelEnum.CRITICAL.value == "CRITICAL"

    def test_api_call_status_enum_is_str_subclass(self):
        assert isinstance(ApiCallStatusEnum.SUCCESS, str)

    def test_anomaly_level_enum_is_str_subclass(self):
        assert isinstance(AnomalyLevelEnum.NORMAL, str)


# ===========================================================================
# 10. Engine / Session Utilities
# ===========================================================================

class TestGetDatabaseUrl:

    def test_database_url_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
        monkeypatch.delenv("DATABASE_URL_DEV", raising=False)
        assert _get_database_url("dev") == "postgresql://user:pass@localhost/db"

    def test_env_specific_var_used_when_generic_absent(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setenv("DATABASE_URL_STAGING", "postgresql://host/staging")
        assert "staging" in _get_database_url("staging")

    def test_missing_url_raises_environment_error(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL",     raising=False)
        monkeypatch.delenv("DATABASE_URL_DEV", raising=False)
        with pytest.raises(EnvironmentError, match="No database URL found"):
            _get_database_url("dev")

    def test_unknown_env_raises_value_error(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "sqlite://")
        with pytest.raises(ValueError, match="Unknown APP_ENV"):
            _get_database_url("production")

    def test_defaults_to_dev_when_app_env_unset(self, monkeypatch):
        monkeypatch.delenv("APP_ENV", raising=False)
        monkeypatch.setenv("DATABASE_URL", "sqlite://")
        assert _get_database_url() == "sqlite://"


class TestGetEngine:

    def test_sqlite_returns_engine(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        assert eng is not None
        eng.dispose()

    def test_engine_executes_select_1(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        with eng.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1
        eng.dispose()

    def test_uses_app_env(self, monkeypatch):
        monkeypatch.setenv("APP_ENV", "staging")
        eng = get_engine(database_url=SQLITE_URL)
        assert eng is not None
        eng.dispose()

    def test_missing_url_raises(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL",     raising=False)
        monkeypatch.delenv("DATABASE_URL_DEV", raising=False)
        monkeypatch.delenv("APP_ENV",          raising=False)
        with pytest.raises(EnvironmentError):
            get_engine()


class TestGetSession:

    def test_commits_on_clean_exit(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)

        with get_session(eng) as sess:
            sess.add(make_currency("NZD", "New Zealand Dollar"))

        with get_session(eng) as sess:
            result = sess.scalars(
                select(Currency).where(Currency.code == "NZD")
            ).one_or_none()
            assert result is not None

        Base.metadata.drop_all(eng)
        eng.dispose()

    def test_rolls_back_on_sqlalchemy_error(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)

        with pytest.raises(SQLAlchemyError):
            with get_session(eng) as sess:
                sess.add(make_currency("MXN", "Mexican Peso"))
                raise SQLAlchemyError("simulated DB error")

        with get_session(eng) as sess:
            result = sess.scalars(
                select(Currency).where(Currency.code == "MXN")
            ).one_or_none()
            assert result is None

        Base.metadata.drop_all(eng)
        eng.dispose()

    def test_rolls_back_on_generic_exception(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)

        with pytest.raises(RuntimeError):
            with get_session(eng) as sess:
                sess.add(make_currency("BRL", "Brazilian Real"))
                raise RuntimeError("unexpected")

        with get_session(eng) as sess:
            result = sess.scalars(
                select(Currency).where(Currency.code == "BRL")
            ).one_or_none()
            assert result is None

        Base.metadata.drop_all(eng)
        eng.dispose()

    def test_session_closed_after_exception(self):
        """After get_session() exits via exception, the session must be closed
        (no transaction/connection bound) — checked via session.get_bind()
        no longer raising and the identity map being cleared, rather than
        ``is_active`` which reflects transaction state, not closed state."""
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)

        captured: list[Session] = []
        with pytest.raises(ValueError):
            with get_session(eng) as sess:
                captured.append(sess)
                sess.add(make_currency("ISK", "Icelandic Krona"))
                raise ValueError("boom")

        closed_session = captured[0]
        # 2.0: a closed session's identity map is empty and any attempt
        # to use it will start a brand-new transaction — so we instead
        # assert the rolled-back object was expunged from the session.
        assert len(closed_session.new) == 0
        assert len(closed_session.identity_map) == 0

        Base.metadata.drop_all(eng)
        eng.dispose()


class TestGetSessionFactory:

    def test_returns_session_instance(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        factory = get_session_factory(eng)
        sess = factory()
        assert isinstance(sess, Session)
        sess.close()
        eng.dispose()

    def test_session_class_is_2_0_compatible(self):
        """Sanity check: sessionmaker produces a 2.0-style Session."""
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        factory = get_session_factory(eng)
        sess = factory()
        # 2.0 sessions are always "autocommit=False" semantically;
        # the autocommit attribute itself was removed.
        assert not hasattr(sess, "autocommit") or sess.autocommit is False
        sess.close()
        eng.dispose()


# ===========================================================================
# 11. Schema utilities
# ===========================================================================

class TestSchemaUtilities:

    def test_create_all_tables_idempotent(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        create_all_tables(eng)
        create_all_tables(eng)  # second call must not raise
        assert "currencies" in inspect(eng).get_table_names()
        eng.dispose()

    def test_drop_all_tables_succeeds_in_dev(self, monkeypatch):
        monkeypatch.setenv("APP_ENV", "dev")
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)
        drop_all_tables(eng)
        assert "currencies" not in inspect(eng).get_table_names()
        eng.dispose()

    def test_drop_all_tables_raises_in_prod(self, monkeypatch):
        monkeypatch.setenv("APP_ENV", "prod")
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        with pytest.raises(RuntimeError, match="must never be called in production"):
            drop_all_tables(eng)
        eng.dispose()

    def test_check_connection_returns_true(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        assert check_connection(eng) is True
        eng.dispose()

    def test_check_connection_returns_false_when_broken(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        eng.dispose()
        with patch.object(
            eng, "connect",
            side_effect=OperationalError("no connection", None, None),
        ):
            assert check_connection(eng) is False


# ===========================================================================
# 12. TimestampMixin
# ===========================================================================

class TestTimestampMixin:

    def test_currency_has_timestamps(self):
        cols = {c.name for c in Currency.__table__.columns}
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_api_source_has_timestamps(self):
        cols = {c.name for c in ApiSource.__table__.columns}
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_exchange_rate_has_timestamps(self):
        cols = {c.name for c in ExchangeRate.__table__.columns}
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_daily_snapshot_has_timestamps(self):
        cols = {c.name for c in DailySnapshot.__table__.columns}
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_api_call_has_no_updated_at(self):
        """ApiCall is insert-only — updated_at must be absent."""
        cols = {c.name for c in ApiCall.__table__.columns}
        assert "updated_at" not in cols
        assert "created_at" in cols