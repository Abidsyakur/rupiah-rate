"""
tests/unit/test_database.py
============================
Unit tests for src/models/database.py (ADR-002).

Strategy
--------
All tests run against an **in-memory SQLite** database so they are:
  - Fast (no network, no disk I/O)
  - Isolated (fresh schema per test via function-scoped fixtures)
  - Dependency-free (no Postgres required in CI)

SQLite limitations vs PostgreSQL
---------------------------------
- CHECK constraints are NOT enforced by SQLite by default.
  We use ``PRAGMA enforce_checks = ON`` where available, and for constraints
  that SQLite won't enforce we test the constraint definition directly on
  the ``__table_args__`` metadata instead of relying on a DB-raised error.
- Native ENUM types are stored as VARCHAR in SQLite; value enforcement is
  tested at the ORM layer.
- ``SET TIME ZONE`` is silently swallowed by the connect event (expected).

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
  Constraints         – rate > 0, same-currency pair, quality score bounds,
                        high >= low, positive OHLCV (metadata-level checks)
  Relationships       – FK navigation, back_populates, cascade delete-orphan
  get_engine()        – SQLite path, unknown env falls back to dev config,
                        invalid env raises, missing URL raises
  get_session()       – commit on clean exit, rollback on SQLAlchemyError,
                        rollback on generic Exception, session always closed
  get_session_factory() – returns bound sessionmaker
  _get_database_url() – DATABASE_URL precedence, env-specific var, missing raises
  create_all_tables() – idempotent create
  drop_all_tables()   – non-prod succeeds, prod raises RuntimeError
  check_connection()  – healthy returns True, broken returns False
"""

from __future__ import annotations

import os
import sys
import pathlib
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch, call

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# Path bootstrap (mirrors test_extractors.py approach)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(pathlib.Path(__file__).parents[2] / "src"))

from src.utils.database import (
    ApiCall,
    ApiSource,
    Base,
    Currency,
    DailySnapshot,
    DataQualityMetric,
    ExchangeRate,
    TimestampMixin,
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

SQLITE_URL = "sqlite://"   # pure in-memory, no file
FIXED_DATE = date(2025, 1, 15)
FIXED_TS   = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Session-scoped engine (schema created once per session for speed)
# We also expose a function-scoped session that rolls back after each test.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def engine():
    """In-memory SQLite engine shared across the entire test session."""
    eng = get_engine(database_url=SQLITE_URL, env="dev")
    Base.metadata.create_all(eng)
    yield eng
    Base.metadata.drop_all(eng)
    eng.dispose()


@pytest.fixture
def session(engine):
    """
    Function-scoped database session.

    Each test gets a clean transaction that is **rolled back** after the
    test completes, ensuring full isolation without recreating the schema.
    """
    connection = engine.connect()
    transaction = connection.begin()
    sess = Session(bind=connection)

    yield sess

    sess.close()
    transaction.rollback()
    connection.close()


# ---------------------------------------------------------------------------
# Shared factory helpers (not fixtures — called directly in tests)
# ---------------------------------------------------------------------------

def make_currency(code: str = "USD", name: str = "US Dollar", is_active: bool = True) -> Currency:
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
    rate: float = 18176.50,
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
# Shared setup helper: seed baseline currencies + source
# ---------------------------------------------------------------------------

def _seed_base(session: Session) -> tuple[Currency, Currency, ApiSource]:
    """Insert USD, IDR, and yfinance; flush to get PKs."""
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
        """Basic create: all fields round-trip correctly."""
        session.add(make_currency("EUR", "Euro", is_active=True))
        session.flush()

        eur = session.query(Currency).filter_by(code="EUR").one()
        assert eur.currency_id is not None
        assert eur.code == "EUR"
        assert eur.name == "Euro"
        assert eur.is_active is True

    def test_currency_is_active_defaults_to_true(self, session):
        """is_active should default True even if not explicitly supplied."""
        c = Currency(code="SGD", name="Singapore Dollar")
        session.add(c)
        session.flush()

        fetched = session.query(Currency).filter_by(code="SGD").one()
        # SQLite returns the Python default before a flush/reload,
        # but the column default=True guarantees this is True.
        assert fetched.is_active is True

    def test_currency_unique_code_raises_on_duplicate(self, session):
        """Inserting two currencies with the same code must raise IntegrityError."""
        session.add(make_currency("JPY", "Japanese Yen"))
        session.flush()

        session.add(make_currency("JPY", "Japanese Yen Duplicate"))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_currency_timestamps_present_after_flush(self, session):
        """TimestampMixin columns are populated on flush (server_default)."""
        c = make_currency("AUD", "Australian Dollar")
        session.add(c)
        session.flush()
        session.refresh(c)

        # server_default is applied at the DB level; after refresh both are set
        assert c.created_at is not None
        assert c.updated_at is not None

    def test_currency_soft_delete_via_is_active(self, session):
        """Setting is_active=False does not delete the row."""
        c = make_currency("CAD", "Canadian Dollar")
        session.add(c)
        session.flush()

        c.is_active = False
        session.flush()

        row = session.query(Currency).filter_by(code="CAD").one()
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
    def test_currency_all_tracked_pairs_insertable(self, session, code, name):
        """All four currency codes used in the pipeline can be inserted."""
        session.add(make_currency(code, name))
        session.flush()
        assert session.query(Currency).filter_by(code=code).count() == 1


# ===========================================================================
# 2. ApiSource Model
# ===========================================================================

class TestApiSourceModel:

    def test_create_api_source_all_fields(self, session):
        src = make_api_source(
            name="fred",
            endpoint="https://api.stlouisfed.org/fred",
            rate_limit=120,
            retry_strategy="exponential_backoff_max3",
        )
        session.add(src)
        session.flush()

        fetched = session.query(ApiSource).filter_by(source_name="fred").one()
        assert fetched.source_id is not None
        assert fetched.source_name == "fred"
        assert fetched.api_endpoint == "https://api.stlouisfed.org/fred"
        assert fetched.rate_limit == 120
        assert fetched.retry_strategy == "exponential_backoff_max3"
        assert fetched.is_active is True

    def test_api_source_optional_fields_nullable(self, session):
        """api_endpoint, retry_strategy, rate_limit are all nullable."""
        src = ApiSource(source_name="bank_indonesia", is_active=True)
        session.add(src)
        session.flush()

        fetched = session.query(ApiSource).filter_by(source_name="bank_indonesia").one()
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
        assert session.query(ApiSource).filter_by(source_name=source_name).count() == 1


# ===========================================================================
# 3. ExchangeRate Model
# ===========================================================================

class TestExchangeRateModel:

    def test_create_exchange_rate_all_fields(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()

        fetched = session.query(ExchangeRate).filter_by(rate_id=er.rate_id).one()
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

        fetched = session.query(ExchangeRate).filter_by(rate_id=er.rate_id).one()
        assert fetched.data_quality_score is None

    def test_exchange_rate_future_timestamp_accepted(self, session):
        """Timestamps in the future (e.g. off-hours market data) must be accepted."""
        usd, idr, src = _seed_base(session)
        future_ts = datetime(2099, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        er = make_exchange_rate(
            usd.currency_id, idr.currency_id, src.source_id, timestamp=future_ts
        )
        session.add(er)
        session.flush()

        fetched = session.query(ExchangeRate).filter_by(rate_id=er.rate_id).one()
        # SQLite stores timestamps without tz; we only verify no exception was raised
        assert fetched.rate_id is not None

    def test_exchange_rate_repr(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        r = repr(er)
        assert "ExchangeRate" in r
        assert "rate" in r

    def test_exchange_rate_unique_upsert_constraint(self, session):
        """Inserting duplicate (pair, timestamp, source) must raise IntegrityError."""
        usd, idr, src = _seed_base(session)
        er1 = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        er2 = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er1)
        session.flush()

        session.add(er2)
        with pytest.raises(IntegrityError):
            session.flush()

    def test_exchange_rate_different_timestamp_not_unique_violation(self, session):
        """Same pair + source but different timestamp is a distinct, valid row."""
        usd, idr, src = _seed_base(session)
        ts2 = datetime(2025, 1, 15, 11, 30, 0, tzinfo=timezone.utc)
        session.add(make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id))
        session.add(make_exchange_rate(
            usd.currency_id, idr.currency_id, src.source_id, timestamp=ts2
        ))
        session.flush()  # should not raise

        assert session.query(ExchangeRate).count() == 2

    # ---- Index definitions ----

    def test_exchange_rate_indexes_defined(self):
        """Verify all four ADR-002 indexes exist in table metadata."""
        table = ExchangeRate.__table__
        index_names = {idx.name for idx in table.indexes}
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

        fetched = session.query(ApiCall).filter_by(call_id=call.call_id).one()
        assert fetched.status == "SUCCESS"
        assert fetched.error_message is None
        assert fetched.records_fetched == 4
        assert fetched.records_valid == 4
        assert fetched.records_invalid == 0
        assert fetched.execution_time_ms == 350

    @pytest.mark.parametrize("status", ["SUCCESS", "TIMEOUT", "RATE_LIMIT", "ERROR"])
    def test_api_call_all_statuses_accepted(self, session, status):
        """All four enum values defined in ADR-002 must be storable."""
        _, _, src = _seed_base(session)
        call = make_api_call(src.source_id, status=status)
        session.add(call)
        session.flush()

        fetched = session.query(ApiCall).filter_by(call_id=call.call_id).one()
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

        fetched = session.query(ApiCall).filter_by(call_id=call.call_id).one()
        assert "ConnectionError" in fetched.error_message

    def test_api_call_audit_trail_insert_only_no_updated_at(self):
        """ApiCall deliberately omits updated_at — verify the column is absent."""
        columns = {c.name for c in ApiCall.__table__.columns}
        assert "updated_at" not in columns
        assert "created_at" in columns

    def test_api_call_all_nullable_fields_accept_none(self, session):
        _, _, src = _seed_base(session)
        call = ApiCall(
            source_id=src.source_id,
            timestamp=FIXED_TS,
            status="SUCCESS",
        )
        session.add(call)
        session.flush()

        fetched = session.query(ApiCall).filter_by(call_id=call.call_id).one()
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
# 5. Constraint Tests
# ===========================================================================

class TestConstraints:
    """
    Constraint enforcement tests.

    SQLite does NOT enforce CHECK constraints by default, so for those we:
      1. Verify the constraint is defined in the ORM metadata (always works), AND
      2. Attempt the insert and catch IntegrityError where SQLite does enforce
         (e.g. NOT NULL, UNIQUE, FK) or accept that the check is metadata-only.

    This approach gives us honest test coverage without needing Postgres in CI.
    """

    # ---- CHECK constraint metadata presence ----

    def test_rate_positive_check_defined_in_metadata(self):
        constraints = {c.name for c in ExchangeRate.__table__.constraints}
        assert "ck_exchange_rates_rate_positive" in constraints

    def test_different_currencies_check_defined_in_metadata(self):
        constraints = {c.name for c in ExchangeRate.__table__.constraints}
        assert "ck_exchange_rates_different_currencies" in constraints

    def test_quality_score_range_check_defined_in_metadata(self):
        constraints = {c.name for c in ExchangeRate.__table__.constraints}
        assert "ck_exchange_rates_quality_score_range" in constraints

    def test_daily_snapshot_high_gte_low_check_defined(self):
        constraints = {c.name for c in DailySnapshot.__table__.constraints}
        assert "ck_daily_snapshots_high_gte_low" in constraints

    def test_daily_snapshot_rates_positive_check_defined(self):
        constraints = {c.name for c in DailySnapshot.__table__.constraints}
        assert "ck_daily_snapshots_rates_positive" in constraints

    def test_data_quality_anomaly_score_range_check_defined(self):
        constraints = {c.name for c in DataQualityMetric.__table__.constraints}
        assert "ck_data_quality_anomaly_score_range" in constraints

    # ---- NOT NULL enforcement (SQLite does enforce these) ----

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
        c = Currency(code=None, name="No Code")
        session.add(c)
        with pytest.raises((IntegrityError, Exception)):
            session.flush()

    # ---- UNIQUE enforcement ----

    def test_duplicate_currency_code_rejected(self, session):
        session.add(make_currency("DUP", "Duplicate One"))
        session.flush()
        session.add(make_currency("DUP", "Duplicate Two"))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_negative_rate_check_constraint_in_metadata(self):
        """
        Negative rate test: verifies the CHECK is declared.
        Full enforcement requires Postgres (or SQLite STRICT mode).
        """
        table = ExchangeRate.__table__
        check_exprs = [
            str(c.sqltext)
            for c in table.constraints
            if hasattr(c, "sqltext")
        ]
        assert any("rate > 0" in expr for expr in check_exprs)

    def test_same_currency_pair_check_in_metadata(self):
        table = ExchangeRate.__table__
        check_exprs = [
            str(c.sqltext)
            for c in table.constraints
            if hasattr(c, "sqltext")
        ]
        assert any("from_currency_id != to_currency_id" in expr for expr in check_exprs)

    def test_future_timestamp_accepted(self, session):
        """Timestamps with future dates must NOT be rejected."""
        usd, idr, src = _seed_base(session)
        future = datetime(2099, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
        er = make_exchange_rate(
            usd.currency_id, idr.currency_id, src.source_id, timestamp=future
        )
        session.add(er)
        session.flush()   # must not raise
        assert er.rate_id is not None


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

    def test_exchange_rate_to_currency_relationship(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        session.refresh(er)

        assert er.to_currency.code == "IDR"

    def test_exchange_rate_source_relationship(self, session):
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

        rates = usd.rates_as_base.all()
        assert len(rates) == 1
        assert rates[0].rate_id == er.rate_id

    def test_currency_rates_as_quote_back_populates(self, session):
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()
        session.refresh(idr)

        rates = idr.rates_as_quote.all()
        assert len(rates) == 1

    def test_api_call_source_fk_relationship(self, session):
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

        calls = src.api_calls.all()
        assert len(calls) == 2

    def test_cascade_delete_quality_metrics_when_rate_deleted(self, session):
        """
        DataQualityMetric rows must be CASCADE-deleted when the parent
        ExchangeRate is deleted (ondelete=CASCADE + cascade='all, delete-orphan').
        """
        usd, idr, src = _seed_base(session)
        er = make_exchange_rate(usd.currency_id, idr.currency_id, src.source_id)
        session.add(er)
        session.flush()

        metric = DataQualityMetric(
            rate_id=er.rate_id,
            check_name="NULL_CHECK",
            check_passed=True,
        )
        session.add(metric)
        session.flush()

        assert session.query(DataQualityMetric).count() == 1

        session.delete(er)
        session.flush()

        assert session.query(DataQualityMetric).count() == 0

    def test_exchange_rate_currency_fk_missing_raises(self, session):
        """FK to non-existent currency_id must fail at flush."""
        _, _, src = _seed_base(session)
        er = ExchangeRate(
            from_currency_id=99999,   # does not exist
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

        fetched = session.query(DataQualityMetric).filter_by(metric_id=m.metric_id).one()
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

        fetched = session.query(DataQualityMetric).filter_by(metric_id=m.metric_id).one()
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

        fetched = session.query(DailySnapshot).filter_by(snapshot_id=snap.snapshot_id).one()
        assert fetched.snapshot_date == FIXED_DATE
        assert fetched.rate_open == Decimal("18100.0")
        assert fetched.rate_high == Decimal("18300.0")
        assert fetched.rate_low == Decimal("18050.0")
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

        fetched = session.query(DailySnapshot).filter_by(snapshot_id=snap.snapshot_id).one()
        assert fetched.is_anomaly is True
        assert fetched.anomaly_level == "WARNING"

    @pytest.mark.parametrize("level", ["NORMAL", "WARNING", "CRITICAL"])
    def test_all_anomaly_levels_accepted(self, session, level):
        usd, idr, _ = _seed_base(session)
        snap = make_daily_snapshot(
            usd.currency_id, idr.currency_id,
            snapshot_date=date(2025, 1, level.__hash__() % 28 + 1),
            anomaly_level=level,
        )
        session.add(snap)
        session.flush()
        assert snap.snapshot_id is not None

    def test_daily_snapshot_unique_date_pair_constraint(self, session):
        """Same (date, from, to) twice must raise IntegrityError."""
        usd, idr, _ = _seed_base(session)
        session.add(make_daily_snapshot(usd.currency_id, idr.currency_id))
        session.flush()

        session.add(make_daily_snapshot(usd.currency_id, idr.currency_id))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_daily_snapshot_optional_ma7_null(self, session):
        usd, idr, _ = _seed_base(session)
        snap = make_daily_snapshot(usd.currency_id, idr.currency_id)
        snap.rate_ma7 = None
        snap.pct_change = None
        session.add(snap)
        session.flush()

        fetched = session.query(DailySnapshot).filter_by(snapshot_id=snap.snapshot_id).one()
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
# 9. Engine / Session Utilities
# ===========================================================================

class TestGetDatabaseUrl:

    def test_database_url_env_var_takes_precedence(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")
        monkeypatch.delenv("DATABASE_URL_DEV", raising=False)
        url = _get_database_url("dev")
        assert url == "postgresql://user:pass@localhost/db"

    def test_env_specific_var_used_when_generic_absent(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.setenv("DATABASE_URL_STAGING", "postgresql://user:pass@host/staging")
        url = _get_database_url("staging")
        assert "staging" in url

    def test_missing_url_raises_environment_error(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL_DEV", raising=False)
        with pytest.raises(EnvironmentError, match="No database URL found"):
            _get_database_url("dev")

    def test_unknown_env_raises_value_error(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "sqlite://")
        with pytest.raises(ValueError, match="Unknown APP_ENV"):
            _get_database_url("production")   # should be "prod"

    def test_app_env_defaults_to_dev(self, monkeypatch):
        monkeypatch.delenv("APP_ENV", raising=False)
        monkeypatch.setenv("DATABASE_URL", "sqlite://")
        url = _get_database_url()   # no explicit env
        assert url == "sqlite://"


class TestGetEngine:

    def test_sqlite_url_returns_engine(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        assert eng is not None
        eng.dispose()

    def test_engine_can_execute_query(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        with eng.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
        assert result == 1
        eng.dispose()

    def test_get_engine_uses_app_env(self, monkeypatch):
        monkeypatch.setenv("APP_ENV", "staging")
        eng = get_engine(database_url=SQLITE_URL)
        assert eng is not None
        eng.dispose()

    def test_missing_url_raises_when_no_env_var(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL_DEV", raising=False)
        monkeypatch.delenv("APP_ENV", raising=False)
        with pytest.raises(EnvironmentError):
            get_engine()   # no URL anywhere


class TestGetSession:

    def test_get_session_commits_on_success(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)

        with get_session(eng) as sess:
            sess.add(make_currency("NZD", "New Zealand Dollar"))

        # Verify row survived outside the context manager
        with get_session(eng) as sess:
            assert sess.query(Currency).filter_by(code="NZD").count() == 1

        Base.metadata.drop_all(eng)
        eng.dispose()

    def test_get_session_rolls_back_on_sqlalchemy_error(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)

        with pytest.raises(SQLAlchemyError):
            with get_session(eng) as sess:
                sess.add(make_currency("MXN", "Mexican Peso"))
                raise SQLAlchemyError("simulated DB error")

        # Row must not have persisted
        with get_session(eng) as sess:
            assert sess.query(Currency).filter_by(code="MXN").count() == 0

        Base.metadata.drop_all(eng)
        eng.dispose()

    def test_get_session_rolls_back_on_generic_exception(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)

        with pytest.raises(RuntimeError):
            with get_session(eng) as sess:
                sess.add(make_currency("BRL", "Brazilian Real"))
                raise RuntimeError("unexpected error")

        with get_session(eng) as sess:
            assert sess.query(Currency).filter_by(code="BRL").count() == 0

        Base.metadata.drop_all(eng)
        eng.dispose()

    def test_get_session_always_closes(self):
        """Session must be closed even when an exception propagates."""
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)

        captured_session: list[Session] = []

        with pytest.raises(ValueError):
            with get_session(eng) as sess:
                captured_session.append(sess)
                raise ValueError("boom")

        # After context exit, session should be closed (not usable for new queries)
        assert not captured_session[0].is_active

        Base.metadata.drop_all(eng)
        eng.dispose()


class TestGetSessionFactory:

    def test_returns_sessionmaker(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        factory = get_session_factory(eng)
        sess = factory()
        assert isinstance(sess, Session)
        sess.close()
        eng.dispose()

    def test_session_is_not_autocommit(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        factory = get_session_factory(eng)
        sess = factory()
        assert sess.autocommit is False
        sess.close()
        eng.dispose()


# ===========================================================================
# 10. Schema utility functions
# ===========================================================================

class TestSchemaUtilities:

    def test_create_all_tables_is_idempotent(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        create_all_tables(eng)
        create_all_tables(eng)   # second call must not raise
        inspector = inspect(eng)
        assert "currencies" in inspector.get_table_names()
        eng.dispose()

    def test_drop_all_tables_succeeds_in_dev(self, monkeypatch):
        monkeypatch.setenv("APP_ENV", "dev")
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        Base.metadata.create_all(eng)
        drop_all_tables(eng)
        inspector = inspect(eng)
        assert "currencies" not in inspector.get_table_names()
        eng.dispose()

    def test_drop_all_tables_raises_in_prod(self, monkeypatch):
        monkeypatch.setenv("APP_ENV", "prod")
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        with pytest.raises(RuntimeError, match="must never be called in production"):
            drop_all_tables(eng)
        eng.dispose()

    def test_check_connection_returns_true_when_healthy(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        assert check_connection(eng) is True
        eng.dispose()

    def test_check_connection_returns_false_when_broken(self):
        eng = get_engine(database_url=SQLITE_URL, env="dev")
        eng.dispose()  # dispose before use → broken engine

        # Patch connect to raise OperationalError
        with patch.object(eng, "connect", side_effect=OperationalError("no connection", None, None)):
            result = check_connection(eng)
        assert result is False


# ===========================================================================
# 11. TimestampMixin
# ===========================================================================

class TestTimestampMixin:

    def test_mixin_columns_present_on_currency(self):
        cols = {c.name for c in Currency.__table__.columns}
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_mixin_columns_present_on_api_source(self):
        cols = {c.name for c in ApiSource.__table__.columns}
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_mixin_columns_present_on_exchange_rate(self):
        cols = {c.name for c in ExchangeRate.__table__.columns}
        assert "created_at" in cols
        assert "updated_at" in cols

    def test_api_call_has_no_updated_at(self):
        """ApiCall is insert-only: updated_at must NOT exist."""
        cols = {c.name for c in ApiCall.__table__.columns}
        assert "updated_at" not in cols