"""
src/models/database.py
======================
SQLAlchemy ORM models for the Rupiah Exchange Rate Intelligence platform.

Schema Reference: docs/SCHEMA.md (ADR-002)

Tables
------
- currencies          Dimension table for currency reference data
- api_sources         Configuration table for API source metadata
- exchange_rates      Fact table for raw exchange rate observations
- api_calls           Audit trail for every API call made
- data_quality_metrics  Per-record quality check results
- daily_snapshots     Pre-aggregated OHLCV daily data

Usage
-----
    from src.models.database import get_engine, get_session, Base
    from src.models.database import Currency, ExchangeRate

    engine = get_engine()
    Base.metadata.create_all(engine)

    with get_session(engine) as session:
        currencies = session.query(Currency).filter_by(is_active=True).all()
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import (
    DeclarativeBase,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.pool import NullPool, QueuePool
from sqlalchemy.sql import func

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Environment / config helpers
# ---------------------------------------------------------------------------

# Supported runtime environments
_ENV_DEV = "dev"
_ENV_STAGING = "staging"
_ENV_PROD = "prod"
_VALID_ENVS = {_ENV_DEV, _ENV_STAGING, _ENV_PROD}

# Connection-pool presets per environment (mirrors config/*.yaml intent)
_POOL_CONFIG: dict[str, dict] = {
    _ENV_DEV: {
        "pool_size": 2,
        "max_overflow": 3,
        "pool_timeout": 30,
        "pool_recycle": 1800,
        "pool_pre_ping": True,
        "echo": True,           # SQL logging on in dev
    },
    _ENV_STAGING: {
        "pool_size": 5,
        "max_overflow": 10,
        "pool_timeout": 30,
        "pool_recycle": 1800,
        "pool_pre_ping": True,
        "echo": False,
    },
    _ENV_PROD: {
        "pool_size": 10,
        "max_overflow": 20,
        "pool_timeout": 30,
        "pool_recycle": 900,    # recycle more aggressively in prod
        "pool_pre_ping": True,
        "echo": False,
    },
}


def _get_database_url(env: Optional[str] = None) -> str:
    """
    Resolve the database connection URL from environment variables.

    Lookup order
    ------------
    1. ``DATABASE_URL``           – generic override (all envs)
    2. ``DATABASE_URL_<ENV>``     – env-specific, e.g. ``DATABASE_URL_PROD``
    3. Raises ``EnvironmentError`` if neither is set.

    Parameters
    ----------
    env:
        Runtime environment identifier.  Defaults to the ``APP_ENV``
        environment variable, falling back to ``"dev"``.

    Returns
    -------
    str
        A fully-qualified SQLAlchemy database URL.

    Raises
    ------
    EnvironmentError
        When no suitable URL is found in the environment.
    """
    env = (env or os.getenv("APP_ENV", _ENV_DEV)).lower()
    if env not in _VALID_ENVS:
        raise ValueError(
            f"Unknown APP_ENV {env!r}. Valid options: {sorted(_VALID_ENVS)}"
        )

    url = (
        os.getenv("DATABASE_URL")
        or os.getenv(f"DATABASE_URL_{env.upper()}")
    )
    if not url:
        raise EnvironmentError(
            f"No database URL found for env={env!r}. "
            f"Set DATABASE_URL or DATABASE_URL_{env.upper()} in your environment."
        )
    return url


# ---------------------------------------------------------------------------
# Declarative base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    """
    Shared declarative base for all ORM models.

    All models inherit from this class.  The ``metadata`` object attached
    here is what ``Base.metadata.create_all(engine)`` iterates over.
    """


# ---------------------------------------------------------------------------
# Timestamp mixin
# ---------------------------------------------------------------------------

class TimestampMixin:
    """
    Mixin that adds ``created_at`` and ``updated_at`` columns to any model.

    Both columns are populated automatically:
    - ``created_at`` is set once on INSERT via ``server_default``.
    - ``updated_at`` is set on INSERT and refreshed on every UPDATE via
      ``onupdate``.
    """

    created_at: Column = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="UTC timestamp of row creation.",
    )
    updated_at: Column = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        doc="UTC timestamp of last update.",
    )


# ---------------------------------------------------------------------------
# Enum types
# ---------------------------------------------------------------------------

ApiCallStatus = Enum(
    "SUCCESS",
    "TIMEOUT",
    "RATE_LIMIT",
    "ERROR",
    name="api_call_status",
    create_type=True,
)

AnomalyLevel = Enum(
    "NORMAL",
    "WARNING",
    "CRITICAL",
    name="anomaly_level",
    create_type=True,
)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Currency(TimestampMixin, Base):
    """
    Dimension table for currency reference data.

    Each row represents a single ISO 4217 currency (e.g. USD, IDR, EUR).
    Used as a foreign key target by :class:`ExchangeRate` and
    :class:`DailySnapshot`.

    Attributes
    ----------
    currency_id : int
        Auto-incrementing primary key.
    code : str
        ISO 4217 three-letter currency code (unique, e.g. ``"USD"``).
    name : str
        Human-readable currency name (e.g. ``"US Dollar"``).
    is_active : bool
        Whether this currency is currently tracked by the pipeline.
    """

    __tablename__ = "currencies"

    currency_id: Column = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        doc="Surrogate primary key.",
    )
    code: Column = Column(
        String(3),
        nullable=False,
        unique=True,
        index=True,
        doc="ISO 4217 currency code, e.g. 'USD'.",
    )
    name: Column = Column(
        String(255),
        nullable=False,
        doc="Full currency name, e.g. 'US Dollar'.",
    )
    is_active: Column = Column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
        doc="Set False to soft-delete without breaking FK references.",
    )

    # ------------------------------------------------------------------ #
    # Relationships
    # ------------------------------------------------------------------ #
    rates_as_base: relationship = relationship(
        "ExchangeRate",
        foreign_keys="ExchangeRate.from_currency_id",
        back_populates="from_currency",
        lazy="dynamic",
        doc="All ExchangeRate rows where this currency is the base.",
    )
    rates_as_quote: relationship = relationship(
        "ExchangeRate",
        foreign_keys="ExchangeRate.to_currency_id",
        back_populates="to_currency",
        lazy="dynamic",
        doc="All ExchangeRate rows where this currency is the quote.",
    )

    def __repr__(self) -> str:
        return f"<Currency id={self.currency_id} code={self.code!r}>"


class ApiSource(TimestampMixin, Base):
    """
    Configuration table for API source metadata.

    Each row describes one external data source (e.g. ``yfinance``, FRED).
    Rate-limit and retry-strategy metadata are stored here so the pipeline
    can adapt at runtime without code changes.

    Attributes
    ----------
    source_id : int
        Auto-incrementing primary key.
    source_name : str
        Canonical short name used throughout the codebase (unique).
    api_endpoint : str | None
        Base URL of the API, for documentation and monitoring purposes.
    retry_strategy : str | None
        Human-readable description of the retry strategy in use.
    rate_limit : int | None
        Maximum requests per hour as documented by the source.
    is_active : bool
        Controls whether the pipeline queries this source.
    """

    __tablename__ = "api_sources"

    source_id: Column = Column(
        Integer,
        primary_key=True,
        autoincrement=True,
        doc="Surrogate primary key.",
    )
    source_name: Column = Column(
        String(100),
        nullable=False,
        unique=True,
        index=True,
        doc="Canonical identifier, e.g. 'yfinance' or 'fred'.",
    )
    api_endpoint: Column = Column(
        String(500),
        nullable=True,
        doc="Base URL of the API.",
    )
    retry_strategy: Column = Column(
        String(100),
        nullable=True,
        doc="E.g. 'exponential_backoff_max3'.",
    )
    rate_limit: Column = Column(
        Integer,
        nullable=True,
        doc="Max requests per hour (NULL = unknown/unlimited).",
    )
    is_active: Column = Column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
        doc="Set False to disable this source without deleting FK-referenced rows.",
    )

    # ------------------------------------------------------------------ #
    # Relationships
    # ------------------------------------------------------------------ #
    exchange_rates: relationship = relationship(
        "ExchangeRate",
        back_populates="source",
        lazy="dynamic",
        doc="All ExchangeRate rows fetched from this source.",
    )
    api_calls: relationship = relationship(
        "ApiCall",
        back_populates="source",
        lazy="dynamic",
        doc="Full audit trail of API calls made to this source.",
    )

    def __repr__(self) -> str:
        return f"<ApiSource id={self.source_id} name={self.source_name!r}>"


class ExchangeRate(TimestampMixin, Base):
    """
    Fact table for raw exchange rate observations.

    Each row records a single rate between two currencies at a specific
    point in time, as returned by one API source.  The combination of
    ``(from_currency_id, to_currency_id, timestamp, source_id)`` must be
    unique to enable idempotent upserts.

    Attributes
    ----------
    rate_id : int
        Auto-incrementing BigInteger primary key.
    from_currency_id : int
        FK → :class:`Currency`.  The base currency (e.g. USD).
    to_currency_id : int
        FK → :class:`Currency`.  The quote currency (e.g. IDR).
    rate : Decimal
        Exchange rate value; must be positive.
    timestamp : datetime
        Market timestamp of the observation (stored as UTC).
    source_id : int
        FK → :class:`ApiSource`.
    data_quality_score : Decimal | None
        Composite quality score in [0.00, 1.00].
    is_valid : bool
        False if the record failed a quality check and should be excluded
        from analytics.
    """

    __tablename__ = "exchange_rates"

    rate_id: Column = Column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        doc="Surrogate BigInteger primary key.",
    )
    from_currency_id: Column = Column(
        Integer,
        ForeignKey("currencies.currency_id", ondelete="RESTRICT"),
        nullable=False,
        doc="Base currency FK (e.g. USD).",
    )
    to_currency_id: Column = Column(
        Integer,
        ForeignKey("currencies.currency_id", ondelete="RESTRICT"),
        nullable=False,
        doc="Quote currency FK (e.g. IDR).",
    )
    rate: Column = Column(
        Numeric(12, 6),
        nullable=False,
        doc="Exchange rate value; enforced > 0 by CHECK constraint.",
    )
    timestamp: Column = Column(
        DateTime(timezone=True),
        nullable=False,
        doc="Market timestamp of the observation (UTC).",
    )
    source_id: Column = Column(
        Integer,
        ForeignKey("api_sources.source_id", ondelete="RESTRICT"),
        nullable=False,
        doc="FK to the API source that provided this rate.",
    )
    data_quality_score: Column = Column(
        Numeric(3, 2),
        nullable=True,
        doc="Composite quality score in [0.00, 1.00].",
    )
    is_valid: Column = Column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
        doc="False if the record failed quality checks.",
    )

    # ------------------------------------------------------------------ #
    # Constraints
    # ------------------------------------------------------------------ #
    __table_args__ = (
        CheckConstraint("rate > 0", name="ck_exchange_rates_rate_positive"),
        CheckConstraint(
            "from_currency_id != to_currency_id",
            name="ck_exchange_rates_different_currencies",
        ),
        CheckConstraint(
            "data_quality_score IS NULL OR "
            "(data_quality_score >= 0.00 AND data_quality_score <= 1.00)",
            name="ck_exchange_rates_quality_score_range",
        ),
        UniqueConstraint(
            "from_currency_id",
            "to_currency_id",
            "timestamp",
            "source_id",
            name="uq_exchange_rates_pair_timestamp_source",
        ),
        # ---- Indexes (ADR-002) ----
        Index(
            "idx_exchange_rates_pair_timestamp",
            "from_currency_id",
            "to_currency_id",
            "timestamp",
        ),
        Index("idx_exchange_rates_timestamp", "timestamp"),
        Index("idx_exchange_rates_source_id", "source_id"),
        Index("idx_exchange_rates_is_valid", "is_valid"),
    )

    # ------------------------------------------------------------------ #
    # Relationships
    # ------------------------------------------------------------------ #
    from_currency: relationship = relationship(
        "Currency",
        foreign_keys=[from_currency_id],
        back_populates="rates_as_base",
        doc="Base currency object.",
    )
    to_currency: relationship = relationship(
        "Currency",
        foreign_keys=[to_currency_id],
        back_populates="rates_as_quote",
        doc="Quote currency object.",
    )
    source: relationship = relationship(
        "ApiSource",
        back_populates="exchange_rates",
        doc="API source that provided this rate.",
    )
    quality_metrics: relationship = relationship(
        "DataQualityMetric",
        back_populates="exchange_rate",
        cascade="all, delete-orphan",
        lazy="dynamic",
        doc="Quality check results associated with this rate.",
    )

    def __repr__(self) -> str:
        return (
            f"<ExchangeRate id={self.rate_id} "
            f"pair={self.from_currency_id}/{self.to_currency_id} "
            f"rate={self.rate} ts={self.timestamp}>"
        )


class ApiCall(Base):
    """
    Audit trail for every API call made by the pipeline.

    Rows are INSERT-only; never updated.  ``updated_at`` is omitted
    intentionally — use ``created_at`` as the record timestamp.

    Attributes
    ----------
    call_id : int
        Auto-incrementing BigInteger primary key.
    source_id : int
        FK → :class:`ApiSource`.
    timestamp : datetime
        When the API call was initiated (UTC).
    status : str
        One of ``SUCCESS``, ``TIMEOUT``, ``RATE_LIMIT``, ``ERROR``.
    error_message : str | None
        Full error details if ``status != 'SUCCESS'``.
    records_fetched : int | None
        Total records returned by the API.
    records_valid : int | None
        Records that passed validation.
    records_invalid : int | None
        Records that failed validation.
    execution_time_ms : int | None
        End-to-end call duration in milliseconds.
    """

    __tablename__ = "api_calls"

    call_id: Column = Column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        doc="Surrogate BigInteger primary key.",
    )
    source_id: Column = Column(
        Integer,
        ForeignKey("api_sources.source_id", ondelete="RESTRICT"),
        nullable=False,
        doc="FK to the source that was called.",
    )
    timestamp: Column = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        doc="UTC timestamp when the call was initiated.",
    )
    status: Column = Column(
        ApiCallStatus,
        nullable=False,
        doc="Outcome of the API call.",
    )
    error_message: Column = Column(
        Text,
        nullable=True,
        doc="Full error text (populated when status != 'SUCCESS').",
    )
    records_fetched: Column = Column(
        Integer,
        nullable=True,
        doc="Total records returned by the API response.",
    )
    records_valid: Column = Column(
        Integer,
        nullable=True,
        doc="Records that passed all validation checks.",
    )
    records_invalid: Column = Column(
        Integer,
        nullable=True,
        doc="Records that failed one or more validation checks.",
    )
    execution_time_ms: Column = Column(
        Integer,
        nullable=True,
        doc="End-to-end duration of the API call in milliseconds.",
    )
    created_at: Column = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="UTC timestamp of row creation (insert-only table).",
    )

    # ------------------------------------------------------------------ #
    # Constraints & indexes
    # ------------------------------------------------------------------ #
    __table_args__ = (
        Index("idx_api_calls_source_timestamp", "source_id", "timestamp"),
        Index("idx_api_calls_status", "status"),
    )

    # ------------------------------------------------------------------ #
    # Relationships
    # ------------------------------------------------------------------ #
    source: relationship = relationship(
        "ApiSource",
        back_populates="api_calls",
        doc="Source that was called.",
    )

    def __repr__(self) -> str:
        return (
            f"<ApiCall id={self.call_id} "
            f"source_id={self.source_id} status={self.status!r}>"
        )


class DataQualityMetric(Base):
    """
    Per-record quality check results for :class:`ExchangeRate` rows.

    Each row records whether a specific named check passed or failed for
    a given ``rate_id``.  Multiple checks can exist per rate.

    Attributes
    ----------
    metric_id : int
        Auto-incrementing BigInteger primary key.
    rate_id : int
        FK → :class:`ExchangeRate`.
    check_name : str
        Name of the quality check (e.g. ``NULL_CHECK``, ``RANGE_CHECK``).
    check_passed : bool
        Whether the check passed.
    anomaly_score : Decimal | None
        Anomaly severity score in [0.00, 1.00]; populated by anomaly checks.
    """

    __tablename__ = "data_quality_metrics"

    metric_id: Column = Column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        doc="Surrogate BigInteger primary key.",
    )
    rate_id: Column = Column(
        BigInteger,
        ForeignKey("exchange_rates.rate_id", ondelete="CASCADE"),
        nullable=False,
        doc="FK to the exchange rate this metric belongs to.",
    )
    check_name: Column = Column(
        String(100),
        nullable=False,
        doc="Quality check identifier, e.g. 'NULL_CHECK', 'RANGE_CHECK', 'ANOMALY_CHECK'.",
    )
    check_passed: Column = Column(
        Boolean,
        nullable=False,
        doc="True if the check passed.",
    )
    anomaly_score: Column = Column(
        Numeric(3, 2),
        nullable=True,
        doc="Anomaly severity in [0.00, 1.00]; NULL for non-anomaly checks.",
    )
    created_at: Column = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="UTC timestamp of row creation.",
    )

    # ------------------------------------------------------------------ #
    # Constraints & indexes
    # ------------------------------------------------------------------ #
    __table_args__ = (
        CheckConstraint(
            "anomaly_score IS NULL OR "
            "(anomaly_score >= 0.00 AND anomaly_score <= 1.00)",
            name="ck_data_quality_anomaly_score_range",
        ),
        Index(
            "idx_data_quality_rate_check",
            "rate_id",
            "check_name",
        ),
        Index("idx_data_quality_check_passed", "check_passed"),
    )

    # ------------------------------------------------------------------ #
    # Relationships
    # ------------------------------------------------------------------ #
    exchange_rate: relationship = relationship(
        "ExchangeRate",
        back_populates="quality_metrics",
        doc="The exchange rate record this metric evaluates.",
    )

    def __repr__(self) -> str:
        return (
            f"<DataQualityMetric id={self.metric_id} "
            f"rate_id={self.rate_id} check={self.check_name!r} "
            f"passed={self.check_passed}>"
        )


class DailySnapshot(TimestampMixin, Base):
    """
    Pre-aggregated OHLCV daily exchange rate data.

    Populated by the dbt ``marts`` layer or a scheduled aggregation job.
    Provides fast access to daily summaries without scanning ``exchange_rates``.

    Attributes
    ----------
    snapshot_id : int
        Auto-incrementing BigInteger primary key.
    snapshot_date : date
        The calendar date this snapshot covers.
    from_currency_id : int
        FK → :class:`Currency` (base).
    to_currency_id : int
        FK → :class:`Currency` (quote).
    rate_open / rate_high / rate_low / rate_close : Decimal
        OHLC values for the day.
    rate_avg : Decimal
        Volume-weighted average rate for the day.
    rate_ma7 : Decimal | None
        7-day simple moving average (NULL for first 6 days of data).
    pct_change : Decimal | None
        Percentage change vs. previous trading day's close.
    is_anomaly : bool
        True if the daily movement triggered an anomaly alert.
    anomaly_level : str
        One of ``NORMAL``, ``WARNING``, ``CRITICAL``.
    """

    __tablename__ = "daily_snapshots"

    snapshot_id: Column = Column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        doc="Surrogate BigInteger primary key.",
    )
    snapshot_date: Column = Column(
        Date,
        nullable=False,
        doc="Calendar date covered by this snapshot.",
    )
    from_currency_id: Column = Column(
        Integer,
        ForeignKey("currencies.currency_id", ondelete="RESTRICT"),
        nullable=False,
        doc="Base currency FK.",
    )
    to_currency_id: Column = Column(
        Integer,
        ForeignKey("currencies.currency_id", ondelete="RESTRICT"),
        nullable=False,
        doc="Quote currency FK.",
    )
    rate_open: Column = Column(
        Numeric(12, 6),
        nullable=False,
        doc="First rate observed on snapshot_date.",
    )
    rate_high: Column = Column(
        Numeric(12, 6),
        nullable=False,
        doc="Highest rate observed on snapshot_date.",
    )
    rate_low: Column = Column(
        Numeric(12, 6),
        nullable=False,
        doc="Lowest rate observed on snapshot_date.",
    )
    rate_close: Column = Column(
        Numeric(12, 6),
        nullable=False,
        doc="Last rate observed on snapshot_date.",
    )
    rate_avg: Column = Column(
        Numeric(12, 6),
        nullable=False,
        doc="Average rate across all observations on snapshot_date.",
    )
    rate_ma7: Column = Column(
        Numeric(12, 6),
        nullable=True,
        doc="7-day simple moving average (NULL when fewer than 7 days of history exist).",
    )
    pct_change: Column = Column(
        Numeric(6, 3),
        nullable=True,
        doc="Percentage change vs. previous trading day's close.",
    )
    is_anomaly: Column = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
        doc="True if the daily movement triggered an anomaly alert.",
    )
    anomaly_level: Column = Column(
        AnomalyLevel,
        nullable=False,
        default="NORMAL",
        server_default=text("'NORMAL'"),
        doc="Severity classification: NORMAL | WARNING | CRITICAL.",
    )

    # ------------------------------------------------------------------ #
    # Constraints & indexes
    # ------------------------------------------------------------------ #
    __table_args__ = (
        CheckConstraint(
            "rate_high >= rate_low",
            name="ck_daily_snapshots_high_gte_low",
        ),
        CheckConstraint(
            "rate_open > 0 AND rate_high > 0 AND rate_low > 0 AND rate_close > 0",
            name="ck_daily_snapshots_rates_positive",
        ),
        UniqueConstraint(
            "snapshot_date",
            "from_currency_id",
            "to_currency_id",
            name="uq_daily_snapshots_date_pair",
        ),
        Index(
            "idx_daily_snapshots_date_pair",
            "snapshot_date",
            "from_currency_id",
            "to_currency_id",
        ),
        Index("idx_daily_snapshots_is_anomaly", "is_anomaly"),
    )

    def __repr__(self) -> str:
        return (
            f"<DailySnapshot id={self.snapshot_id} "
            f"date={self.snapshot_date} "
            f"pair={self.from_currency_id}/{self.to_currency_id}>"
        )


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------

def get_engine(
    database_url: Optional[str] = None,
    env: Optional[str] = None,
) -> Engine:
    """
    Create and return a SQLAlchemy :class:`Engine` configured for the
    target environment.

    Parameters
    ----------
    database_url:
        Explicit connection URL.  When omitted, resolved via
        :func:`_get_database_url`.
    env:
        Runtime environment (``"dev"``, ``"staging"``, ``"prod"``).
        Defaults to ``APP_ENV`` environment variable or ``"dev"``.

    Returns
    -------
    Engine
        A fully configured SQLAlchemy engine with connection pooling.

    Raises
    ------
    EnvironmentError
        If the database URL cannot be resolved.
    OperationalError
        If the engine cannot reach the database (surfaced on first use).

    Example
    -------
    >>> engine = get_engine()
    >>> Base.metadata.create_all(engine)
    """
    env = (env or os.getenv("APP_ENV", _ENV_DEV)).lower()
    url = database_url or _get_database_url(env)
    pool_cfg = _POOL_CONFIG.get(env, _POOL_CONFIG[_ENV_DEV])

    echo = pool_cfg.pop("echo", False)

    logger.info("Creating engine for env=%r (echo=%s)", env, echo)

    # SQLite (used in unit tests) doesn't support QueuePool properly
    if url.startswith("sqlite"):
        engine = create_engine(url, echo=echo, poolclass=NullPool)
    else:
        engine = create_engine(
            url,
            echo=echo,
            poolclass=QueuePool,
            **pool_cfg,
        )

    # Re-insert echo so the dict is unchanged for repeated calls
    pool_cfg["echo"] = echo

    # Register a connect event to enforce UTC on every new connection
    @event.listens_for(engine, "connect")
    def _set_timezone(dbapi_conn, _connection_record) -> None:  # type: ignore[type-arg]
        try:
            cursor = dbapi_conn.cursor()
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.close()
        except Exception:
            # SQLite and some drivers don't support SET TIME ZONE — that's OK
            pass

    logger.info("Engine created: %s", engine.url.render_as_string(hide_password=True))
    return engine


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    """
    Return a :class:`sessionmaker` bound to the given engine.

    Parameters
    ----------
    engine:
        A SQLAlchemy engine (typically from :func:`get_engine`).

    Returns
    -------
    sessionmaker
        A callable that produces :class:`Session` instances.
    """
    return sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,   # avoids lazy-load after commit in pipelines
    )


@contextmanager
def get_session(engine: Engine) -> Generator[Session, None, None]:
    """
    Context manager that yields a database session and handles
    commit/rollback/close automatically.

    Parameters
    ----------
    engine:
        A SQLAlchemy engine (typically from :func:`get_engine`).

    Yields
    ------
    Session
        An active SQLAlchemy session.

    Raises
    ------
    SQLAlchemyError
        Any database error is logged, the transaction rolled back,
        and the exception re-raised so the caller can handle it.

    Example
    -------
    >>> engine = get_engine()
    >>> with get_session(engine) as session:
    ...     session.add(Currency(code="USD", name="US Dollar"))
    ...     # commit happens automatically on context exit
    """
    factory = get_session_factory(engine)
    session: Session = factory()
    try:
        yield session
        session.commit()
        logger.debug("Session committed successfully.")
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error("Session rolled back due to database error: %s", exc)
        raise
    except Exception as exc:
        session.rollback()
        logger.error("Session rolled back due to unexpected error: %s", exc)
        raise
    finally:
        session.close()
        logger.debug("Session closed.")


# ---------------------------------------------------------------------------
# Schema utilities
# ---------------------------------------------------------------------------

def create_all_tables(engine: Engine) -> None:
    """
    Create all tables defined in :attr:`Base.metadata` if they do not exist.

    Safe to call repeatedly (uses ``checkfirst=True`` internally via
    SQLAlchemy's ``CREATE TABLE IF NOT EXISTS`` semantics).

    Parameters
    ----------
    engine:
        Target database engine.
    """
    logger.info("Creating schema on: %s", engine.url.render_as_string(hide_password=True))
    Base.metadata.create_all(engine)
    logger.info("Schema creation complete.")


def drop_all_tables(engine: Engine) -> None:
    """
    Drop all tables managed by :attr:`Base.metadata`.

    .. warning::
        **Destructive operation.**  Never call in production.
        Guarded by an ``APP_ENV`` check.

    Parameters
    ----------
    engine:
        Target database engine.

    Raises
    ------
    RuntimeError
        If called when ``APP_ENV`` is ``"prod"``.
    """
    env = os.getenv("APP_ENV", _ENV_DEV).lower()
    if env == _ENV_PROD:
        raise RuntimeError(
            "drop_all_tables() must never be called in production. "
            "Use Alembic downgrade migrations instead."
        )
    logger.warning("Dropping all tables (env=%r). This is irreversible.", env)
    Base.metadata.drop_all(engine)
    logger.warning("All tables dropped.")


def check_connection(engine: Engine) -> bool:
    """
    Verify that the engine can reach the database.

    Parameters
    ----------
    engine:
        The engine to probe.

    Returns
    -------
    bool
        ``True`` if the connection is healthy, ``False`` otherwise.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection OK.")
        return True
    except OperationalError as exc:
        logger.error("Database connection check failed: %s", exc)
        return False