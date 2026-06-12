"""
src/utils/database.py
======================
SQLAlchemy 2.0 ORM models for the Rupiah Exchange Rate Intelligence platform.

Schema Reference : docs/SCHEMA.md  (ADR-002)
SQLAlchemy       : 2.0+ (Mapped / mapped_column API, 2.0-style sessions)

Tables
------
- currencies           Dimension  – ISO 4217 currency reference data
- api_sources          Config     – External API source registry
- exchange_rates       Fact       – Raw rate observations
- api_calls            Audit      – Immutable API-call log
- data_quality_metrics Tracking   – Per-record quality-check results
- daily_snapshots      Aggregated – Pre-computed OHLCV daily data

Key SQLAlchemy 2.0 patterns used
---------------------------------
- ``DeclarativeBase``            instead of ``declarative_base()``
- ``Mapped[T]`` / ``mapped_column()`` instead of ``Column()`` assignments
- ``Optional[T]``                to mark nullable columns
- ``relationship()`` with ``Mapped[list[...]]`` / ``Mapped[...]`` annotations
- ``Session.execute(select(...))`` instead of ``Session.query(...)``
- ``with Session(engine) as session``  (2.0-style context manager)
- ``AsyncSession`` -ready structure (sync engine shown; swap easily)

Usage
-----
    from src.utils.database import get_engine, get_session, Base
    from src.utils.database import Currency, ExchangeRate
    from sqlalchemy import select

    engine = get_engine()
    Base.metadata.create_all(engine)

    with get_session(engine) as session:
        stmt = select(Currency).where(Currency.is_active == True)
        currencies = session.scalars(stmt).all()
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Generator, List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
    func,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedColumn,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.pool import QueuePool, StaticPool

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Python-side Enum definitions (used for type hints and validation)
# ---------------------------------------------------------------------------

class ApiCallStatusEnum(str, PyEnum):
    """Valid status values for :class:`ApiCall.status`."""
    SUCCESS    = "SUCCESS"
    TIMEOUT    = "TIMEOUT"
    RATE_LIMIT = "RATE_LIMIT"
    ERROR      = "ERROR"


class AnomalyLevelEnum(str, PyEnum):
    """Valid anomaly severity levels for :class:`DailySnapshot.anomaly_level`."""
    NORMAL   = "NORMAL"
    WARNING  = "WARNING"
    CRITICAL = "CRITICAL"


# ---------------------------------------------------------------------------
# Environment / connection-pool config
# ---------------------------------------------------------------------------

_ENV_DEV     = "dev"
_ENV_STAGING = "staging"
_ENV_PROD    = "prod"
_VALID_ENVS  = {_ENV_DEV, _ENV_STAGING, _ENV_PROD}

_POOL_CONFIG: dict[str, dict] = {
    _ENV_DEV: {
        "pool_size": 2, "max_overflow": 3,
        "pool_timeout": 30, "pool_recycle": 1800,
        "pool_pre_ping": True, "echo": True,
    },
    _ENV_STAGING: {
        "pool_size": 5, "max_overflow": 10,
        "pool_timeout": 30, "pool_recycle": 1800,
        "pool_pre_ping": True, "echo": False,
    },
    _ENV_PROD: {
        "pool_size": 10, "max_overflow": 20,
        "pool_timeout": 30, "pool_recycle": 900,
        "pool_pre_ping": True, "echo": False,
    },
}


def _get_database_url(env: Optional[str] = None) -> str:
    """
    Resolve the database connection URL from environment variables.

    Lookup order
    ------------
    1. ``DATABASE_URL``          – generic override (all envs)
    2. ``DATABASE_URL_<ENV>``    – env-specific, e.g. ``DATABASE_URL_PROD``
    3. Raises :exc:`EnvironmentError` if neither is set.

    Parameters
    ----------
    env:
        Runtime environment identifier.  Defaults to the ``APP_ENV``
        environment variable, falling back to ``"dev"``.

    Raises
    ------
    ValueError
        If *env* is not one of ``dev``, ``staging``, ``prod``.
    EnvironmentError
        If no URL is found in the environment.
    """
    resolved = (env or os.getenv("APP_ENV", _ENV_DEV)).lower()
    if resolved not in _VALID_ENVS:
        raise ValueError(
            f"Unknown APP_ENV {resolved!r}. Valid options: {sorted(_VALID_ENVS)}"
        )
    url = os.getenv("DATABASE_URL") or os.getenv(f"DATABASE_URL_{resolved.upper()}")
    if not url:
        raise EnvironmentError(
            f"No database URL found for env={resolved!r}. "
            f"Set DATABASE_URL or DATABASE_URL_{resolved.upper()} in your environment."
        )
    return url


# ---------------------------------------------------------------------------
# Declarative base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    """
    Shared declarative base for all ORM models (SQLAlchemy 2.0).

    All models inherit from this class.  ``Base.metadata`` is the single
    source of truth for schema creation and Alembic autogenerate.
    """


# ---------------------------------------------------------------------------
# Timestamp mixin  (2.0-style: MappedColumn annotations)
# ---------------------------------------------------------------------------

class TimestampMixin:
    """
    Adds ``created_at`` and ``updated_at`` to any model.

    - ``created_at`` is set once on INSERT via ``server_default``.
    - ``updated_at`` is refreshed on every UPDATE via ``onupdate``.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="UTC timestamp of row creation.",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        doc="UTC timestamp of last update.",
    )


# ===========================================================================
# Models
# ===========================================================================

class Currency(TimestampMixin, Base):
    """
    Dimension table for ISO 4217 currency reference data.

    Used as a foreign-key target by :class:`ExchangeRate` and
    :class:`DailySnapshot`.

    Attributes
    ----------
    currency_id : int
        Auto-incrementing surrogate primary key.
    code : str
        ISO 4217 three-letter code (``"USD"``, ``"IDR"`` …). Unique.
    name : str
        Human-readable name (``"US Dollar"``).
    is_active : bool
        Soft-delete flag; ``False`` retires the currency without breaking FK
        references.
    """

    __tablename__ = "currencies"

    # ---- Columns ----
    currency_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        doc="Surrogate primary key.",
    )
    code: Mapped[str] = mapped_column(
        String(3),
        nullable=False,
        unique=True,
        index=True,
        doc="ISO 4217 code, e.g. 'USD'.",
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        doc="Full currency name.",
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
        doc="False = soft-deleted; row is kept for FK integrity.",
    )

    # ---- Relationships ----
    rates_as_base: Mapped[List["ExchangeRate"]] = relationship(
        "ExchangeRate",
        foreign_keys="ExchangeRate.from_currency_id",
        back_populates="from_currency",
        lazy="select",
        doc="ExchangeRate rows where this currency is the base.",
    )
    rates_as_quote: Mapped[List["ExchangeRate"]] = relationship(
        "ExchangeRate",
        foreign_keys="ExchangeRate.to_currency_id",
        back_populates="to_currency",
        lazy="select",
        doc="ExchangeRate rows where this currency is the quote.",
    )

    def __repr__(self) -> str:
        return f"<Currency id={self.currency_id} code={self.code!r}>"


class ApiSource(TimestampMixin, Base):
    """
    Configuration table for external API source metadata.

    Each row describes one provider (``yfinance``, ``fred``, …).  Pipeline
    behaviour (retry strategy, rate limit) is stored here so it can be tuned
    at runtime without code changes.

    Attributes
    ----------
    source_id : int
        Auto-incrementing surrogate primary key.
    source_name : str
        Canonical short name used in code and config.  Unique.
    api_endpoint : str | None
        Base URL of the external API (documentation/monitoring use).
    retry_strategy : str | None
        Human-readable retry description, e.g. ``"exponential_backoff_max3"``.
    rate_limit : int | None
        Max requests per hour; ``None`` = unknown / unlimited.
    is_active : bool
        ``False`` disables the source without deleting FK-referenced rows.
    """

    __tablename__ = "api_sources"

    # ---- Columns ----
    source_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        autoincrement=True,
        doc="Surrogate primary key.",
    )
    source_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        unique=True,
        index=True,
        doc="Canonical identifier, e.g. 'yfinance' or 'fred'.",
    )
    api_endpoint: Mapped[Optional[str]] = mapped_column(
        String(500),
        nullable=True,
        doc="Base URL of the external API.",
    )
    retry_strategy: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        doc="Human-readable retry strategy description.",
    )
    rate_limit: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        doc="Max requests per hour (None = unknown/unlimited).",
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
        doc="False = source disabled; rows kept for FK integrity.",
    )

    # ---- Relationships ----
    exchange_rates: Mapped[List["ExchangeRate"]] = relationship(
        "ExchangeRate",
        back_populates="source",
        lazy="select",
        doc="ExchangeRate rows fetched from this source.",
    )
    api_calls: Mapped[List["ApiCall"]] = relationship(
        "ApiCall",
        back_populates="source",
        lazy="select",
        doc="Full audit trail of API calls to this source.",
    )

    def __repr__(self) -> str:
        return f"<ApiSource id={self.source_id} name={self.source_name!r}>"


class ExchangeRate(TimestampMixin, Base):
    """
    Fact table for raw exchange rate observations.

    Each row is a single rate between two currencies at one point in time
    from one API source.  The 4-tuple
    ``(from_currency_id, to_currency_id, timestamp, source_id)`` is unique
    to enable idempotent upserts.

    Attributes
    ----------
    rate_id : int
        Auto-incrementing BigInteger primary key.
    from_currency_id : int
        FK → :class:`Currency` (base currency, e.g. USD).
    to_currency_id : int
        FK → :class:`Currency` (quote currency, e.g. IDR).
    source_id : int
        FK → :class:`ApiSource`.
    rate : Decimal
        Exchange rate value; must be > 0.
    timestamp : datetime
        Market timestamp of the observation (UTC).
    data_quality_score : Decimal | None
        Composite quality score in ``[0.00, 1.00]``.
    is_valid : bool
        ``False`` if the record failed a quality check.
    """

    __tablename__ = "exchange_rates"

    # ---- Columns ----
    rate_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
        doc="Surrogate BigInteger primary key (INTEGER on SQLite for rowid autoincrement).",
    )
    from_currency_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("currencies.currency_id", ondelete="RESTRICT"),
        nullable=False,
        doc="Base currency FK (e.g. USD).",
    )
    to_currency_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("currencies.currency_id", ondelete="RESTRICT"),
        nullable=False,
        doc="Quote currency FK (e.g. IDR).",
    )
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("api_sources.source_id", ondelete="RESTRICT"),
        nullable=False,
        doc="FK to the API source that provided this rate.",
    )
    rate: Mapped[Decimal] = mapped_column(
        Numeric(12, 6),
        nullable=False,
        doc="Exchange rate; enforced > 0 by CHECK constraint.",
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        doc="Market timestamp of the observation (UTC).",
    )
    data_quality_score: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(3, 2),
        nullable=True,
        doc="Composite quality score 0.00 – 1.00.",
    )
    is_valid: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        server_default=text("true"),
        doc="False = failed quality checks; excluded from analytics.",
    )

    # ---- Table-level constraints & indexes ----
    __table_args__ = (
        CheckConstraint("rate > 0",
                        name="ck_exchange_rates_rate_positive"),
        CheckConstraint("from_currency_id != to_currency_id",
                        name="ck_exchange_rates_different_currencies"),
        CheckConstraint(
            "data_quality_score IS NULL OR "
            "(data_quality_score >= 0.00 AND data_quality_score <= 1.00)",
            name="ck_exchange_rates_quality_score_range",
        ),
        UniqueConstraint(
            "from_currency_id", "to_currency_id", "timestamp", "source_id",
            name="uq_exchange_rates_pair_timestamp_source",
        ),
        Index("idx_exchange_rates_pair_timestamp",
              "from_currency_id", "to_currency_id", "timestamp"),
        Index("idx_exchange_rates_timestamp",   "timestamp"),
        Index("idx_exchange_rates_source_id",   "source_id"),
        Index("idx_exchange_rates_is_valid",    "is_valid"),
    )

    # ---- Relationships ----
    from_currency: Mapped["Currency"] = relationship(
        "Currency",
        foreign_keys=[from_currency_id],
        back_populates="rates_as_base",
        doc="Base currency object.",
    )
    to_currency: Mapped["Currency"] = relationship(
        "Currency",
        foreign_keys=[to_currency_id],
        back_populates="rates_as_quote",
        doc="Quote currency object.",
    )
    source: Mapped["ApiSource"] = relationship(
        "ApiSource",
        back_populates="exchange_rates",
        doc="API source that provided this rate.",
    )
    quality_metrics: Mapped[List["DataQualityMetric"]] = relationship(
        "DataQualityMetric",
        back_populates="exchange_rate",
        cascade="all, delete-orphan",
        lazy="select",
        doc="Quality-check results for this rate.",
    )

    def __repr__(self) -> str:
        return (
            f"<ExchangeRate id={self.rate_id} "
            f"pair={self.from_currency_id}/{self.to_currency_id} "
            f"rate={self.rate} ts={self.timestamp}>"
        )


class ApiCall(Base):
    """
    Immutable audit trail for every API call made by the pipeline.

    Rows are INSERT-only; ``updated_at`` is intentionally absent.

    Attributes
    ----------
    call_id : int
        Auto-incrementing BigInteger primary key.
    source_id : int
        FK → :class:`ApiSource`.
    timestamp : datetime
        UTC time the call was initiated.
    status : str
        One of ``SUCCESS``, ``TIMEOUT``, ``RATE_LIMIT``, ``ERROR``.
    error_message : str | None
        Full error text when ``status != 'SUCCESS'``.
    records_fetched / records_valid / records_invalid : int | None
        Record counts from the API response.
    execution_time_ms : int | None
        End-to-end call duration in milliseconds.
    """

    __tablename__ = "api_calls"

    # ---- Columns ----
    call_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
        doc="Surrogate BigInteger primary key (INTEGER on SQLite for rowid autoincrement).",
    )
    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("api_sources.source_id", ondelete="RESTRICT"),
        nullable=False,
        doc="FK to the source that was called.",
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        doc="UTC timestamp when the call was initiated.",
    )
    status: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        doc="Call outcome: SUCCESS | TIMEOUT | RATE_LIMIT | ERROR.",
    )
    error_message: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        doc="Full error text (populated when status != 'SUCCESS').",
    )
    records_fetched: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        doc="Total records returned by the API.",
    )
    records_valid: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        doc="Records that passed all validation checks.",
    )
    records_invalid: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        doc="Records that failed one or more validation checks.",
    )
    execution_time_ms: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        doc="End-to-end API call duration in milliseconds.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        doc="UTC row-creation timestamp (insert-only table).",
    )

    # ---- Table-level constraints & indexes ----
    __table_args__ = (
        Index("idx_api_calls_source_timestamp", "source_id", "timestamp"),
        Index("idx_api_calls_status",           "status"),
    )

    # ---- Relationships ----
    source: Mapped["ApiSource"] = relationship(
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

    Multiple rows can exist per ``rate_id`` (one per check name).
    Deleted automatically when the parent ``ExchangeRate`` is deleted
    (``ondelete=CASCADE``).

    Attributes
    ----------
    metric_id : int
        Auto-incrementing BigInteger primary key.
    rate_id : int
        FK → :class:`ExchangeRate`.
    check_name : str
        Check identifier: ``NULL_CHECK`` | ``RANGE_CHECK`` | ``ANOMALY_CHECK``.
    check_passed : bool
        ``True`` if the check passed.
    anomaly_score : Decimal | None
        Anomaly severity in ``[0.00, 1.00]``; ``None`` for non-anomaly checks.
    """

    __tablename__ = "data_quality_metrics"

    # ---- Columns ----
    metric_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
        doc="Surrogate BigInteger primary key (INTEGER on SQLite for rowid autoincrement).",
    )
    rate_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        ForeignKey("exchange_rates.rate_id", ondelete="CASCADE"),
        nullable=False,
        doc="FK to the exchange rate this metric evaluates.",
    )
    check_name: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        doc="Quality check identifier: NULL_CHECK | RANGE_CHECK | ANOMALY_CHECK.",
    )
    check_passed: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        doc="True if the check passed.",
    )
    anomaly_score: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(3, 2),
        nullable=True,
        doc="Anomaly severity 0.00 – 1.00; None for non-anomaly checks.",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        doc="UTC row-creation timestamp.",
    )

    # ---- Table-level constraints & indexes ----
    __table_args__ = (
        CheckConstraint(
            "anomaly_score IS NULL OR "
            "(anomaly_score >= 0.00 AND anomaly_score <= 1.00)",
            name="ck_data_quality_anomaly_score_range",
        ),
        Index("idx_data_quality_rate_check",   "rate_id", "check_name"),
        Index("idx_data_quality_check_passed", "check_passed"),
    )

    # ---- Relationships ----
    exchange_rate: Mapped["ExchangeRate"] = relationship(
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
    Provides fast access to daily summaries without scanning
    ``exchange_rates``.

    Attributes
    ----------
    snapshot_id : int
        Auto-incrementing BigInteger primary key.
    snapshot_date : date
        The calendar date this snapshot covers.
    from_currency_id / to_currency_id : int
        FK → :class:`Currency` (base / quote).
    rate_open / rate_high / rate_low / rate_close : Decimal
        OHLC values for the day.
    rate_avg : Decimal
        Average rate across all observations.
    rate_ma7 : Decimal | None
        7-day simple moving average (``None`` for the first 6 days).
    pct_change : Decimal | None
        Percentage change vs. previous trading day's close.
    is_anomaly : bool
        ``True`` if the daily movement triggered an anomaly alert.
    anomaly_level : str
        ``NORMAL`` | ``WARNING`` | ``CRITICAL``.
    """

    __tablename__ = "daily_snapshots"

    # ---- Columns ----
    snapshot_id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
        doc="Surrogate BigInteger primary key (INTEGER on SQLite for rowid autoincrement).",
    )
    snapshot_date: Mapped[date] = mapped_column(
        Date,
        nullable=False,
        doc="Calendar date covered by this snapshot.",
    )
    from_currency_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("currencies.currency_id", ondelete="RESTRICT"),
        nullable=False,
        doc="Base currency FK.",
    )
    to_currency_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("currencies.currency_id", ondelete="RESTRICT"),
        nullable=False,
        doc="Quote currency FK.",
    )

    # ---- OHLCV ----
    rate_open:  Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False,
                                                doc="Opening rate for the day.")
    rate_high:  Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False,
                                                doc="Highest rate for the day.")
    rate_low:   Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False,
                                                doc="Lowest rate for the day.")
    rate_close: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False,
                                                doc="Closing rate for the day.")
    rate_avg:   Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False,
                                                doc="Average rate for the day.")
    rate_ma7:   Mapped[Optional[Decimal]] = mapped_column(
        Numeric(12, 6), nullable=True,
        doc="7-day simple moving average (None for first 6 days).",
    )

    # ---- Derived metrics ----
    pct_change: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(6, 3),
        nullable=True,
        doc="% change vs. previous trading day close.",
    )

    # ---- Anomaly fields ----
    is_anomaly: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
        doc="True if the daily movement triggered an anomaly alert.",
    )
    anomaly_level: Mapped[str] = mapped_column(
        String(50),
        nullable=False,
        default=AnomalyLevelEnum.NORMAL.value,
        server_default=text("'NORMAL'"),
        doc="Severity: NORMAL | WARNING | CRITICAL.",
    )

    # ---- Table-level constraints & indexes ----
    __table_args__ = (
        CheckConstraint("rate_high >= rate_low",
                        name="ck_daily_snapshots_high_gte_low"),
        CheckConstraint(
            "rate_open > 0 AND rate_high > 0 AND rate_low > 0 AND rate_close > 0",
            name="ck_daily_snapshots_rates_positive",
        ),
        UniqueConstraint(
            "snapshot_date", "from_currency_id", "to_currency_id",
            name="uq_daily_snapshots_date_pair",
        ),
        Index("idx_daily_snapshots_date_pair",
              "snapshot_date", "from_currency_id", "to_currency_id"),
        Index("idx_daily_snapshots_is_anomaly", "is_anomaly"),
    )

    def __repr__(self) -> str:
        return (
            f"<DailySnapshot id={self.snapshot_id} "
            f"date={self.snapshot_date} "
            f"pair={self.from_currency_id}/{self.to_currency_id}>"
        )


# ===========================================================================
# Engine factory
# ===========================================================================

def get_engine(
    database_url: Optional[str] = None,
    env: Optional[str] = None,
) -> Engine:
    """
    Create a SQLAlchemy 2.0 :class:`Engine` configured for the target env.

    Parameters
    ----------
    database_url:
        Explicit connection URL.  When omitted, resolved via
        :func:`_get_database_url`.
    env:
        Runtime environment (``"dev"``, ``"staging"``, ``"prod"``).
        Defaults to the ``APP_ENV`` env var or ``"dev"``.

    Returns
    -------
    Engine

    Raises
    ------
    EnvironmentError
        If the database URL cannot be resolved.

    Example
    -------
    >>> engine = get_engine()
    >>> Base.metadata.create_all(engine)
    """
    resolved_env = (env or os.getenv("APP_ENV", _ENV_DEV)).lower()
    url = database_url or _get_database_url(resolved_env)

    # Deep-copy pool config so pop() doesn't mutate the shared dict
    pool_cfg = dict(_POOL_CONFIG.get(resolved_env, _POOL_CONFIG[_ENV_DEV]))
    echo = pool_cfg.pop("echo", False)

    logger.info("Creating engine env=%r echo=%s url=%s",
                resolved_env, echo,
                url.split("@")[-1] if "@" in url else url)

    if url.startswith("sqlite"):
        # In-memory SQLite (":memory:" / "sqlite://") creates a *new*,
        # separate database for every connection unless all connections
        # share a single underlying connection via StaticPool.
        # File-based SQLite doesn't have this problem but StaticPool is
        # harmless there too for test/dev usage.
        engine = create_engine(
            url,
            echo=echo,
            poolclass=StaticPool,
            connect_args={"check_same_thread": False},
        )
    else:
        engine = create_engine(url, echo=echo, poolclass=QueuePool, **pool_cfg)

    # Enforce UTC on every new PostgreSQL connection
    @event.listens_for(engine, "connect")
    def _set_utc(dbapi_conn, _record) -> None:  # type: ignore[type-arg]
        try:
            cur = dbapi_conn.cursor()
            cur.execute("SET TIME ZONE 'UTC'")
            cur.close()
        except Exception:
            pass  # SQLite and other drivers silently skip

    # SQLite does not enforce FOREIGN KEY constraints unless explicitly
    # enabled per-connection via PRAGMA. Without this, ondelete=RESTRICT/
    # CASCADE and FK existence checks are silently ignored.
    if url.startswith("sqlite"):
        @event.listens_for(engine, "connect")
        def _enable_sqlite_fk(dbapi_conn, _record) -> None:  # type: ignore[type-arg]
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

    return engine


# ===========================================================================
# Session utilities  (SQLAlchemy 2.0 style)
# ===========================================================================

def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    """
    Return a :class:`sessionmaker` bound to *engine*.

    ``expire_on_commit=False`` prevents lazy-load surprises after a commit
    in pipeline code that continues to use detached instances.

    Parameters
    ----------
    engine:
        A configured SQLAlchemy engine.
    """
    return sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )


@contextmanager
def get_session(engine: Engine) -> Generator[Session, None, None]:
    """
    Context manager that yields a 2.0-style database session.

    Commits automatically on clean exit; rolls back and re-raises on any
    exception; always closes the session in the ``finally`` block.

    Parameters
    ----------
    engine:
        A configured SQLAlchemy engine.

    Yields
    ------
    Session

    Raises
    ------
    SQLAlchemyError
        Re-raised after rollback on database errors.
    Exception
        Re-raised after rollback on all other errors.

    Example
    -------
    >>> from sqlalchemy import select
    >>> engine = get_engine()
    >>> with get_session(engine) as session:
    ...     result = session.scalars(select(Currency)).all()
    """
    factory = get_session_factory(engine)
    session: Session = factory()
    try:
        yield session
        session.commit()
        logger.debug("Session committed.")
    except SQLAlchemyError:
        session.rollback()
        logger.exception("Session rolled back (SQLAlchemyError).")
        raise
    except Exception:
        session.rollback()
        logger.exception("Session rolled back (unexpected error).")
        raise
    finally:
        session.close()
        logger.debug("Session closed.")


# ===========================================================================
# Schema utilities
# ===========================================================================

def create_all_tables(engine: Engine) -> None:
    """
    Create all tables in :attr:`Base.metadata` if they do not exist.

    Safe to call repeatedly (``CREATE TABLE IF NOT EXISTS`` semantics).
    """
    logger.info("Creating schema...")
    Base.metadata.create_all(engine)
    logger.info("Schema creation complete.")


def drop_all_tables(engine: Engine) -> None:
    """
    Drop all tables in :attr:`Base.metadata`.

    .. warning::
        **Destructive.**  Guarded against accidental production use.

    Raises
    ------
    RuntimeError
        If ``APP_ENV`` is ``"prod"``.
    """
    if os.getenv("APP_ENV", _ENV_DEV).lower() == _ENV_PROD:
        raise RuntimeError(
            "drop_all_tables() must never be called in production. "
            "Use Alembic downgrade migrations instead."
        )
    logger.warning("Dropping all tables — this is irreversible.")
    Base.metadata.drop_all(engine)
    logger.warning("All tables dropped.")


def check_connection(engine: Engine) -> bool:
    """
    Probe the database with ``SELECT 1``.

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
        logger.error("Database connection failed: %s", exc)
        return False