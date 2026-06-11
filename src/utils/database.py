"""
src/utils/database.py
SQLAlchemy 2.0 ORM models for the Rupiah Exchange Rate Intelligence platform.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Generator, Optional

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
    true,
    false,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.pool import NullPool, QueuePool, StaticPool

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Python-side Enum definitions
# ---------------------------------------------------------------------------
class ApiCallStatusEnum(str, PyEnum):
    SUCCESS = "SUCCESS"
    TIMEOUT = "TIMEOUT"
    RATE_LIMIT = "RATE_LIMIT"
    ERROR = "ERROR"

class AnomalyLevelEnum(str, PyEnum):
    NORMAL = "NORMAL"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"

# ---------------------------------------------------------------------------
# Environment / connection-pool config
# ---------------------------------------------------------------------------
_ENV_DEV = "dev"
_ENV_STAGING = "staging"
_ENV_PROD = "prod"
_VALID_ENVS = {_ENV_DEV, _ENV_STAGING, _ENV_PROD}

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
# Declarative base & Mixins
# ---------------------------------------------------------------------------
class Base(DeclarativeBase):
    pass

class TimestampMixin:
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
    __tablename__ = "currencies"

    currency_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(3), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=true()
    )

    rates_as_base: Mapped[list["ExchangeRate"]] = relationship(
        "ExchangeRate", foreign_keys="ExchangeRate.from_currency_id", back_populates="from_currency"
    )
    rates_as_quote: Mapped[list["ExchangeRate"]] = relationship(
        "ExchangeRate", foreign_keys="ExchangeRate.to_currency_id", back_populates="to_currency"
    )

    def __repr__(self) -> str:
        return f"<Currency id={self.currency_id} code={self.code!r}>"


class ApiSource(TimestampMixin, Base):
    __tablename__ = "api_sources"

    source_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    api_endpoint: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    retry_strategy: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    rate_limit: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=true()
    )

    exchange_rates: Mapped[list["ExchangeRate"]] = relationship("ExchangeRate", back_populates="source")
    api_calls: Mapped[list["ApiCall"]] = relationship("ApiCall", back_populates="source")

    def __repr__(self) -> str:
        return f"<ApiSource id={self.source_id} name={self.source_name!r}>"


class ExchangeRate(TimestampMixin, Base):
    __tablename__ = "exchange_rates"

    rate_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    from_currency_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("currencies.currency_id", ondelete="RESTRICT"), nullable=False
    )
    to_currency_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("currencies.currency_id", ondelete="RESTRICT"), nullable=False
    )
    source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("api_sources.source_id", ondelete="RESTRICT"), nullable=False
    )
    rate: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    data_quality_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2), nullable=True)
    is_valid: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=true()
    )

    __table_args__ = (
        CheckConstraint("rate > 0", name="ck_exchange_rates_rate_positive"),
        CheckConstraint("from_currency_id != to_currency_id", name="ck_exchange_rates_different_currencies"),
        CheckConstraint(
            "data_quality_score IS NULL OR (data_quality_score >= 0.00 AND data_quality_score <= 1.00)",
            name="ck_exchange_rates_quality_score_range",
        ),
        UniqueConstraint(
            "from_currency_id", "to_currency_id", "timestamp", "source_id",
            name="uq_exchange_rates_pair_timestamp_source",
        ),
        Index("idx_exchange_rates_pair_timestamp", "from_currency_id", "to_currency_id", "timestamp"),
        Index("idx_exchange_rates_timestamp", "timestamp"),
        Index("idx_exchange_rates_source_id", "source_id"),
        Index("idx_exchange_rates_is_valid", "is_valid"),
    )

    from_currency: Mapped["Currency"] = relationship(
        "Currency", foreign_keys=[from_currency_id], back_populates="rates_as_base"
    )
    to_currency: Mapped["Currency"] = relationship(
        "Currency", foreign_keys=[to_currency_id], back_populates="rates_as_quote"
    )
    source: Mapped["ApiSource"] = relationship("ApiSource", back_populates="exchange_rates")
    quality_metrics: Mapped[list["DataQualityMetric"]] = relationship(
        "DataQualityMetric", back_populates="exchange_rate", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<ExchangeRate id={self.rate_id} pair={self.from_currency_id}/{self.to_currency_id} rate={self.rate}>"


class ApiCall(Base):
    __tablename__ = "api_calls"

    call_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("api_sources.source_id", ondelete="RESTRICT"), nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    records_fetched: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    records_valid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    records_invalid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    execution_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_api_calls_source_timestamp", "source_id", "timestamp"),
        Index("idx_api_calls_status", "status"),
    )

    source: Mapped["ApiSource"] = relationship("ApiSource", back_populates="api_calls")

    def __repr__(self) -> str:
        return f"<ApiCall id={self.call_id} source_id={self.source_id} status={self.status!r}>"


class DataQualityMetric(Base):
    __tablename__ = "data_quality_metrics"

    metric_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    rate_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("exchange_rates.rate_id", ondelete="CASCADE"), nullable=False
    )
    check_name: Mapped[str] = mapped_column(String(100), nullable=False)
    check_passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    anomaly_score: Mapped[Optional[Decimal]] = mapped_column(Numeric(3, 2), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "anomaly_score IS NULL OR (anomaly_score >= 0.00 AND anomaly_score <= 1.00)",
            name="ck_data_quality_anomaly_score_range",
        ),
        Index("idx_data_quality_rate_check", "rate_id", "check_name"),
        Index("idx_data_quality_check_passed", "check_passed"),
    )

    exchange_rate: Mapped["ExchangeRate"] = relationship("ExchangeRate", back_populates="quality_metrics")

    def __repr__(self) -> str:
        return f"<DataQualityMetric id={self.metric_id} rate_id={self.rate_id} check={self.check_name!r}>"


class DailySnapshot(TimestampMixin, Base):
    __tablename__ = "daily_snapshots"

    snapshot_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    snapshot_date: Mapped[date] = mapped_column(Date, nullable=False)
    from_currency_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("currencies.currency_id", ondelete="RESTRICT"), nullable=False
    )
    to_currency_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("currencies.currency_id", ondelete="RESTRICT"), nullable=False
    )

    rate_open: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_high: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_low: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_close: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_avg: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_ma7: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6), nullable=True)
    pct_change: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 3), nullable=True)

    is_anomaly: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=false()
    )
    anomaly_level: Mapped[str] = mapped_column(
        String(50), nullable=False, default=AnomalyLevelEnum.NORMAL.value, server_default=text("'NORMAL'")
    )

    __table_args__ = (
        CheckConstraint("rate_high >= rate_low", name="ck_daily_snapshots_high_gte_low"),
        CheckConstraint(
            "rate_open > 0 AND rate_high > 0 AND rate_low > 0 AND rate_close > 0",
            name="ck_daily_snapshots_rates_positive",
        ),
        UniqueConstraint(
            "snapshot_date", "from_currency_id", "to_currency_id",
            name="uq_daily_snapshots_date_pair",
        ),
        Index("idx_daily_snapshots_date_pair", "snapshot_date", "from_currency_id", "to_currency_id"),
        Index("idx_daily_snapshots_is_anomaly", "is_anomaly"),
    )

    def __repr__(self) -> str:
        return f"<DailySnapshot id={self.snapshot_id} date={self.snapshot_date} pair={self.from_currency_id}/{self.to_currency_id}>"

# ===========================================================================
# Engine & Session Utilities
# ===========================================================================
def get_engine(
    database_url: Optional[str] = None,
    env: Optional[str] = None,
) -> Engine:
    resolved_env = (env or os.getenv("APP_ENV", _ENV_DEV)).lower()
    url = database_url or _get_database_url(resolved_env)

    pool_cfg = dict(_POOL_CONFIG.get(resolved_env, _POOL_CONFIG[_ENV_DEV]))
    echo = pool_cfg.pop("echo", False)

    if url.startswith("sqlite"):
        # PENTING: StaticPool untuk SQLite in-memory agar tabel persisten antar koneksi
        if url in ("sqlite://", "sqlite:///:memory:") or ":memory:" in url:
            engine = create_engine(
                url,
                echo=echo,
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
            )
        else:
            engine = create_engine(url, echo=echo, poolclass=NullPool)
    else:
        engine = create_engine(url, echo=echo, poolclass=QueuePool, **pool_cfg)

    @event.listens_for(engine, "connect")
    def _set_utc(dbapi_conn, _record) -> None:
        try:
            cur = dbapi_conn.cursor()
            cur.execute("SET TIME ZONE 'UTC'")
            cur.close()
        except Exception:
            pass

    return engine

def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
    )

@contextmanager
def get_session(engine: Engine) -> Generator[Session, None, None]:
    factory = get_session_factory(engine)
    session: Session = factory()
    try:
        yield session
        session.commit()
    except SQLAlchemyError:
        session.rollback()
        raise
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def create_all_tables(engine: Engine) -> None:
    Base.metadata.create_all(engine)

def drop_all_tables(engine: Engine) -> None:
    if os.getenv("APP_ENV", _ENV_DEV).lower() == _ENV_PROD:
        raise RuntimeError("drop_all_tables() must never be called in production.")
    Base.metadata.drop_all(engine)

def check_connection(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except OperationalError:
        return False