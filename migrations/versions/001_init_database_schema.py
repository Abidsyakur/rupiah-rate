"""
migrations/versions/001_initial_schema.py
==========================================
Initial database schema migration for the Rupiah Exchange Rate Intelligence
platform.

Revision  : 001_initial_schema
Created   : 2025-01-15
Author    : rupiah-exchange-rate-intelligence
Schema Ref: docs/SCHEMA.md (ADR-002)
Models Ref: src/models/database.py

What this migration does
------------------------
upgrade() / up()
  1.  Detect dialect (PostgreSQL vs SQLite) to conditionally create PG-only
      objects (ENUM types, CHECK constraints via ALTER TABLE).
  2.  Create ENUM types  – api_call_status, anomaly_level  (PostgreSQL only)
  3.  Create tables in dependency order:
        currencies → api_sources → exchange_rates
        exchange_rates → api_calls
        exchange_rates → data_quality_metrics
        currencies → daily_snapshots
  4.  Create all indexes defined in ADR-002.
  5.  Seed reference data:
        - 5 currencies : USD, IDR, EUR, SGD, JPY
        - 2 api_sources: yfinance, FRED API

downgrade() / down()
  1.  Delete all seed data rows (idempotent ON CONFLICT DO NOTHING inverse).
  2.  Drop indexes (explicit, so SQLite's lack of CASCADE is not an issue).
  3.  Drop tables in reverse dependency order.
  4.  Drop ENUM types (PostgreSQL only).

Idempotency
-----------
Every CREATE uses IF NOT EXISTS.
Seed INSERT uses ON CONFLICT DO NOTHING (PostgreSQL) / INSERT OR IGNORE (SQLite).
The migration can therefore be re-run safely on a database that already has
some or all of these objects.

Rollback safety
---------------
downgrade() is wrapped in a try/except per logical step.  A failed DROP of
one object does not abort the cleanup of the others.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Alembic revision identifiers
# ---------------------------------------------------------------------------

revision: str = "001_initial_schema"
down_revision: str | None = None    # first migration in the chain
branch_labels: str | None = None
depends_on: str | None = None


# ---------------------------------------------------------------------------
# Dialect helper
# ---------------------------------------------------------------------------

def _is_postgresql() -> bool:
    """Return True when the current migration context targets PostgreSQL."""
    bind = op.get_bind()
    return bind.dialect.name == "postgresql"


def _is_sqlite() -> bool:
    bind = op.get_bind()
    return bind.dialect.name == "sqlite"


def _now_sql() -> str:
    """Return the current-timestamp SQL expression for the active dialect."""
    return "now()" if _is_postgresql() else "CURRENT_TIMESTAMP"


# ---------------------------------------------------------------------------
# Seed-data helpers
# ---------------------------------------------------------------------------

# Reference currencies required by the pipeline (ADR-001 currency pairs)
_SEED_CURRENCIES: list[dict[str, Any]] = [
    {"code": "USD", "name": "US Dollar",             "is_active": True},
    {"code": "IDR", "name": "Indonesian Rupiah",      "is_active": True},
    {"code": "EUR", "name": "Euro",                   "is_active": True},
    {"code": "SGD", "name": "Singapore Dollar",       "is_active": True},
    {"code": "JPY", "name": "Japanese Yen",           "is_active": True},
]

# Initial API sources (mirrors config/dev.yaml extractors block)
_SEED_API_SOURCES: list[dict[str, Any]] = [
    {
        "source_name":    "yfinance",
        "api_endpoint":   "https://finance.yahoo.com",
        "retry_strategy": "exponential_backoff_max3",
        "rate_limit":     2000,        # unofficial; conservative estimate
        "is_active":      True,
    },
    {
        "source_name":    "fred",
        "api_endpoint":   "https://api.stlouisfed.org/fred",
        "retry_strategy": "exponential_backoff_max3",
        "rate_limit":     120,         # FRED documented limit: 120 req/min
        "is_active":      True,
    },
]


def _insert_seed_row(table: str, row: dict[str, Any], conflict_column: str) -> None:
    """
    Insert a single seed row idempotently.

    PostgreSQL uses ``ON CONFLICT DO NOTHING``.
    SQLite     uses ``INSERT OR IGNORE``.

    Parameters
    ----------
    table:
        Target table name.
    row:
        Column → value mapping to insert.
    conflict_column:
        The unique column name used as the conflict target (PG only).
    """
    now = datetime.now(timezone.utc).isoformat()
    row_with_ts = {**row, "created_at": now, "updated_at": now}

    if _is_postgresql():
        # Use raw SQL with ON CONFLICT DO NOTHING for idempotency
        cols   = ", ".join(row_with_ts.keys())
        params = ", ".join(f":{k}" for k in row_with_ts.keys())
        sql = sa.text(
            f"INSERT INTO {table} ({cols}) "
            f"VALUES ({params}) "
            f"ON CONFLICT ({conflict_column}) DO NOTHING"
        )
    else:
        # SQLite: INSERT OR IGNORE achieves the same effect
        cols   = ", ".join(row_with_ts.keys())
        params = ", ".join(f":{k}" for k in row_with_ts.keys())
        sql = sa.text(
            f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({params})"
        )

    op.get_bind().execute(sql, row_with_ts)
    logger.debug("Seeded %s: %s", table, row.get("code") or row.get("source_name"))


# ===========================================================================
# upgrade  (up)
# ===========================================================================

def upgrade() -> None:
    """
    Apply the initial schema.

    Steps
    -----
    1. Create PostgreSQL ENUM types.
    2. Create all six tables with full constraints and indexes.
    3. Insert seed currencies and API sources.
    """
    _create_enum_types()
    _create_currencies_table()
    _create_api_sources_table()
    _create_exchange_rates_table()
    _create_api_calls_table()
    _create_data_quality_metrics_table()
    _create_daily_snapshots_table()
    _seed_currencies()
    _seed_api_sources()
    logger.info("[001_initial_schema] upgrade complete.")


# Alias used in tests / documentation
up = upgrade


# ---------------------------------------------------------------------------
# Step 1 – ENUM types  (PostgreSQL only)
# ---------------------------------------------------------------------------

def _create_enum_types() -> None:
    """
    Create PostgreSQL ENUM types used by api_calls and daily_snapshots.

    SQLite stores these as plain VARCHAR — no action needed there.
    Both types use CREATE TYPE IF NOT EXISTS so re-runs are safe.
    """
    if not _is_postgresql():
        logger.debug("Skipping ENUM creation — not PostgreSQL.")
        return

    # api_call_status
    op.get_bind().execute(sa.text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_type WHERE typname = 'api_call_status'
            ) THEN
                CREATE TYPE api_call_status AS ENUM (
                    'SUCCESS', 'TIMEOUT', 'RATE_LIMIT', 'ERROR'
                );
            END IF;
        END
        $$;
    """))
    logger.debug("ENUM type 'api_call_status' ensured.")

    # anomaly_level
    op.get_bind().execute(sa.text("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_type WHERE typname = 'anomaly_level'
            ) THEN
                CREATE TYPE anomaly_level AS ENUM (
                    'NORMAL', 'WARNING', 'CRITICAL'
                );
            END IF;
        END
        $$;
    """))
    logger.debug("ENUM type 'anomaly_level' ensured.")


# ---------------------------------------------------------------------------
# Step 2 – currencies
# ---------------------------------------------------------------------------

def _create_currencies_table() -> None:
    """
    Create the ``currencies`` dimension table.

    Stores ISO 4217 currency reference data.  Used as FK target by
    exchange_rates and daily_snapshots.
    """
    op.create_table(
        "currencies",

        # ---- Primary key ----
        sa.Column(
            "currency_id",
            sa.Integer,
            primary_key=True,
            autoincrement=True,
            comment="Surrogate primary key.",
        ),

        # ---- Business columns ----
        sa.Column(
            "code",
            sa.String(3),
            nullable=False,
            comment="ISO 4217 three-letter code, e.g. USD.",
        ),
        sa.Column(
            "name",
            sa.String(255),
            nullable=False,
            comment="Human-readable currency name.",
        ),
        sa.Column(
            "is_active",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("true"),
            comment="Soft-delete flag; false = retired currency.",
        ),

        # ---- Timestamps ----
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="UTC row-creation timestamp.",
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="UTC last-update timestamp.",
        ),

        # ---- Constraints ----
        sa.UniqueConstraint("code", name="uq_currencies_code"),

        if_not_exists=True,
        comment="ISO 4217 currency reference data (dimension table).",
    )

    # Dedicated index on code for fast FK lookups
    _create_index_if_not_exists(
        "idx_currencies_code", "currencies", ["code"], unique=True
    )
    logger.debug("Table 'currencies' ensured.")


# ---------------------------------------------------------------------------
# Step 3 – api_sources
# ---------------------------------------------------------------------------

def _create_api_sources_table() -> None:
    """
    Create the ``api_sources`` configuration table.

    Each row describes one external data provider (yfinance, FRED, etc.).
    """
    op.create_table(
        "api_sources",

        sa.Column(
            "source_id",
            sa.Integer,
            primary_key=True,
            autoincrement=True,
            comment="Surrogate primary key.",
        ),
        sa.Column(
            "source_name",
            sa.String(100),
            nullable=False,
            comment="Canonical short name, e.g. 'yfinance' or 'fred'.",
        ),
        sa.Column(
            "api_endpoint",
            sa.String(500),
            nullable=True,
            comment="Base URL of the external API.",
        ),
        sa.Column(
            "retry_strategy",
            sa.String(100),
            nullable=True,
            comment="Human-readable retry strategy description.",
        ),
        sa.Column(
            "rate_limit",
            sa.Integer,
            nullable=True,
            comment="Max requests per hour (NULL = unknown/unlimited).",
        ),
        sa.Column(
            "is_active",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("true"),
            comment="False = source is disabled; rows are kept for FK integrity.",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),

        sa.UniqueConstraint("source_name", name="uq_api_sources_source_name"),

        if_not_exists=True,
        comment="External API source registry.",
    )

    _create_index_if_not_exists(
        "idx_api_sources_source_name", "api_sources", ["source_name"], unique=True
    )
    logger.debug("Table 'api_sources' ensured.")


# ---------------------------------------------------------------------------
# Step 4 – exchange_rates  (main fact table)
# ---------------------------------------------------------------------------

def _create_exchange_rates_table() -> None:
    """
    Create the ``exchange_rates`` fact table.

    Each row is a single rate observation from one API source at one point
    in time.  The (from_currency, to_currency, timestamp, source) 4-tuple
    must be unique for idempotent upserts.

    CHECK constraints
    -----------------
    - rate > 0
    - from_currency_id != to_currency_id
    - data_quality_score in [0.00, 1.00] or NULL
    """
    op.create_table(
        "exchange_rates",

        # ---- Primary key ----
        sa.Column(
            "rate_id",
            sa.BigInteger,
            primary_key=True,
            autoincrement=True,
            comment="Surrogate BigInteger PK.",
        ),

        # ---- Foreign keys ----
        sa.Column(
            "from_currency_id",
            sa.Integer,
            sa.ForeignKey(
                "currencies.currency_id",
                name="fk_exchange_rates_from_currency",
                ondelete="RESTRICT",
            ),
            nullable=False,
            comment="Base currency (e.g. USD).",
        ),
        sa.Column(
            "to_currency_id",
            sa.Integer,
            sa.ForeignKey(
                "currencies.currency_id",
                name="fk_exchange_rates_to_currency",
                ondelete="RESTRICT",
            ),
            nullable=False,
            comment="Quote currency (e.g. IDR).",
        ),
        sa.Column(
            "source_id",
            sa.Integer,
            sa.ForeignKey(
                "api_sources.source_id",
                name="fk_exchange_rates_source",
                ondelete="RESTRICT",
            ),
            nullable=False,
            comment="FK to the API source that provided this rate.",
        ),

        # ---- Business columns ----
        sa.Column(
            "rate",
            sa.Numeric(12, 6),
            nullable=False,
            comment="Exchange rate value.  Enforced > 0 via CHECK constraint.",
        ),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            nullable=False,
            comment="Market timestamp of the observation (UTC).",
        ),
        sa.Column(
            "data_quality_score",
            sa.Numeric(3, 2),
            nullable=True,
            comment="Composite quality score 0.00 – 1.00.",
        ),
        sa.Column(
            "is_valid",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("true"),
            comment="False = failed quality checks; excluded from analytics.",
        ),

        # ---- Timestamps ----
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),

        # ---- Constraints ----
        sa.CheckConstraint(
            "rate > 0",
            name="ck_exchange_rates_rate_positive",
        ),
        sa.CheckConstraint(
            "from_currency_id != to_currency_id",
            name="ck_exchange_rates_different_currencies",
        ),
        sa.CheckConstraint(
            "data_quality_score IS NULL OR "
            "(data_quality_score >= 0.00 AND data_quality_score <= 1.00)",
            name="ck_exchange_rates_quality_score_range",
        ),
        sa.UniqueConstraint(
            "from_currency_id",
            "to_currency_id",
            "timestamp",
            "source_id",
            name="uq_exchange_rates_pair_timestamp_source",
        ),

        if_not_exists=True,
        comment="Raw exchange rate observations (fact table).",
    )

    # ADR-002 indexes
    _create_index_if_not_exists(
        "idx_exchange_rates_pair_timestamp",
        "exchange_rates",
        ["from_currency_id", "to_currency_id", "timestamp"],
    )
    _create_index_if_not_exists(
        "idx_exchange_rates_timestamp",
        "exchange_rates",
        ["timestamp"],
    )
    _create_index_if_not_exists(
        "idx_exchange_rates_source_id",
        "exchange_rates",
        ["source_id"],
    )
    _create_index_if_not_exists(
        "idx_exchange_rates_is_valid",
        "exchange_rates",
        ["is_valid"],
    )
    logger.debug("Table 'exchange_rates' ensured.")


# ---------------------------------------------------------------------------
# Step 5 – api_calls  (audit trail, insert-only)
# ---------------------------------------------------------------------------

def _create_api_calls_table() -> None:
    """
    Create the ``api_calls`` audit table.

    Insert-only — rows are never updated, so no ``updated_at`` column.
    Status column uses the PostgreSQL ENUM type created in step 1;
    SQLite stores it as VARCHAR(50).
    """
    # Choose the status column type based on the dialect
    status_type: sa.types.TypeEngine
    if _is_postgresql():
        status_type = sa.Enum(
            "SUCCESS", "TIMEOUT", "RATE_LIMIT", "ERROR",
            name="api_call_status",
            create_type=False,   # already created in step 1
        )
    else:
        status_type = sa.String(50)

    op.create_table(
        "api_calls",

        sa.Column(
            "call_id",
            sa.BigInteger,
            primary_key=True,
            autoincrement=True,
            comment="Surrogate BigInteger PK.",
        ),
        sa.Column(
            "source_id",
            sa.Integer,
            sa.ForeignKey(
                "api_sources.source_id",
                name="fk_api_calls_source",
                ondelete="RESTRICT",
            ),
            nullable=False,
            comment="FK to the source that was called.",
        ),
        sa.Column(
            "timestamp",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="UTC timestamp when the API call was initiated.",
        ),
        sa.Column(
            "status",
            status_type,
            nullable=False,
            comment="Call outcome: SUCCESS | TIMEOUT | RATE_LIMIT | ERROR.",
        ),
        sa.Column(
            "error_message",
            sa.Text,
            nullable=True,
            comment="Full error text when status != SUCCESS.",
        ),
        sa.Column(
            "records_fetched",
            sa.Integer,
            nullable=True,
            comment="Total records returned by the API.",
        ),
        sa.Column(
            "records_valid",
            sa.Integer,
            nullable=True,
            comment="Records that passed all validation checks.",
        ),
        sa.Column(
            "records_invalid",
            sa.Integer,
            nullable=True,
            comment="Records that failed one or more validation checks.",
        ),
        sa.Column(
            "execution_time_ms",
            sa.Integer,
            nullable=True,
            comment="End-to-end API call duration in milliseconds.",
        ),

        # Insert-only: created_at only, no updated_at
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
            comment="UTC row-creation timestamp.",
        ),

        if_not_exists=True,
        comment="Immutable API call audit trail.",
    )

    _create_index_if_not_exists(
        "idx_api_calls_source_timestamp",
        "api_calls",
        ["source_id", "timestamp"],
    )
    _create_index_if_not_exists(
        "idx_api_calls_status",
        "api_calls",
        ["status"],
    )
    logger.debug("Table 'api_calls' ensured.")


# ---------------------------------------------------------------------------
# Step 6 – data_quality_metrics
# ---------------------------------------------------------------------------

def _create_data_quality_metrics_table() -> None:
    """
    Create the ``data_quality_metrics`` tracking table.

    One row per (rate_id, check_name) quality-check execution.
    CASCADE on delete: removing a rate removes its metrics automatically.
    """
    op.create_table(
        "data_quality_metrics",

        sa.Column(
            "metric_id",
            sa.BigInteger,
            primary_key=True,
            autoincrement=True,
            comment="Surrogate BigInteger PK.",
        ),
        sa.Column(
            "rate_id",
            sa.BigInteger,
            sa.ForeignKey(
                "exchange_rates.rate_id",
                name="fk_data_quality_metrics_rate",
                ondelete="CASCADE",     # orphan cleanup handled by DB
            ),
            nullable=False,
            comment="FK to the exchange rate this metric evaluates.",
        ),
        sa.Column(
            "check_name",
            sa.String(100),
            nullable=False,
            comment="Check identifier: NULL_CHECK | RANGE_CHECK | ANOMALY_CHECK.",
        ),
        sa.Column(
            "check_passed",
            sa.Boolean,
            nullable=False,
            comment="True = check passed.",
        ),
        sa.Column(
            "anomaly_score",
            sa.Numeric(3, 2),
            nullable=True,
            comment="Anomaly severity 0.00 – 1.00; NULL for non-anomaly checks.",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),

        sa.CheckConstraint(
            "anomaly_score IS NULL OR "
            "(anomaly_score >= 0.00 AND anomaly_score <= 1.00)",
            name="ck_data_quality_anomaly_score_range",
        ),

        if_not_exists=True,
        comment="Per-record data quality check results.",
    )

    _create_index_if_not_exists(
        "idx_data_quality_rate_check",
        "data_quality_metrics",
        ["rate_id", "check_name"],
    )
    _create_index_if_not_exists(
        "idx_data_quality_check_passed",
        "data_quality_metrics",
        ["check_passed"],
    )
    logger.debug("Table 'data_quality_metrics' ensured.")


# ---------------------------------------------------------------------------
# Step 7 – daily_snapshots
# ---------------------------------------------------------------------------

def _create_daily_snapshots_table() -> None:
    """
    Create the ``daily_snapshots`` materialized/aggregated table.

    Pre-computed OHLCV data populated by dbt marts or a scheduled job.
    CHECK constraints enforce OHLC integrity (high >= low, all rates > 0).
    Anomaly level uses the PostgreSQL ENUM; VARCHAR otherwise.
    """
    anomaly_level_type: sa.types.TypeEngine
    if _is_postgresql():
        anomaly_level_type = sa.Enum(
            "NORMAL", "WARNING", "CRITICAL",
            name="anomaly_level",
            create_type=False,
        )
    else:
        anomaly_level_type = sa.String(50)

    op.create_table(
        "daily_snapshots",

        sa.Column(
            "snapshot_id",
            sa.BigInteger,
            primary_key=True,
            autoincrement=True,
            comment="Surrogate BigInteger PK.",
        ),
        sa.Column(
            "snapshot_date",
            sa.Date,
            nullable=False,
            comment="Calendar date this snapshot covers.",
        ),
        sa.Column(
            "from_currency_id",
            sa.Integer,
            sa.ForeignKey(
                "currencies.currency_id",
                name="fk_daily_snapshots_from_currency",
                ondelete="RESTRICT",
            ),
            nullable=False,
            comment="Base currency FK.",
        ),
        sa.Column(
            "to_currency_id",
            sa.Integer,
            sa.ForeignKey(
                "currencies.currency_id",
                name="fk_daily_snapshots_to_currency",
                ondelete="RESTRICT",
            ),
            nullable=False,
            comment="Quote currency FK.",
        ),

        # ---- OHLCV columns ----
        sa.Column("rate_open",  sa.Numeric(12, 6), nullable=False,
                  comment="First rate of the day."),
        sa.Column("rate_high",  sa.Numeric(12, 6), nullable=False,
                  comment="Highest rate of the day."),
        sa.Column("rate_low",   sa.Numeric(12, 6), nullable=False,
                  comment="Lowest rate of the day."),
        sa.Column("rate_close", sa.Numeric(12, 6), nullable=False,
                  comment="Last rate of the day."),
        sa.Column("rate_avg",   sa.Numeric(12, 6), nullable=False,
                  comment="Average rate across all observations."),
        sa.Column("rate_ma7",   sa.Numeric(12, 6), nullable=True,
                  comment="7-day moving average (NULL for first 6 days)."),

        # ---- Derived metrics ----
        sa.Column(
            "pct_change",
            sa.Numeric(6, 3),
            nullable=True,
            comment="Percent change vs. previous trading day close.",
        ),

        # ---- Anomaly fields ----
        sa.Column(
            "is_anomaly",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("false"),
            comment="True if daily movement triggered an anomaly alert.",
        ),
        sa.Column(
            "anomaly_level",
            anomaly_level_type,
            nullable=False,
            server_default=sa.text("'NORMAL'"),
            comment="Severity: NORMAL | WARNING | CRITICAL.",
        ),

        # ---- Timestamps ----
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),

        # ---- Constraints ----
        sa.CheckConstraint(
            "rate_high >= rate_low",
            name="ck_daily_snapshots_high_gte_low",
        ),
        sa.CheckConstraint(
            "rate_open > 0 AND rate_high > 0 AND rate_low > 0 AND rate_close > 0",
            name="ck_daily_snapshots_rates_positive",
        ),
        sa.UniqueConstraint(
            "snapshot_date",
            "from_currency_id",
            "to_currency_id",
            name="uq_daily_snapshots_date_pair",
        ),

        if_not_exists=True,
        comment="Pre-aggregated daily OHLCV exchange rate snapshots.",
    )

    _create_index_if_not_exists(
        "idx_daily_snapshots_date_pair",
        "daily_snapshots",
        ["snapshot_date", "from_currency_id", "to_currency_id"],
    )
    _create_index_if_not_exists(
        "idx_daily_snapshots_is_anomaly",
        "daily_snapshots",
        ["is_anomaly"],
    )
    logger.debug("Table 'daily_snapshots' ensured.")


# ---------------------------------------------------------------------------
# Steps 8 & 9 – seed data
# ---------------------------------------------------------------------------

def _seed_currencies() -> None:
    """
    Insert the five reference currencies required by ADR-001 currency pairs.

    USD, IDR, EUR, SGD, JPY.
    Uses INSERT OR IGNORE / ON CONFLICT DO NOTHING for idempotency.
    """
    logger.info("Seeding currencies...")
    for row in _SEED_CURRENCIES:
        _insert_seed_row("currencies", row, conflict_column="code")
    logger.info("Seeded %d currencies.", len(_SEED_CURRENCIES))


def _seed_api_sources() -> None:
    """
    Insert the initial API source records for yfinance and FRED.

    Matches the extractors block in config/dev.yaml.
    """
    logger.info("Seeding api_sources...")
    for row in _SEED_API_SOURCES:
        _insert_seed_row("api_sources", row, conflict_column="source_name")
    logger.info("Seeded %d api_sources.", len(_SEED_API_SOURCES))


# ===========================================================================
# downgrade  (down)
# ===========================================================================

def downgrade() -> None:
    """
    Reverse the initial schema migration.

    Steps (reverse of upgrade)
    --------------------------
    1. Delete seed data rows (safe no-op if already absent).
    2. Drop indexes explicitly before tables.
    3. Drop tables in reverse FK-dependency order.
    4. Drop ENUM types (PostgreSQL only).
    """
    _delete_seed_data()
    _drop_indexes()
    _drop_tables()
    _drop_enum_types()
    logger.info("[001_initial_schema] downgrade complete.")


# Alias
down = downgrade


# ---------------------------------------------------------------------------
# Downgrade helpers
# ---------------------------------------------------------------------------

def _delete_seed_data() -> None:
    """Remove seeded rows.  Wrapped per-table so one failure doesn't block others."""
    bind = op.get_bind()

    # Delete api_sources seed rows
    for row in _SEED_API_SOURCES:
        try:
            bind.execute(
                sa.text("DELETE FROM api_sources WHERE source_name = :name"),
                {"name": row["source_name"]},
            )
        except Exception as exc:
            logger.warning("Could not delete api_source %s: %s", row["source_name"], exc)

    # Delete currencies seed rows
    for row in _SEED_CURRENCIES:
        try:
            bind.execute(
                sa.text("DELETE FROM currencies WHERE code = :code"),
                {"code": row["code"]},
            )
        except Exception as exc:
            logger.warning("Could not delete currency %s: %s", row["code"], exc)

    logger.debug("Seed data deletion attempted.")


def _drop_indexes() -> None:
    """
    Drop all named indexes created in upgrade().

    op.drop_index() is safe to call even on SQLite; we catch any error
    per-index so one missing index doesn't abort the rest.
    """
    indexes_by_table: list[tuple[str, str]] = [
        # (index_name, table_name)
        ("idx_daily_snapshots_is_anomaly",      "daily_snapshots"),
        ("idx_daily_snapshots_date_pair",        "daily_snapshots"),
        ("idx_data_quality_check_passed",        "data_quality_metrics"),
        ("idx_data_quality_rate_check",          "data_quality_metrics"),
        ("idx_api_calls_status",                 "api_calls"),
        ("idx_api_calls_source_timestamp",       "api_calls"),
        ("idx_exchange_rates_is_valid",          "exchange_rates"),
        ("idx_exchange_rates_source_id",         "exchange_rates"),
        ("idx_exchange_rates_timestamp",         "exchange_rates"),
        ("idx_exchange_rates_pair_timestamp",    "exchange_rates"),
        ("idx_api_sources_source_name",          "api_sources"),
        ("idx_currencies_code",                  "currencies"),
    ]
    for index_name, table_name in indexes_by_table:
        try:
            op.drop_index(index_name, table_name=table_name, if_exists=True)
            logger.debug("Dropped index %s.", index_name)
        except Exception as exc:
            logger.warning("Could not drop index %s: %s", index_name, exc)


def _drop_tables() -> None:
    """
    Drop all tables in reverse FK-dependency order.

    Leaf tables (no outgoing FKs) are dropped first to avoid FK violations.
    """
    tables_in_order: list[str] = [
        "daily_snapshots",         # FK → currencies
        "data_quality_metrics",    # FK → exchange_rates  (CASCADE, but explicit is safer)
        "api_calls",               # FK → api_sources
        "exchange_rates",          # FK → currencies, api_sources
        "api_sources",             # referenced by exchange_rates, api_calls
        "currencies",              # referenced by exchange_rates, daily_snapshots
    ]
    for table_name in tables_in_order:
        try:
            op.drop_table(table_name, if_exists=True)
            logger.debug("Dropped table %s.", table_name)
        except Exception as exc:
            logger.warning("Could not drop table %s: %s", table_name, exc)


def _drop_enum_types() -> None:
    """Drop PostgreSQL ENUM types created in _create_enum_types()."""
    if not _is_postgresql():
        return

    for type_name in ("anomaly_level", "api_call_status"):
        try:
            op.get_bind().execute(
                sa.text(f"DROP TYPE IF EXISTS {type_name}")
            )
            logger.debug("Dropped ENUM type %s.", type_name)
        except Exception as exc:
            logger.warning("Could not drop ENUM type %s: %s", type_name, exc)


# ---------------------------------------------------------------------------
# Shared utility
# ---------------------------------------------------------------------------

def _create_index_if_not_exists(
    index_name: str,
    table_name: str,
    columns: list[str],
    unique: bool = False,
) -> None:
    """
    Create a named index only if it does not already exist.

    Alembic's ``op.create_index`` with ``if_not_exists=True`` handles this
    for both PostgreSQL and SQLite.

    Parameters
    ----------
    index_name:
        Unique name for the index.
    table_name:
        Table the index is applied to.
    columns:
        List of column names to include in the index.
    unique:
        If True, creates a UNIQUE index.
    """
    op.create_index(
        index_name,
        table_name,
        columns,
        unique=unique,
        if_not_exists=True,
    )