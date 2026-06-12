# Database Schema Reference

> **Generated from:** `src/models/database.py` (SQLAlchemy 2.0)
> **Schema reference:** `docs/SCHEMA.md` (ADR-002)
> **Dialect notes:** PostgreSQL is the production target. SQLite (used in unit
> tests) requires two compatibility shims, called out inline below.

This document describes all six tables, their columns, constraints, indexes,
and relationships, as defined by the ORM models.

---

## Table of Contents

1. [currencies](#1-currencies)
2. [api_sources](#2-api_sources)
3. [exchange_rates](#3-exchange_rates)
4. [api_calls](#4-api_calls)
5. [data_quality_metrics](#5-data_quality_metrics)
6. [daily_snapshots](#6-daily_snapshots)
7. [Entity Relationship Diagram](#7-entity-relationship-diagram)
8. [Cross-cutting conventions](#8-cross-cutting-conventions)

---

## 1. `currencies`

**ORM class:** `Currency`
**Type:** Dimension table
**Purpose:** ISO 4217 currency reference data. Acts as the FK target for both
`exchange_rates` and `daily_snapshots`.

### Columns

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `currency_id` | `INTEGER` | No | autoincrement | Surrogate primary key. |
| `code` | `VARCHAR(3)` | No | — | ISO 4217 three-letter code, e.g. `USD`. |
| `name` | `VARCHAR(255)` | No | — | Full currency name, e.g. `US Dollar`. |
| `is_active` | `BOOLEAN` | No | `true` | Soft-delete flag; `false` retires the currency without breaking FK references. |
| `created_at` | `TIMESTAMPTZ` | No | `now()` | Row-creation timestamp (UTC). |
| `updated_at` | `TIMESTAMPTZ` | No | `now()`, refreshed `onupdate` | Last-update timestamp (UTC). |

### Constraints & Indexes

| Name | Type | Columns | Notes |
|---|---|---|---|
| (implicit PK) | PRIMARY KEY | `currency_id` | |
| `code` unique | UNIQUE + INDEX | `code` | Declared via `unique=True, index=True` on the column. |

### Relationships

| Attribute | Target | Type | `back_populates` |
|---|---|---|---|
| `rates_as_base` | `ExchangeRate` (via `from_currency_id`) | one-to-many | `ExchangeRate.from_currency` |
| `rates_as_quote` | `ExchangeRate` (via `to_currency_id`) | one-to-many | `ExchangeRate.to_currency` |

---

## 2. `api_sources`

**ORM class:** `ApiSource`
**Type:** Configuration table
**Purpose:** Registry of external data providers (`yfinance`, `fred`, …).
Stores retry/rate-limit metadata so pipeline behaviour can be tuned without
code changes.

### Columns

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `source_id` | `INTEGER` | No | autoincrement | Surrogate primary key. |
| `source_name` | `VARCHAR(100)` | No | — | Canonical identifier, e.g. `yfinance` or `fred`. |
| `api_endpoint` | `VARCHAR(500)` | Yes | `NULL` | Base URL of the external API. |
| `retry_strategy` | `VARCHAR(100)` | Yes | `NULL` | Human-readable retry description, e.g. `exponential_backoff_max3`. |
| `rate_limit` | `INTEGER` | Yes | `NULL` | Max requests per hour. `NULL` = unknown/unlimited. |
| `is_active` | `BOOLEAN` | No | `true` | `false` disables the source without deleting FK-referenced rows. |
| `created_at` | `TIMESTAMPTZ` | No | `now()` | Row-creation timestamp (UTC). |
| `updated_at` | `TIMESTAMPTZ` | No | `now()`, refreshed `onupdate` | Last-update timestamp (UTC). |

### Constraints & Indexes

| Name | Type | Columns | Notes |
|---|---|---|---|
| (implicit PK) | PRIMARY KEY | `source_id` | |
| `source_name` unique | UNIQUE + INDEX | `source_name` | Declared via `unique=True, index=True` on the column. |

### Relationships

| Attribute | Target | Type | `back_populates` |
|---|---|---|---|
| `exchange_rates` | `ExchangeRate` | one-to-many | `ExchangeRate.source` |
| `api_calls` | `ApiCall` | one-to-many | `ApiCall.source` |

---

## 3. `exchange_rates`

**ORM class:** `ExchangeRate`
**Type:** Fact table
**Purpose:** Raw exchange-rate observations. One row = one rate between two
currencies, at one point in time, from one source. The 4-tuple
`(from_currency_id, to_currency_id, timestamp, source_id)` is unique to
support idempotent upserts.

### Columns

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `rate_id` | `BIGINT`¹ | No | autoincrement | Surrogate primary key. |
| `from_currency_id` | `INTEGER` | No | — | FK → `currencies.currency_id`. Base currency (e.g. USD). |
| `to_currency_id` | `INTEGER` | No | — | FK → `currencies.currency_id`. Quote currency (e.g. IDR). |
| `source_id` | `INTEGER` | No | — | FK → `api_sources.source_id`. |
| `rate` | `NUMERIC(12, 6)` | No | — | Exchange rate value. Enforced `> 0` via CHECK. |
| `timestamp` | `TIMESTAMPTZ` | No | — | Market timestamp of the observation (UTC). |
| `data_quality_score` | `NUMERIC(3, 2)` | Yes | `NULL` | Composite quality score, `0.00`–`1.00`. |
| `is_valid` | `BOOLEAN` | No | `true` | `false` = failed quality checks; excluded from analytics. |
| `created_at` | `TIMESTAMPTZ` | No | `now()` | Row-creation timestamp (UTC). |
| `updated_at` | `TIMESTAMPTZ` | No | `now()`, refreshed `onupdate` | Last-update timestamp (UTC). |

¹ `BigInteger().with_variant(Integer, "sqlite")` — compiles to `BIGINT` on
PostgreSQL, but `INTEGER` on SQLite so the column remains a rowid alias and
autoincrement continues to work in tests.

### Constraints & Indexes

| Name | Type | Definition |
|---|---|---|
| `ck_exchange_rates_rate_positive` | CHECK | `rate > 0` |
| `ck_exchange_rates_different_currencies` | CHECK | `from_currency_id != to_currency_id` |
| `ck_exchange_rates_quality_score_range` | CHECK | `data_quality_score IS NULL OR (data_quality_score >= 0.00 AND data_quality_score <= 1.00)` |
| `uq_exchange_rates_pair_timestamp_source` | UNIQUE | `(from_currency_id, to_currency_id, timestamp, source_id)` |
| `idx_exchange_rates_pair_timestamp` | INDEX | `(from_currency_id, to_currency_id, timestamp)` |
| `idx_exchange_rates_timestamp` | INDEX | `(timestamp)` |
| `idx_exchange_rates_source_id` | INDEX | `(source_id)` |
| `idx_exchange_rates_is_valid` | INDEX | `(is_valid)` |

### Foreign Keys

| Column | References | On Delete |
|---|---|---|
| `from_currency_id` | `currencies.currency_id` | `RESTRICT` |
| `to_currency_id` | `currencies.currency_id` | `RESTRICT` |
| `source_id` | `api_sources.source_id` | `RESTRICT` |

### Relationships

| Attribute | Target | Type | `back_populates` / `cascade` |
|---|---|---|---|
| `from_currency` | `Currency` (via `from_currency_id`) | many-to-one | `Currency.rates_as_base` |
| `to_currency` | `Currency` (via `to_currency_id`) | many-to-one | `Currency.rates_as_quote` |
| `source` | `ApiSource` | many-to-one | `ApiSource.exchange_rates` |
| `quality_metrics` | `DataQualityMetric` | one-to-many | `cascade="all, delete-orphan"` |

---

## 4. `api_calls`

**ORM class:** `ApiCall`
**Type:** Audit trail (insert-only)
**Purpose:** Immutable log of every API call made by the pipeline. Rows are
never updated — note the deliberate **absence** of `updated_at`.

### Columns

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `call_id` | `BIGINT`¹ | No | autoincrement | Surrogate primary key. |
| `source_id` | `INTEGER` | No | — | FK → `api_sources.source_id`. |
| `timestamp` | `TIMESTAMPTZ` | No | `now()` | UTC time the call was initiated. |
| `status` | `VARCHAR(50)` | No | — | One of `SUCCESS`, `TIMEOUT`, `RATE_LIMIT`, `ERROR`. |
| `error_message` | `TEXT` | Yes | `NULL` | Full error text when `status != 'SUCCESS'`. |
| `records_fetched` | `INTEGER` | Yes | `NULL` | Total records returned by the API. |
| `records_valid` | `INTEGER` | Yes | `NULL` | Records that passed all validation checks. |
| `records_invalid` | `INTEGER` | Yes | `NULL` | Records that failed one or more validation checks. |
| `execution_time_ms` | `INTEGER` | Yes | `NULL` | End-to-end call duration in milliseconds. |
| `created_at` | `TIMESTAMPTZ` | No | `now()` | Row-creation timestamp (UTC). **No `updated_at` — insert-only.** |

¹ `BigInteger().with_variant(Integer, "sqlite")` — see footnote in §3.

> **Status enum:** validated at the Python level via `ApiCallStatusEnum`
> (`SUCCESS`, `TIMEOUT`, `RATE_LIMIT`, `ERROR`). The column itself is
> `VARCHAR(50)` for cross-dialect portability.

### Constraints & Indexes

| Name | Type | Definition |
|---|---|---|
| (implicit PK) | PRIMARY KEY | `call_id` |
| `idx_api_calls_source_timestamp` | INDEX | `(source_id, timestamp)` |
| `idx_api_calls_status` | INDEX | `(status)` |

### Foreign Keys

| Column | References | On Delete |
|---|---|---|
| `source_id` | `api_sources.source_id` | `RESTRICT` |

### Relationships

| Attribute | Target | Type | `back_populates` |
|---|---|---|---|
| `source` | `ApiSource` | many-to-one | `ApiSource.api_calls` |

---

## 5. `data_quality_metrics`

**ORM class:** `DataQualityMetric`
**Type:** Tracking table
**Purpose:** Per-record results of data-quality checks run against
`exchange_rates` rows. Multiple rows can exist per `rate_id` (one per check
name). Automatically cleaned up when the parent rate is deleted.

### Columns

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `metric_id` | `BIGINT`¹ | No | autoincrement | Surrogate primary key. |
| `rate_id` | `BIGINT`¹ | No | — | FK → `exchange_rates.rate_id`. |
| `check_name` | `VARCHAR(100)` | No | — | `NULL_CHECK` \| `RANGE_CHECK` \| `ANOMALY_CHECK`. |
| `check_passed` | `BOOLEAN` | No | — | `true` if the check passed. |
| `anomaly_score` | `NUMERIC(3, 2)` | Yes | `NULL` | Anomaly severity, `0.00`–`1.00`. `NULL` for non-anomaly checks. |
| `created_at` | `TIMESTAMPTZ` | No | `now()` | Row-creation timestamp (UTC). |

¹ `BigInteger().with_variant(Integer, "sqlite")` — see footnote in §3. Applied
to both `metric_id` and the `rate_id` FK so the FK type matches its
referenced column on every dialect.

### Constraints & Indexes

| Name | Type | Definition |
|---|---|---|
| (implicit PK) | PRIMARY KEY | `metric_id` |
| `ck_data_quality_anomaly_score_range` | CHECK | `anomaly_score IS NULL OR (anomaly_score >= 0.00 AND anomaly_score <= 1.00)` |
| `idx_data_quality_rate_check` | INDEX | `(rate_id, check_name)` |
| `idx_data_quality_check_passed` | INDEX | `(check_passed)` |

### Foreign Keys

| Column | References | On Delete |
|---|---|---|
| `rate_id` | `exchange_rates.rate_id` | `CASCADE` |

### Relationships

| Attribute | Target | Type | `back_populates` |
|---|---|---|---|
| `exchange_rate` | `ExchangeRate` | many-to-one | `ExchangeRate.quality_metrics` |

---

## 6. `daily_snapshots`

**ORM class:** `DailySnapshot`
**Type:** Pre-aggregated / materialized table
**Purpose:** OHLCV-style daily summaries per currency pair, populated by the
dbt `marts` layer or a scheduled aggregation job. Avoids scanning
`exchange_rates` for dashboard queries.

### Columns

| Column | Type | Nullable | Default | Description |
|---|---|---|---|---|
| `snapshot_id` | `BIGINT`¹ | No | autoincrement | Surrogate primary key. |
| `snapshot_date` | `DATE` | No | — | Calendar date covered by this snapshot. |
| `from_currency_id` | `INTEGER` | No | — | FK → `currencies.currency_id` (base). |
| `to_currency_id` | `INTEGER` | No | — | FK → `currencies.currency_id` (quote). |
| `rate_open` | `NUMERIC(12, 6)` | No | — | Opening rate for the day. |
| `rate_high` | `NUMERIC(12, 6)` | No | — | Highest rate for the day. |
| `rate_low` | `NUMERIC(12, 6)` | No | — | Lowest rate for the day. |
| `rate_close` | `NUMERIC(12, 6)` | No | — | Closing rate for the day. |
| `rate_avg` | `NUMERIC(12, 6)` | No | — | Average rate across all observations for the day. |
| `rate_ma7` | `NUMERIC(12, 6)` | Yes | `NULL` | 7-day simple moving average. `NULL` for the first 6 days of history. |
| `pct_change` | `NUMERIC(6, 3)` | Yes | `NULL` | Percent change vs. previous trading day's close. |
| `is_anomaly` | `BOOLEAN` | No | `false` | `true` if the daily movement triggered an anomaly alert. |
| `anomaly_level` | `VARCHAR(50)` | No | `'NORMAL'` | `NORMAL` \| `WARNING` \| `CRITICAL`. |
| `created_at` | `TIMESTAMPTZ` | No | `now()` | Row-creation timestamp (UTC). |
| `updated_at` | `TIMESTAMPTZ` | No | `now()`, refreshed `onupdate` | Last-update timestamp (UTC). |

¹ `BigInteger().with_variant(Integer, "sqlite")` — see footnote in §3.

> **Anomaly level enum:** validated at the Python level via
> `AnomalyLevelEnum` (`NORMAL`, `WARNING`, `CRITICAL`). The column itself is
> `VARCHAR(50)` for cross-dialect portability.

### Constraints & Indexes

| Name | Type | Definition |
|---|---|---|
| (implicit PK) | PRIMARY KEY | `snapshot_id` |
| `ck_daily_snapshots_high_gte_low` | CHECK | `rate_high >= rate_low` |
| `ck_daily_snapshots_rates_positive` | CHECK | `rate_open > 0 AND rate_high > 0 AND rate_low > 0 AND rate_close > 0` |
| `uq_daily_snapshots_date_pair` | UNIQUE | `(snapshot_date, from_currency_id, to_currency_id)` |
| `idx_daily_snapshots_date_pair` | INDEX | `(snapshot_date, from_currency_id, to_currency_id)` |
| `idx_daily_snapshots_is_anomaly` | INDEX | `(is_anomaly)` |

### Foreign Keys

| Column | References | On Delete |
|---|---|---|
| `from_currency_id` | `currencies.currency_id` | `RESTRICT` |
| `to_currency_id` | `currencies.currency_id` | `RESTRICT` |

### Relationships

This model has no declared ORM `relationship()` attributes — it is treated
as a read-mostly reporting table, joined ad hoc via `from_currency_id` /
`to_currency_id` where needed.

---

## 7. Entity Relationship Diagram

```
                       +------------------+
                       |    currencies     |
                       | ----------------- |
                       | PK currency_id     |
                       | UQ code            |
                       +---------+----------+
                 +--------------------+--------------------+
                 | from/to FK         |          from/to FK |
                 v (RESTRICT)         |                      v (RESTRICT)
       +-------------------+          |           +-------------------+
       |  exchange_rates    |          |           |  daily_snapshots   |
       | ------------------ |          |           | ------------------ |
       | PK rate_id         |          |           | PK snapshot_id     |
       | FK from_currency_id|----------+           | FK from_currency_id|
       | FK to_currency_id  |                       | FK to_currency_id  |
       | FK source_id ------+-------+               | UQ (date, pair)    |
       | UQ (pair,ts,src)   |       |               +-------------------+
       +---------+----------+       |
                 | CASCADE           | RESTRICT
                 v                   v
   +--------------------------+   +-------------------+
   | data_quality_metrics       |   |    api_sources     |
   | -------------------------- |   | ------------------- |
   | PK metric_id               |   | PK source_id        |
   | FK rate_id (CASCADE)       |   | UQ source_name      |
   +--------------------------+   +---------+----------+
                                              | RESTRICT
                                              v
                                    +-------------------+
                                    |     api_calls       |
                                    | ------------------- |
                                    | PK call_id          |
                                    | FK source_id        |
                                    | (insert-only)       |
                                    +-------------------+
```

---

## 8. Cross-cutting conventions

### Timestamp mixin

`Currency`, `ApiSource`, `ExchangeRate`, and `DailySnapshot` all inherit
`TimestampMixin`, which adds:

- `created_at TIMESTAMPTZ NOT NULL DEFAULT now()`
- `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`, refreshed via `onupdate=func.now()`

`ApiCall` and `DataQualityMetric` are **insert-only / append-only** tables and
therefore have `created_at` but **not** `updated_at`.

### Soft-delete pattern

`Currency.is_active` and `ApiSource.is_active` allow disabling a row without
violating `RESTRICT` foreign keys from `exchange_rates`, `api_calls`, or
`daily_snapshots`.

### SQLite compatibility shims

All `BigInteger` primary keys (and the FK in `data_quality_metrics.rate_id`)
use:

```python
BigInteger().with_variant(Integer, "sqlite")
```

so that SQLite — which only autoincrements columns declared as exactly
`INTEGER PRIMARY KEY` — can still generate PK values during unit tests, while
PostgreSQL retains the full `BIGINT` range in production.

The engine factory (`get_engine()`) also enables `PRAGMA foreign_keys=ON` for
every SQLite connection, since SQLite disables FK enforcement by default.

### Enum-like columns

Two columns are constrained by Python-side `Enum` classes but stored as
`VARCHAR` for portability:

| Column | Python Enum | Allowed values |
|---|---|---|
| `api_calls.status` | `ApiCallStatusEnum` | `SUCCESS`, `TIMEOUT`, `RATE_LIMIT`, `ERROR` |
| `daily_snapshots.anomaly_level` | `AnomalyLevelEnum` | `NORMAL`, `WARNING`, `CRITICAL` |