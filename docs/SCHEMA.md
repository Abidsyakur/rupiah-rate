# ADR-002: Database Schema Design

## Status
PROPOSED (for review in PR #003)

## Context
Need to design PostgreSQL schema for storing exchange rates, API call logs, and data quality metrics.

## Decision
Create normalized schema with:
1. **currencies table** - Reference data (USD, IDR, EUR, etc)
2. **exchange_rates table** - Main fact table (rates)
3. **api_sources table** - Track which APIs we use
4. **api_calls table** - Audit trail of API calls
5. **data_quality_metrics table** - Quality scores per record
6. **daily_snapshots table** - Pre-aggregated daily data

## Schema Design

### currencies (Dimension)
currency_id (PK): Integer
code (UK): VARCHAR(3) - USD, IDR, EUR, etc
name: VARCHAR(255)
is_active: Boolean
created_at: Timestamp
updated_at: Timestamp

### api_sources (Configuration)
source_id (PK): Integer
source_name (UK): VARCHAR(100) - yfinance, FRED API, BI, etc
api_endpoint: VARCHAR(500)
retry_strategy: VARCHAR(100)
rate_limit: Integer (requests per hour)
is_active: Boolean
created_at: Timestamp
updated_at: Timestamp

### exchange_rates (Fact Table)
rate_id (PK): BigInteger (auto-increment)
from_currency_id (FK): Integer → currencies.currency_id
to_currency_id (FK): Integer → currencies.currency_id
rate: DECIMAL(12,6) - NOT NULL, CHECK > 0
timestamp: Timestamp - NOT NULL
source_id (FK): Integer → api_sources.source_id
data_quality_score: DECIMAL(3,2) - 0.00 to 1.00
is_valid: Boolean - default true
created_at: Timestamp
updated_at: Timestamp
Indexes:

(from_currency_id, to_currency_id, timestamp) - for queries
timestamp - for time-series analysis
source_id - for data source tracking
is_valid - for filtering bad data

Constraints:

rate > 0
timestamp NOT NULL
from_currency_id != to_currency_id

### api_calls (Audit Trail)
call_id (PK): BigInteger
source_id (FK): Integer → api_sources.source_id
timestamp: Timestamp
status: VARCHAR(50) - SUCCESS, TIMEOUT, RATE_LIMIT, ERROR
error_message: TEXT
records_fetched: Integer
records_valid: Integer
records_invalid: Integer
execution_time_ms: Integer
created_at: Timestamp
Indexes:
(source_id, timestamp) - for monitoring
status - for finding errors

### data_quality_metrics (Tracking)
metric_id (PK): BigInteger
rate_id (FK): BigInteger → exchange_rates.rate_id
check_name: VARCHAR(100) - NULL_CHECK, RANGE_CHECK, ANOMALY_CHECK
check_passed: Boolean
anomaly_score: DECIMAL(3,2) - 0.00 to 1.00
created_at: Timestamp
Indexes:

(rate_id, check_name)
check_passed - for finding failures

### daily_snapshots (Materialized)
snapshot_id (PK): BigInteger
snapshot_date: DATE (UK)
from_currency_id (FK): Integer
to_currency_id (FK): Integer
rate_open: DECIMAL(12,6)
rate_high: DECIMAL(12,6)
rate_low: DECIMAL(12,6)
rate_close: DECIMAL(12,6)
rate_avg: DECIMAL(12,6)
rate_ma7: DECIMAL(12,6) - 7-day moving average
pct_change: DECIMAL(6,3) - percent change from previous day
is_anomaly: Boolean
anomaly_level: VARCHAR(50) - NORMAL, WARNING, CRITICAL
created_at: Timestamp
updated_at: Timestamp
Indexes:

(snapshot_date, from_currency_id, to_currency_id) UK
is_anomaly - for alerting

## Rationale
1. **Normalized design** - avoids data duplication
2. **Audit trails** - tracks every API call
3. **Quality tracking** - each record has quality score
4. **Performance indexes** - optimized for common queries
5. **Constraints** - enforces data integrity
6. **Timestamps** - enables time-travel queries

## Alternatives Considered
- NoSQL (MongoDB) - decided: we need ACID guarantees
- Single table (denormalized) - decided: need flexibility for new sources
- No indexes - decided: would kill performance on 1M+ records

## Migration Strategy
Use Alembic for versioned migrations:
- Initial creation: migration 001_create_schema.py
- Future changes tracked and reversible
- Test migrations in CI/CD