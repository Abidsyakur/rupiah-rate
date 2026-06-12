# Phase 2: Database Schema Setup & PostgreSQL Models

## Feature Description
Implement persistent data storage layer for exchange rates using PostgreSQL with SQLAlchemy ORM and Alembic migrations. This feature creates the foundation for data integrity, audit trails, and analytics queries.

## Context
Phase 1 (ADR-001) successfully implemented exchange rate extraction from Yfinance and FRED APIs. Phase 2 must provide a robust database schema to:
- Store extracted exchange rates reliably
- Track data quality metrics
- Maintain API call logs for monitoring
- Support idempotent upserts (avoid duplicates)
- Enable historical analysis

## Requirements

### 1. Database Schema (SQLAlchemy Models)
- **exchange_rates**: Core fact table
  - rate_id (BIGSERIAL, PK)
  - timestamp (TIMESTAMPTZ) - market time
  - currency_pair (VARCHAR 20) - e.g., USD_IDR
  - rate (DECIMAL 20,6) - exchange rate value
  - source (VARCHAR 50) - yfinance, fred, etc.
  - fetched_at (TIMESTAMPTZ) - when we got it
  - data_quality_score (DECIMAL 3,2) - 0.0-1.0
  - created_at, updated_at (audit trail)
  - Constraints: rate > 0, unique(currency_pair, timestamp, source)

- **currencies**: Dimension table
  - currency_id (BIGSERIAL, PK)
  - code (VARCHAR 3) - IDR, USD, EUR, etc.
  - name (VARCHAR 100)
  - is_active (BOOLEAN)

- **api_calls**: Monitoring & audit
  - call_id (BIGSERIAL, PK)
  - source (VARCHAR 50)
  - pairs_requested (TEXT) - JSON array
  - status (VARCHAR 20) - success, failed, partial
  - error_message (TEXT)
  - latency_ms (INTEGER)
  - timestamp (TIMESTAMPTZ)

- **data_quality_logs**: Quality tracking
  - log_id (BIGSERIAL, PK)
  - rate_id (FK)
  - quality_score (DECIMAL 3,2)
  - validation_status (VARCHAR 50) - in_bounds, out_of_bounds, etc.
  - timestamp (TIMESTAMPTZ)

### 2. Indexes & Constraints
```
exchange_rates:
  - PK: rate_id
  - UNIQUE: (currency_pair, timestamp, source)
  - INDEX: (currency_pair, timestamp DESC)
  - INDEX: (fetched_at DESC)
  - CHECK: rate > 0 AND rate < 100000
  - FK: currency_pair -> currencies.code

api_calls:
  - INDEX: (source, timestamp DESC)
  - INDEX: (status, timestamp DESC)

data_quality_logs:
  - INDEX: (rate_id)
  - INDEX: (timestamp DESC)
  - FK: rate_id -> exchange_rates.rate_id
```

### 3. SQLAlchemy ORM Models
- Base model with audit fields (created_at, updated_at)
- Type hints for all fields
- Relationships between models
- Repr and str methods for debugging

### 4. Alembic Migrations
- Initial schema migration
- Sample data migration (optional)
- Rollback support
- Migration versioning

### 5. Database Utilities
- Connection manager (DatabaseManager)
- Session factory
- Health check endpoint
- Seed/fixture data loader

### 6. Testing & Documentation
- Unit tests: 85%+ coverage
  - Model creation/validation
  - Constraint enforcement
  - Foreign key relationships
  - Audit trail functionality
- Integration tests
  - Real DB operations (SQLite for tests)
  - Transaction rollback
  - Concurrent access
- Documentation
  - SCHEMA.md with ER diagram
  - Migration guide
  - API reference

## Acceptance Criteria
- [x] SQLAlchemy models defined with proper types
- [x] Alembic migration scripts created
- [x] All tables created with constraints/indexes
- [x] Relationships tested and working
- [x] Unit tests 85%+ coverage
- [x] Integration tests pass
- [x] Sample data loader works
- [x] SCHEMA.md documentation complete
- [x] Database health check utility
- [x] All tests pass in CI/CD

## Dependencies
- **Depends on**: ADR-001 (Phase 1 extraction complete)
- **Blocks**: Phase 3 (Analytics & transformations)
- **Related files**:
  - src/etl/loaders.py (uses these models)
  - tests/integration/test_pipeline.py (integration tests)

## Implementation Details

### Models Location
```
src/
├── models/
│   ├── __init__.py
│   ├── base.py           - Base model with audit fields
│   ├── exchange_rates.py - Core fact table
│   ├── currencies.py     - Dimension table
│   ├── api_calls.py      - Monitoring
│   └── quality_logs.py   - Quality tracking
```

### Migrations Location
```
migrations/
├── versions/
│   ├── 001_init_schema.py
│   └── 002_sample_data.py
└── env.py
```

## Technical Decisions

### Why SQLAlchemy?
- ✅ Type-safe ORM
- ✅ Works with Alembic
- ✅ Good migration support
- ✅ Relationships & constraints

### Why Alembic?
- ✅ Python-based migrations
- ✅ Version control friendly
- ✅ Supports rollbacks
- ✅ Integrates with SQLAlchemy

### Idempotent Upserts
Using PostgreSQL `INSERT ... ON CONFLICT DO UPDATE`:
```sql
INSERT INTO exchange_rates (...)
VALUES (...)
ON CONFLICT (currency_pair, timestamp, source)
DO UPDATE SET
  rate = EXCLUDED.rate,
  data_quality_score = EXCLUDED.data_quality_score,
  updated_at = NOW();
```

## Testing Strategy

### Unit Tests
- Model instantiation
- Field validation
- Relationship loading
- to_dict() serialization

### Integration Tests
- Create/read/update/delete operations
- Foreign key constraints
- Transaction handling
- Concurrent writes

### Data Quality Tests
- Constraint enforcement (rate > 0)
- Audit trail (created_at always set)
- Unique constraint on (pair, timestamp, source)

## Performance Considerations
- Indexes on frequently queried columns (pair, timestamp)
- Batch insert optimization for daily pipeline
- Connection pooling (pool_size=5, max_overflow=10)
- Query execution time < 100ms for typical queries

## Rollout Plan
1. Create models in src/models/
2. Create migrations
3. Test locally with SQLite
4. Test with PostgreSQL
5. Run integration tests
6. Deploy to staging
7. Verify with live data

## Success Metrics
- ✅ All tests pass (85%+ coverage)
- ✅ Can insert 1000 rates in < 1 second
- ✅ Constraints prevent invalid data
- ✅ Audit trail captures all changes
- ✅ Zero duplicate rates (unique constraint)
- ✅ Query response < 100ms

## Phase 3 Blockers
- Phase 2 tests must pass before Phase 3 starts
- Database must handle concurrent writes
- Audit trail must be complete

---

**Labels**: `enhancement`, `database`, `phase-2`  
**Depends on**: #ADR-001  
**Blocks**: Phase 3 (Analytics)  
**Priority**: High  
**Estimated**: 3-5 days  
**Owner**: @Abidsyakur
