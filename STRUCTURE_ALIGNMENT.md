# Project Structure Alignment

## Summary
Struktur project telah disesuaikan dengan **design Anda** yang fokus pada **Exchange Rate Extraction** dengan ADR-001.

## Core Components

### Phase 1: Exchange Rate Extraction (Current)
```
src/etl/
├── __init__.py          ✓ Exports main classes
├── extractors.py        ✓ YFinanceExtractor, FREDExtractor (ADR-001)
├── validators.py        ✓ Data quality validation
├── loaders.py          ✓ Database upsert operations
└── pipeline.py         ✓ Orchestration logic
```

**Key Features:**
- Yfinance & FRED API support (4 currency pairs)
- Exponential backoff retry logic (max 3 attempts)
- Rate bounds validation per pair
- Data freshness checks
- Idempotent database upsert

### Phase 2: Analytics (Planned)
```
src/analytics/
├── trends.py           ✓ Moving averages, trend detection
├── volatility.py       ✓ Standard deviation, Bollinger Bands
├── forecasting.py      ⏳ ARIMA, ML models (Phase 3+)
└── anomalies.py        ⏳ Anomaly detection (Phase 3+)
```

### Supporting Modules
```
src/utils/
├── config.py           ✓ Environment-based configuration
├── logging.py          ✓ Structured logging setup
└── database.py         ✓ PostgreSQL connection management
```

## Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| YFinanceExtractor | ✅ Implemented | With retry & validation |
| FREDExtractor | ✅ Implemented | Placeholder for series IDs |
| validate_rate() | ✅ Implemented | Per-pair bounds |
| with_retry() | ✅ Implemented | Exponential backoff decorator |
| ExchangeRateLoader | ⏳ Partial | Upsert logic needs DB schema |
| ExchangeRateValidator | ✅ Implemented | Quality checks |
| ExtractionPipeline | ✅ Implemented | Orchestration ready |
| Analytics Phase 2 | ⏳ Stub | Basic structure ready |
| Forecasting Phase 3 | ⏳ Stub | Placeholder only |

## Configuration Files

| File | Purpose |
|------|---------|
| `config/dev.yaml` | Development environment |
| `config/staging.yaml` | Staging environment |
| `config/prod.yaml` | Production environment |
| `.env.example` | Environment variables template |

## Database Schema (To Implement)

```sql
CREATE TABLE exchange_rates (
    rate_id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    currency_pair VARCHAR(20) NOT NULL,
    rate DECIMAL(20, 6) NOT NULL,
    source VARCHAR(50) NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    data_quality_score DECIMAL(3, 2),
    UNIQUE(currency_pair, timestamp, source)
);

CREATE INDEX idx_exchange_rates_pair_timestamp 
ON exchange_rates(currency_pair, timestamp DESC);
```

## Supported Currency Pairs

| Pair | Yfinance | FRED | Status |
|------|----------|------|--------|
| USD_IDR | ✅ | ✅ | Active |
| EUR_IDR | ✅ | ✅ | Active |
| SGD_IDR | ✅ | ❌ | Active |
| JPY_IDR | ✅ | ❌ | Active |

## Next Steps

1. **Database Schema**: Create `exchange_rates` table in PostgreSQL
2. **Implement Loaders**: Complete `ExchangeRateLoader.upsert_rates()`
3. **API Keys**: Set `FRED_API_KEY` environment variable
4. **Test Extraction**: Run tests for extractors
5. **Deploy Pipeline**: Configure Airflow DAG for daily runs

## Running the Pipeline

```bash
# Local development
python -m src.main

# With Docker
docker-compose up -d app

# Via Airflow
airflow dags trigger rupiah_pipeline_dag
```

---
**Last Updated**: 2026-06-11  
**Status**: Ready for Phase 1 (Extraction) implementation
