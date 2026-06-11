# Architecture Design: Yfinance and FRED API Rate Extraction

## Architecture Decision Record (ADR-001)

### Status
PROPOSED (waiting for review)

### Context
We need to extract exchange rates from Yfinance and FRED API for pipeline.

### Decision
Use Python requests library with:
- Exponential backoff retry strategy
- Connection timeout: 10 seconds
- Read timeout: 30 seconds
- Max retries: 3

### Rationale
1. Simple, well-tested library
2. Exponential backoff prevents API throttling
3. Timeouts prevent hanging requests
4. Matches our existing error handling patterns

### Consequences
- Adding new dependency (requests)
- Need to handle rate limiting
- Need to monitor API availability

### Alternatives Considered
- Use httpx (async) - decided too complex for initial MVP
- Use urllib (stdlib) - requests has better retry mechanisms

---

## Data Model

### `exchange_rates` table

| Column | Type | Description |
|--------|------|-------------|
| rate_id | UUID / BIGSERIAL | Primary Key |
| timestamp | TIMESTAMPTZ | When the rate was recorded (market time) |
| currency_pair | VARCHAR(20) | e.g., USD_IDR, EUR_IDR, SGD_IDR, JPY_IDR |
| rate | DECIMAL(20, 6) | Exchange rate value |
| source | VARCHAR(50) | API name: 'yfinance' or 'fred' |
| fetched_at | TIMESTAMPTZ | When we fetched the data |
| data_quality_score | DECIMAL(3, 2) | Quality score 0.00 - 1.00 |

### Indexes
- Primary Key: `rate_id`
- Unique constraint: `(currency_pair, timestamp, source)` for idempotent upserts
- Index: `idx_exchange_rates_pair_timestamp` on `(currency_pair, timestamp DESC)`
- Index: `idx_exchange_rates_fetched_at` on `fetched_at`

---

## API Specification

### Internal Extractor Interface

```python
# src/etl/extractors.py
class ExchangeRateExtractor(ABC):
    @abstractmethod
    def fetch_rates(self, pairs: List[str]) -> List[ExchangeRate]:
        pass

    @abstractmethod
    def get_source_name(self) -> str:
        pass
```

### Response Format

```json
{
  "rates": [
    {"pair": "USD_IDR", "rate": 18176.50, "timestamp": "2025-01-15T10:30:00Z"},
    {"pair": "EUR_IDR", "rate": 19234.25, "timestamp": "2025-01-15T10:30:00Z"}
  ],
  "fetched_at": "2025-01-15T10:30:05Z",
  "source": "yfinance"
}
```

### Currency Pairs to Extract
| Pair | Description | Source |
|------|-------------|--------|
| USD_IDR | US Dollar to Indonesian Rupiah | yfinance, FRED |
| EUR_IDR | Euro to Indonesian Rupiah | yfinance, FRED |
| SGD_IDR | Singapore Dollar to Indonesian Rupiah | yfinance |
| JPY_IDR | Japanese Yen to Indonesian Rupiah | yfinance |

---

## Error Handling Strategy

| Error Type | HTTP Code | Strategy |
|------------|-----------|----------|
| Connection timeout | N/A | Retry with exponential backoff (max 3) |
| Read timeout | N/A | Retry with exponential backoff (max 3) |
| Rate limit | 429 | Wait `Retry-After` header, then retry |
| Server error | 5xx | Retry with exponential backoff (max 3) |
| Invalid JSON | 200 | Log error, skip record, continue |
| Invalid rate value | 200 | Validate: rate > 0, rate < 100000, log warning |
| Network error | N/A | Retry up to 3 times with backoff |

### Retry Configuration
```python
RETRY_CONFIG = {
    "max_attempts": 3,
    "base_delay": 1.0,      # seconds
    "max_delay": 30.0,      # seconds
    "exponential_base": 2,
    "jitter": True
}
```

---

## Testing Strategy

### Unit Tests (`tests/unit/test_extractors.py`)
- Mock API responses (success, empty, malformed)
- Test retry logic with simulated failures
- Test data validation (negative rates, null values, outliers)
- Test source-specific parsing logic

### Integration Tests (`tests/integration/test_pipeline.py`)
- Real API calls against staging environment
- End-to-end: extract → validate → load
- Verify idempotent upsert behavior

### Data Quality Tests
- Rate range validation (e.g., USD_IDR: 10,000 - 20,000)
- Timestamp freshness check (not older than 24h for hourly)
- Duplicate detection
- Completeness check (all 4 pairs present)

---

## Implementation Plan

### Phase 1: Core Extractors
1. `YFinanceExtractor` - using `yfinance` library
2. `FREDExtractor` - using FRED API with API key

### Phase 2: Pipeline Integration
1. `ExtractionPipeline` orchestrator
2. Validation layer
3. Database loader with upsert

### Phase 3: Observability
1. Structured logging
2. Metrics (success rate, latency, data quality)
3. Alerting on failures

---

## Configuration

```yaml
# config/dev.yaml
extractors:
  yfinance:
    enabled: true
    pairs: ["USD_IDR", "EUR_IDR", "SGD_IDR", "JPY_IDR"]
    interval_minutes: 60
  fred:
    enabled: true
    pairs: ["USD_IDR", "EUR_IDR"]
    api_key: "${FRED_API_KEY}"
    interval_minutes: 1440  # daily

retry:
  max_attempts: 3
  base_delay_seconds: 1
  max_delay_seconds: 30

timeouts:
  connect: 10
  read: 30
```

---

## Monitoring & Alerting

| Metric | Threshold | Alert |
|--------|-----------|-------|
| Extraction success rate | < 95% | PagerDuty |
| Data freshness | > 2 hours | Slack |
| Data quality score | < 0.80 | Slack |
| API latency (p95) | > 10s | Slack |

---

## Open Questions

1. **Caching**: Should we implement in-memory caching for repeated requests within same minute?
2. **Rate Limits**: Yfinance has unofficial limits; FRED has 120 requests/min. Need token bucket?
3. **Historical Backfill**: Separate job for backfilling historical data?
4. **Timezone Handling**: All timestamps stored as UTC, but market hours vary.