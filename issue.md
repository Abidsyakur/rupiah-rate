Title: Extract Exchange Rates from Yfinance and FRED API

Type: Feature
Labels: enhancement, high-priority, data-pipeline

Description:
## Problem
We need real-time exchange rates from Yfinance and FRED API to feed our pipeline.

## Acceptance Criteria
- [ ] Extract USD/IDR, EUR/IDR, SGD/IDR, JPY/IDR
- [ ] Handle API failures (retry logic)
- [ ] Validate data quality
- [ ] Store in database
- [ ] Unit tests (>80% coverage)

## Technical Approach
[Created by AI after architectural discussion]
- Use requests library with retry decorator
- Implement exponential backoff
- Validate rates are positive, non-null
- Idempotent loading (upsert pattern)

## Questions/Notes
- Should we cache rates in-memory?
- What retry strategy for rate limit?