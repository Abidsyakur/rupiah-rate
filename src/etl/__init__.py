"""
src/etl/__init__.py
===================
ETL module for exchange rate extraction and loading.
"""

from src.etl.extractors import (
    DEFAULT_BOUNDS,
    FRED_BASE_URL,
    FRED_SERIES_MAP,
    RATE_BOUNDS,
    RETRY_CONFIG,
    TIMEOUT,
    YFINANCE_TICKER_MAP,
    ExchangeRate,
    ExtractionResult,
    FREDExtractor,
    YFinanceExtractor,
    get_extractor,
    validate_rate,
    with_retry,
)

__all__ = [
    "ExchangeRate",
    "ExtractionResult",
    "YFinanceExtractor",
    "FREDExtractor",
    "get_extractor",
    "validate_rate",
    "with_retry",
    "RETRY_CONFIG",
    "TIMEOUT",
    "YFINANCE_TICKER_MAP",
    "FRED_SERIES_MAP",
    "FRED_BASE_URL",
    "RATE_BOUNDS",
    "DEFAULT_BOUNDS",
]
