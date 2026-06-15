"""
src/etl/extractors.py
=====================
Exchange rate extractors for Yfinance and FRED API.

Architecture Reference: ADR-001
  - Exponential backoff retry strategy (max 3 attempts)
  - Connection timeout: 10s, Read timeout: 30s
  - Supported pairs: USD_IDR, EUR_IDR, SGD_IDR, JPY_IDR
  - Sources: yfinance (all 4 pairs), FRED (USD_IDR, EUR_IDR only)
"""

from __future__ import annotations

import logging
import os
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar

import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants (mirrors config/dev.yaml)
# ---------------------------------------------------------------------------

RETRY_CONFIG: Dict[str, Any] = {
    "max_attempts": 3,
    "base_delay": 1.0,       # seconds
    "max_delay": 30.0,       # seconds
    "exponential_base": 2,
    "jitter": True,
}

TIMEOUT = (10, 30)  # (connect, read) seconds

# Yfinance ticker symbols for IDR pairs
YFINANCE_TICKER_MAP: Dict[str, str] = {
    "USD_IDR": "USDIDR=X",
    "EUR_IDR": "EURIDR=X",
    "SGD_IDR": "SGDIDR=X",
    "JPY_IDR": "JPYIDR=X",
}

# FRED series IDs for IDR pairs.
#
# IMPORTANT: FRED does not publish a direct EUR/IDR series. The mapping below
# only includes USD_IDR (FRED series "DEXINUS" = Indonesian Rupiahs to One
# U.S. Dollar, daily, noon buying rate from the Federal Reserve). EUR_IDR is
# intentionally NOT mapped — requesting it from FREDExtractor will be skipped
# as "unsupported" (see SUPPORTED_PAIRS / _filter_supported_pairs).
#
# If EUR_IDR via FRED becomes a hard requirement, it must be derived
# (e.g. EUR_USD * USD_IDR) — that is out of scope for this extractor, which
# maps 1 pair -> 1 FRED series.
FRED_SERIES_MAP: Dict[str, str] = {
    "USD_IDR": "CCUSMA02IDM618N",   # Indonesian Rupiahs to One U.S. Dollar (daily)
}

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Validation bounds per pair  (rate > 0 AND rate < 100_000 as per spec)
RATE_BOUNDS: Dict[str, tuple[float, float]] = {
    "USD_IDR": (10_000.0, 25_000.0),
    "EUR_IDR": (10_000.0, 30_000.0),
    "SGD_IDR": (8_000.0,  20_000.0),
    "JPY_IDR": (50.0,     250.0),
}

DEFAULT_BOUNDS = (0.0, 100_000.0)  # fallback for unknown pairs

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ExchangeRate:
    """Represents a single exchange rate observation."""

    pair: str
    rate: float
    timestamp: datetime
    source: str
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data_quality_score: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dictionary (matches the pipeline's response format)."""
        return {
            "pair": self.pair,
            "rate": self.rate,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "fetched_at": self.fetched_at.isoformat(),
            "data_quality_score": self.data_quality_score,
        }


@dataclass
class ExtractionResult:
    """Aggregated result returned by fetch_rates()."""

    rates: List[ExchangeRate]
    fetched_at: datetime
    source: str
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to the canonical response format defined in ADR-001."""
        return {
            "rates": [r.to_dict() for r in self.rates],
            "fetched_at": self.fetched_at.isoformat(),
            "source": self.source,
            "errors": self.errors,
        }


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------

F = TypeVar("F", bound=Callable[..., Any])


def with_retry(
    max_attempts: int = RETRY_CONFIG["max_attempts"],
    base_delay: float = RETRY_CONFIG["base_delay"],
    max_delay: float = RETRY_CONFIG["max_delay"],
    exponential_base: int = RETRY_CONFIG["exponential_base"],
    jitter: bool = RETRY_CONFIG["jitter"],
    retryable_exceptions: tuple[type[Exception], ...] = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
    ),
) -> Callable[[F], F]:
    """
    Decorator that retries a function with exponential backoff.

    Parameters
    ----------
    max_attempts:
        Maximum number of total attempts (first call + retries).
    base_delay:
        Initial wait time in seconds before the first retry.
    max_delay:
        Upper cap on wait time between retries.
    exponential_base:
        Multiplier applied to the delay after each failed attempt.
    jitter:
        If True, adds random noise to avoid thundering-herd problems.
    retryable_exceptions:
        Exception types that trigger a retry; all others propagate immediately.

    Example
    -------
    >>> @with_retry(max_attempts=3)
    ... def call_api():
    ...     ...
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        logger.error(
                            "All %d attempts exhausted for %s: %s",
                            max_attempts,
                            func.__qualname__,
                            exc,
                        )
                        raise

                    delay = min(base_delay * (exponential_base ** (attempt - 1)), max_delay)
                    if jitter:
                        delay += random.uniform(0, delay * 0.2)

                    logger.warning(
                        "Attempt %d/%d failed for %s (%s). Retrying in %.2fs…",
                        attempt,
                        max_attempts,
                        func.__qualname__,
                        exc,
                        delay,
                    )
                    time.sleep(delay)

            # Should never reach here, but satisfies type checkers
            raise last_exc  # type: ignore[misc]

        return wrapper  # type: ignore[return-value]

    return decorator


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_rate(pair: str, rate: Any) -> tuple[float, float]:
    """
    Validate and score a raw rate value.

    Parameters
    ----------
    pair:
        Currency pair identifier, e.g. "USD_IDR".
    rate:
        Raw value from the API response.

    Returns
    -------
    (validated_rate, quality_score)
        validated_rate – float value that passed all checks.
        quality_score  – float in [0, 1]; 1.0 = fully valid.

    Raises
    ------
    ValueError
        If `rate` is None, non-numeric, non-positive, or outside expected bounds.
    """
    if rate is None:
        raise ValueError(f"[{pair}] Rate is None.")

    try:
        rate_float = float(rate)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"[{pair}] Non-numeric rate value: {rate!r}") from exc

    if rate_float <= 0:
        raise ValueError(f"[{pair}] Rate must be positive, got {rate_float}.")

    if rate_float >= 100_000:
        raise ValueError(f"[{pair}] Rate exceeds absolute maximum (100 000): {rate_float}.")

    lo, hi = RATE_BOUNDS.get(pair, DEFAULT_BOUNDS)
    if not (lo <= rate_float <= hi):
        logger.warning(
            "[%s] Rate %.4f is outside expected range [%.0f, %.0f]. Flagging quality.",
            pair,
            rate_float,
            lo,
            hi,
        )
        quality_score = 0.6
    else:
        quality_score = 1.0

    return rate_float, quality_score


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class ExchangeRateExtractor(ABC):
    """
    Abstract base for exchange-rate extractors.

    All concrete extractors must implement :meth:`fetch_rates` and
    :meth:`get_source_name`.  The base class exposes the validated
    :attr:`SUPPORTED_PAIRS` contract and shared logging utilities.
    """

    #: Pairs this extractor can serve. Override in subclasses.
    SUPPORTED_PAIRS: List[str] = []

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_rates(self, pairs: List[str]) -> Dict[str, Any]:
        """
        Fetch exchange rates for the requested currency pairs.

        Parameters
        ----------
        pairs:
            List of currency pair identifiers, e.g. ["USD_IDR", "EUR_IDR"].
            Unknown or unsupported pairs are skipped with a warning.

        Returns
        -------
        dict
            Matches the canonical response schema from ADR-001::

                {
                    "rates": [
                        {"pair": "USD_IDR", "rate": 18176.50,
                         "timestamp": "2025-01-15T10:30:00Z", ...},
                        ...
                    ],
                    "fetched_at": "2025-01-15T10:30:05Z",
                    "source": "<source_name>",
                    "errors": ["<error messages for any failed pairs>"]
                }
        """

    @abstractmethod
    def get_source_name(self) -> str:
        """Return the canonical source identifier, e.g. 'yfinance' or 'fred'."""

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    def _filter_supported_pairs(self, pairs: List[str]) -> List[str]:
        """Return only the pairs this extractor supports, logging any skipped ones."""
        supported, skipped = [], []
        for pair in pairs:
            if pair in self.SUPPORTED_PAIRS:
                supported.append(pair)
            else:
                skipped.append(pair)
        if skipped:
            logger.warning(
                "[%s] Skipping unsupported pairs: %s",
                self.get_source_name(),
                skipped,
            )
        return supported


# ---------------------------------------------------------------------------
# Yfinance extractor
# ---------------------------------------------------------------------------

class YFinanceExtractor(ExchangeRateExtractor):
    """
    Extracts exchange rates using the ``yfinance`` library.

    Supports all four IDR pairs defined in ADR-001:
    ``USD_IDR``, ``EUR_IDR``, ``SGD_IDR``, ``JPY_IDR``.

    Example
    -------
    >>> extractor = YFinanceExtractor()
    >>> result = extractor.fetch_rates(["USD_IDR", "EUR_IDR"])
    >>> result["source"]
    'yfinance'
    """

    SUPPORTED_PAIRS: List[str] = list(YFINANCE_TICKER_MAP.keys())

    def get_source_name(self) -> str:
        return "yfinance"

    @with_retry()
    def _fetch_single_pair(self, pair: str) -> ExchangeRate:
        """
        Download the latest tick for one currency pair via yfinance.

        Parameters
        ----------
        pair:
            Currency pair identifier, e.g. "USD_IDR".

        Returns
        -------
        ExchangeRate
            Validated rate object.

        Raises
        ------
        ValueError
            If the ticker returns no data or the rate fails validation.
        requests.exceptions.Timeout
            Propagated from yfinance's underlying HTTP call if timeout is hit.
        """
        ticker_symbol = YFINANCE_TICKER_MAP[pair]
        logger.debug("[yfinance] Fetching %s → %s", pair, ticker_symbol)

        ticker = yf.Ticker(ticker_symbol)
        # fast=True returns only last price data
        info = ticker.fast_info

        raw_rate = getattr(info, "last_price", None)
        if raw_rate is None:
            # Fall back to history if fast_info is unavailable
            hist = ticker.history(period="1d")
            if hist.empty:
                raise ValueError(
                    f"[yfinance] No data returned for ticker {ticker_symbol} ({pair})."
                )
            raw_rate = float(hist["Close"].iloc[-1])
            ts = hist.index[-1].to_pydatetime().replace(tzinfo=timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        rate_float, quality = validate_rate(pair, raw_rate)
        logger.info(
            "[yfinance] %s = %.4f  (quality=%.2f)",
            pair,
            rate_float,
            quality,
        )
        return ExchangeRate(
            pair=pair,
            rate=rate_float,
            timestamp=ts,
            source=self.get_source_name(),
            data_quality_score=quality,
        )

    def fetch_rates(self, pairs: List[str]) -> Dict[str, Any]:
        """
        Fetch the latest exchange rates from Yahoo Finance.

        Parameters
        ----------
        pairs:
            Subset of ``SUPPORTED_PAIRS`` to fetch.

        Returns
        -------
        dict
            Canonical response dict (see :meth:`ExchangeRateExtractor.fetch_rates`).
        """
        fetched_at = datetime.now(timezone.utc)
        valid_pairs = self._filter_supported_pairs(pairs)

        results: List[ExchangeRate] = []
        errors: List[str] = []

        for pair in valid_pairs:
            try:
                exchange_rate = self._fetch_single_pair(pair)
                results.append(exchange_rate)
            except ValueError as exc:
                msg = f"[yfinance] Validation error for {pair}: {exc}"
                logger.error(msg)
                errors.append(msg)
            except Exception as exc:  # noqa: BLE001
                msg = f"[yfinance] Unexpected error for {pair}: {exc}"
                logger.exception(msg)
                errors.append(msg)

        return ExtractionResult(
            rates=results,
            fetched_at=fetched_at,
            source=self.get_source_name(),
            errors=errors,
        ).to_dict()


# ---------------------------------------------------------------------------
# FRED extractor
# ---------------------------------------------------------------------------

class FREDExtractor(ExchangeRateExtractor):
    """
    Extracts monthly/annual exchange rate aggregates from the Federal
    Reserve FRED API.

    Project decision: **yfinance covers daily rates**; **FRED covers
    monthly/annual aggregates** for longer-horizon trend analysis. This
    extractor therefore requests FRED's ``frequency``-aggregated endpoint
    (default ``frequency="m"``, ``aggregation_method="avg"``) rather than the
    raw daily series.

    Supports ``USD_IDR`` only — the sole IDR pair FRED publishes directly
    (underlying daily series ``DEXINUS``, aggregated to monthly/annual here).
    ``EUR_IDR`` is not available from FRED and will be skipped with a warning
    if requested.

    Configuration
    -------------
    Set the ``FRED_API_KEY`` environment variable before instantiating.

    Example
    -------
    >>> extractor = FREDExtractor()  # defaults to monthly averages
    >>> result = extractor.fetch_rates(["USD_IDR"])
    >>> result["source"]
    'fred'

    >>> annual = FREDExtractor(frequency="a", aggregation_method="eop")
    >>> annual.fetch_rates(["USD_IDR"])
    """

    SUPPORTED_PAIRS: List[str] = list(FRED_SERIES_MAP.keys())

    def __init__(
        self,
        api_key: Optional[str] = None,
        frequency: str = "m",
        aggregation_method: str = "avg",
    ) -> None:
        """
        Parameters
        ----------
        api_key:
            FRED API key.  Falls back to the ``FRED_API_KEY`` environment
            variable when not supplied directly.
        frequency:
            FRED ``frequency`` aggregation code applied to the underlying
            daily series. Per project decision, **FRED is used for
            monthly/annual aggregates** while yfinance covers daily rates.
            Common values: ``"m"`` (monthly, default), ``"q"`` (quarterly),
            ``"a"`` (annual). See:
            https://fred.stlouisfed.org/docs/api/fred/series_observations.html
        aggregation_method:
            How FRED aggregates the daily series into the requested
            ``frequency``. One of ``"avg"`` (default), ``"sum"``, ``"eop"``
            (end-of-period). For exchange rates, ``"avg"`` or ``"eop"`` are
            most meaningful.

        Raises
        ------
        EnvironmentError
            If no API key is available from either source.
        """
        self._api_key = api_key or os.getenv("FRED_API_KEY")
        if not self._api_key:
            raise EnvironmentError(
                "FRED API key is required. "
                "Set the FRED_API_KEY environment variable or pass api_key= to FREDExtractor()."
            )
        self._frequency = frequency
        self._aggregation_method = aggregation_method
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    def get_source_name(self) -> str:
        return "fred"

    @with_retry(
        retryable_exceptions=(
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
        )
    )
    def _fetch_single_pair(self, pair: str) -> ExchangeRate:
        """
        Retrieve the most recent observation for one currency pair from FRED.

        Parameters
        ----------
        pair:
            Currency pair identifier, e.g. "USD_IDR".

        Returns
        -------
        ExchangeRate
            Validated rate object.

        Raises
        ------
        requests.exceptions.HTTPError
            On 4xx/5xx responses (triggering retry for 5xx, propagated for 4xx).
        ValueError
            On missing or invalid rate data.
        """
        series_id = FRED_SERIES_MAP[pair]
        logger.debug("[fred] Fetching %s → series %s", pair, series_id)

        params = {
            "series_id": series_id,
            "api_key": self._api_key,
            "file_type": "json",
            "sort_order": "desc",
            # Fetch a few recent periods: the most recent one is often "."
            # because the current (incomplete) month/quarter/year hasn't
            # finished yet, so FRED has nothing to aggregate. We pick the
            # first period that actually has a value.
            "limit": 5,
            "frequency": self._frequency,
            "aggregation_method": self._aggregation_method,
        }

        response = self._session.get(
            FRED_BASE_URL,
            params=params,
            timeout=TIMEOUT,
        )

        # Handle rate limiting explicitly before generic raise_for_status
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            logger.warning(
                "[fred] Rate limited on %s. Waiting %ds (Retry-After).",
                pair,
                retry_after,
            )
            time.sleep(retry_after)
            response = self._session.get(FRED_BASE_URL, params=params, timeout=TIMEOUT)

        response.raise_for_status()

        try:
            payload = response.json()
        except ValueError as exc:
            raise ValueError(
                f"[fred] Invalid JSON in response for series {series_id} ({pair})."
            ) from exc

        observations = payload.get("observations", [])
        if not observations:
            raise ValueError(
                f"[fred] No observations returned for series {series_id} ({pair})."
            )

        # Walk observations newest-first, skipping incomplete-period "."
        # sentinels (e.g. the current month before it has closed).
        latest = None
        raw_value = "."
        for obs in observations:
            val = obs.get("value", ".")
            if val != ".":
                latest = obs
                raw_value = val
                break

        if latest is None:
            newest_date = observations[0].get("date")
            raise ValueError(
                f"[fred] All {len(observations)} most recent observations for "
                f"series {series_id} ({pair}) are missing ('.'). "
                f"Newest attempted date: {newest_date}."
            )

        rate_float, quality = validate_rate(pair, raw_value)

        ts_str: str = latest.get("date", "")
        try:
            ts = datetime.strptime(ts_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            logger.warning("[fred] Could not parse date %r for %s; using now().", ts_str, pair)
            ts = datetime.now(timezone.utc)

        logger.info(
            "[fred] %s = %.4f  date=%s  (quality=%.2f)",
            pair,
            rate_float,
            ts_str,
            quality,
        )
        return ExchangeRate(
            pair=pair,
            rate=rate_float,
            timestamp=ts,
            source=self.get_source_name(),
            data_quality_score=quality,
        )

    def fetch_rates(self, pairs: List[str]) -> Dict[str, Any]:
        """
        Fetch the latest exchange rates from the FRED API.

        Parameters
        ----------
        pairs:
            Subset of ``SUPPORTED_PAIRS`` to fetch.

        Returns
        -------
        dict
            Canonical response dict (see :meth:`ExchangeRateExtractor.fetch_rates`).
        """
        fetched_at = datetime.now(timezone.utc)
        valid_pairs = self._filter_supported_pairs(pairs)

        results: List[ExchangeRate] = []
        errors: List[str] = []

        for pair in valid_pairs:
            try:
                exchange_rate = self._fetch_single_pair(pair)
                results.append(exchange_rate)
            except requests.exceptions.HTTPError as exc:
                msg = f"[fred] HTTP error for {pair}: {exc}"
                logger.error(msg)
                errors.append(msg)
            except ValueError as exc:
                msg = f"[fred] Validation/parse error for {pair}: {exc}"
                logger.error(msg)
                errors.append(msg)
            except Exception as exc:  # noqa: BLE001
                msg = f"[fred] Unexpected error for {pair}: {exc}"
                logger.exception(msg)
                errors.append(msg)

        return ExtractionResult(
            rates=results,
            fetched_at=fetched_at,
            source=self.get_source_name(),
            errors=errors,
        ).to_dict()


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def get_extractor(source: str, **kwargs: Any) -> ExchangeRateExtractor:
    """
    Factory that returns the correct extractor for a given source name.

    Parameters
    ----------
    source:
        ``"yfinance"`` or ``"fred"``.
    **kwargs:
        Forwarded to the extractor's ``__init__`` (e.g. ``api_key`` for FRED).

    Returns
    -------
    ExchangeRateExtractor

    Raises
    ------
    ValueError
        If *source* is not recognised.

    Example
    -------
    >>> extractor = get_extractor("fred", api_key="abc123")
    >>> type(extractor).__name__
    'FREDExtractor'
    """
    registry: Dict[str, type[ExchangeRateExtractor]] = {
        "yfinance": YFinanceExtractor,
        "fred": FREDExtractor,
    }
    if source not in registry:
        raise ValueError(
            f"Unknown extractor source {source!r}. "
            f"Valid options: {sorted(registry.keys())}"
        )
    return registry[source](**kwargs)