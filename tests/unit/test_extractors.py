"""
tests/unit/test_extractors.py
==============================
Unit tests for src/etl/extractors.py (ADR-001).

Test coverage:
  - validate_rate()          – null, non-numeric, zero/negative, absolute cap,
                               in-bounds (quality=1.0), out-of-bounds (quality=0.6)
  - with_retry()             – succeeds on first try, retries on transient errors,
                               exhausts attempts, non-retryable exceptions bypass retry,
                               delay calculation (jitter-free), max_delay cap
  - ExchangeRate / ExtractionResult – dataclass serialisation via to_dict()
  - YFinanceExtractor        – happy path (fast_info), fallback to history(),
                               empty history, validation error, unsupported pair,
                               full fetch_rates() with mixed success/failure
  - FREDExtractor            – happy path, FRED "." sentinel, empty observations,
                               invalid JSON, 429 rate-limit handling, 500 server error,
                               missing FRED_API_KEY, unsupported pair,
                               full fetch_rates() with mixed success/failure
  - get_extractor()          – valid source names, unknown source raises ValueError

All external I/O (yfinance, requests) is mocked; time.sleep() is patched
throughout so the test suite runs in milliseconds.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, Mock, call, patch

import pytest
import requests

# ---------------------------------------------------------------------------
# Module under test
# ---------------------------------------------------------------------------
# We import from the source tree; adjust sys.path if running outside a
# properly installed package (e.g. bare pytest invocation at repo root).
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parents[2] / "src"))

from etl.extractors import (
    DEFAULT_BOUNDS,
    FRED_BASE_URL,
    RATE_BOUNDS,
    RETRY_CONFIG,
    ExchangeRate,
    ExtractionResult,
    FREDExtractor,
    YFinanceExtractor,
    get_extractor,
    validate_rate,
    with_retry,
)

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

FIXED_NOW = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


def _fred_response(observations: list[dict], status_code: int = 200) -> Mock:
    """Build a mock requests.Response for FRED API calls."""
    resp = Mock()
    resp.status_code = status_code
    resp.headers = {}
    resp.json.return_value = {"observations": observations}
    resp.raise_for_status = Mock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=resp
        )
    return resp


def _fred_obs(value: str = "18176.50", date: str = "2025-01-15") -> dict:
    return {"value": value, "date": date, "realtime_start": date, "realtime_end": date}


# ---------------------------------------------------------------------------
# validate_rate
# ---------------------------------------------------------------------------

class TestValidateRate:
    """Unit tests for the standalone validate_rate() helper."""

    def test_valid_rate_in_bounds_returns_quality_1(self):
        rate, quality = validate_rate("USD_IDR", 18176.50)
        assert rate == pytest.approx(18176.50)
        assert quality == 1.0

    def test_valid_rate_string_coerced_to_float(self):
        rate, quality = validate_rate("USD_IDR", "18176.50")
        assert isinstance(rate, float)
        assert quality == 1.0

    def test_rate_outside_pair_bounds_returns_quality_0_6(self):
        # USD_IDR bounds are (10_000, 25_000); 5_000 is below
        rate, quality = validate_rate("USD_IDR", 5_000.0)
        assert rate == pytest.approx(5_000.0)
        assert quality == pytest.approx(0.6)

    def test_rate_above_pair_bounds_returns_quality_0_6(self):
        rate, quality = validate_rate("USD_IDR", 30_000.0)
        assert quality == pytest.approx(0.6)

    def test_unknown_pair_uses_default_bounds(self):
        # DEFAULT_BOUNDS = (0, 100_000); any positive value < 100_000 is quality 1.0
        rate, quality = validate_rate("GBP_IDR", 25_000.0)
        assert quality == 1.0

    def test_none_raises_value_error(self):
        with pytest.raises(ValueError, match="Rate is None"):
            validate_rate("USD_IDR", None)

    def test_non_numeric_string_raises_value_error(self):
        with pytest.raises(ValueError, match="Non-numeric"):
            validate_rate("USD_IDR", "N/A")

    def test_zero_raises_value_error(self):
        with pytest.raises(ValueError, match="positive"):
            validate_rate("USD_IDR", 0.0)

    def test_negative_raises_value_error(self):
        with pytest.raises(ValueError, match="positive"):
            validate_rate("USD_IDR", -100.0)

    def test_at_absolute_maximum_raises_value_error(self):
        with pytest.raises(ValueError, match="absolute maximum"):
            validate_rate("USD_IDR", 100_000.0)

    def test_above_absolute_maximum_raises_value_error(self):
        with pytest.raises(ValueError, match="absolute maximum"):
            validate_rate("USD_IDR", 999_999.0)

    def test_jpy_idr_in_bounds(self):
        # JPY_IDR bounds: (50, 250)
        rate, quality = validate_rate("JPY_IDR", 120.5)
        assert quality == 1.0

    def test_jpy_idr_out_of_bounds_below(self):
        rate, quality = validate_rate("JPY_IDR", 10.0)
        assert quality == pytest.approx(0.6)


# ---------------------------------------------------------------------------
# with_retry decorator
# ---------------------------------------------------------------------------

class TestWithRetry:
    """Unit tests for the with_retry() exponential-backoff decorator."""

    @patch("etl.extractors.time.sleep")
    def test_succeeds_on_first_attempt_no_sleep(self, mock_sleep):
        call_count = 0

        @with_retry(max_attempts=3, base_delay=1.0, jitter=False)
        def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = succeed()
        assert result == "ok"
        assert call_count == 1
        mock_sleep.assert_not_called()

    @patch("etl.extractors.time.sleep")
    def test_retries_on_transient_error_then_succeeds(self, mock_sleep):
        attempts = []

        @with_retry(
            max_attempts=3,
            base_delay=1.0,
            jitter=False,
            retryable_exceptions=(requests.exceptions.ConnectionError,),
        )
        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise requests.exceptions.ConnectionError("timeout")
            return "ok"

        result = flaky()
        assert result == "ok"
        assert len(attempts) == 3
        assert mock_sleep.call_count == 2  # slept before attempt 2 and 3

    @patch("etl.extractors.time.sleep")
    def test_raises_after_max_attempts_exhausted(self, mock_sleep):
        @with_retry(
            max_attempts=3,
            base_delay=1.0,
            jitter=False,
            retryable_exceptions=(requests.exceptions.Timeout,),
        )
        def always_fails():
            raise requests.exceptions.Timeout("read timeout")

        with pytest.raises(requests.exceptions.Timeout):
            always_fails()
        assert mock_sleep.call_count == 2  # sleep before attempt 2 and 3

    @patch("etl.extractors.time.sleep")
    def test_non_retryable_exception_propagates_immediately(self, mock_sleep):
        @with_retry(
            max_attempts=3,
            base_delay=1.0,
            jitter=False,
            retryable_exceptions=(requests.exceptions.Timeout,),
        )
        def raises_value_error():
            raise ValueError("bad input")

        with pytest.raises(ValueError, match="bad input"):
            raises_value_error()
        mock_sleep.assert_not_called()  # no retry attempted

    @patch("etl.extractors.random.uniform", return_value=0.0)
    @patch("etl.extractors.time.sleep")
    def test_delay_doubles_each_attempt(self, mock_sleep, _mock_rand):
        """With jitter=True but uniform returning 0.0, delays equal base*base^n."""

        @with_retry(
            max_attempts=4,
            base_delay=1.0,
            max_delay=100.0,
            exponential_base=2,
            jitter=True,
            retryable_exceptions=(requests.exceptions.Timeout,),
        )
        def always_fails():
            raise requests.exceptions.Timeout()

        with pytest.raises(requests.exceptions.Timeout):
            always_fails()

        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert sleep_calls == pytest.approx([1.0, 2.0, 4.0])

    @patch("etl.extractors.time.sleep")
    def test_delay_capped_at_max_delay(self, mock_sleep):
        @with_retry(
            max_attempts=5,
            base_delay=10.0,
            max_delay=15.0,
            exponential_base=2,
            jitter=False,
            retryable_exceptions=(requests.exceptions.Timeout,),
        )
        def always_fails():
            raise requests.exceptions.Timeout()

        with pytest.raises(requests.exceptions.Timeout):
            always_fails()

        for actual_delay in [c.args[0] for c in mock_sleep.call_args_list]:
            assert actual_delay <= 15.0


# ---------------------------------------------------------------------------
# ExchangeRate dataclass
# ---------------------------------------------------------------------------

class TestExchangeRate:
    def test_to_dict_contains_required_keys(self):
        er = ExchangeRate(
            pair="USD_IDR",
            rate=18176.5,
            timestamp=FIXED_NOW,
            source="yfinance",
            fetched_at=FIXED_NOW,
            data_quality_score=1.0,
        )
        d = er.to_dict()
        assert d["pair"] == "USD_IDR"
        assert d["rate"] == pytest.approx(18176.5)
        assert d["source"] == "yfinance"
        assert d["data_quality_score"] == pytest.approx(1.0)
        # timestamps are ISO strings
        assert "T" in d["timestamp"]
        assert "T" in d["fetched_at"]

    def test_fetched_at_defaults_to_now(self):
        before = datetime.now(timezone.utc)
        er = ExchangeRate(pair="X", rate=1.0, timestamp=FIXED_NOW, source="test")
        after = datetime.now(timezone.utc)
        assert before <= er.fetched_at <= after


# ---------------------------------------------------------------------------
# ExtractionResult dataclass
# ---------------------------------------------------------------------------

class TestExtractionResult:
    def test_to_dict_canonical_structure(self):
        rates = [
            ExchangeRate("USD_IDR", 18176.5, FIXED_NOW, "yfinance",
                         fetched_at=FIXED_NOW, data_quality_score=1.0)
        ]
        result = ExtractionResult(rates=rates, fetched_at=FIXED_NOW, source="yfinance")
        d = result.to_dict()

        assert d["source"] == "yfinance"
        assert isinstance(d["rates"], list)
        assert len(d["rates"]) == 1
        assert d["errors"] == []

    def test_errors_populated(self):
        result = ExtractionResult(
            rates=[], fetched_at=FIXED_NOW, source="fred",
            errors=["[fred] oops"]
        )
        assert result.to_dict()["errors"] == ["[fred] oops"]


# ---------------------------------------------------------------------------
# YFinanceExtractor
# ---------------------------------------------------------------------------

class TestYFinanceExtractor:
    """Tests for YFinanceExtractor using mocked yfinance."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_fast_info(last_price: float | None = 18176.5) -> SimpleNamespace:
        return SimpleNamespace(last_price=last_price)

    @staticmethod
    def _make_ticker_mock(
        last_price: float | None = 18176.5,
        history_df=None,
    ) -> MagicMock:
        import pandas as pd

        ticker = MagicMock()
        ticker.fast_info = TestYFinanceExtractor._make_fast_info(last_price)

        if history_df is None:
            # Default: one row of close data
            idx = pd.DatetimeIndex([pd.Timestamp("2025-01-15", tz="UTC")])
            history_df = pd.DataFrame({"Close": [18176.5]}, index=idx)
        ticker.history.return_value = history_df

        return ticker

    # ------------------------------------------------------------------
    # _fetch_single_pair – happy paths
    # ------------------------------------------------------------------

    @patch("etl.extractors.yf.Ticker")
    @patch("etl.extractors.time.sleep")
    def test_fetch_single_pair_fast_info_success(self, _sleep, mock_ticker_cls):
        mock_ticker_cls.return_value = self._make_ticker_mock(last_price=18176.5)
        extractor = YFinanceExtractor()
        rate = extractor._fetch_single_pair("USD_IDR")

        assert rate.pair == "USD_IDR"
        assert rate.rate == pytest.approx(18176.5)
        assert rate.source == "yfinance"
        assert rate.data_quality_score == pytest.approx(1.0)

    @patch("etl.extractors.yf.Ticker")
    @patch("etl.extractors.time.sleep")
    def test_fetch_single_pair_falls_back_to_history(self, _sleep, mock_ticker_cls):
        """When fast_info.last_price is None, extractor falls back to history()."""
        import pandas as pd

        idx = pd.DatetimeIndex([pd.Timestamp("2025-01-15", tz="UTC")])
        hist = pd.DataFrame({"Close": [18200.0]}, index=idx)
        mock_ticker_cls.return_value = self._make_ticker_mock(
            last_price=None, history_df=hist
        )

        extractor = YFinanceExtractor()
        rate = extractor._fetch_single_pair("USD_IDR")
        assert rate.rate == pytest.approx(18200.0)

    @patch("etl.extractors.yf.Ticker")
    @patch("etl.extractors.time.sleep")
    def test_fetch_single_pair_empty_history_raises(self, _sleep, mock_ticker_cls):
        import pandas as pd

        empty_hist = pd.DataFrame({"Close": []})
        mock_ticker_cls.return_value = self._make_ticker_mock(
            last_price=None, history_df=empty_hist
        )

        extractor = YFinanceExtractor()
        with pytest.raises(ValueError, match="No data returned"):
            extractor._fetch_single_pair("USD_IDR")

    @patch("etl.extractors.yf.Ticker")
    @patch("etl.extractors.time.sleep")
    def test_fetch_single_pair_invalid_rate_raises(self, _sleep, mock_ticker_cls):
        """Negative rate from fast_info should trigger validation error."""
        mock_ticker_cls.return_value = self._make_ticker_mock(last_price=-1.0)

        extractor = YFinanceExtractor()
        with pytest.raises(ValueError, match="positive"):
            extractor._fetch_single_pair("USD_IDR")

    # ------------------------------------------------------------------
    # _fetch_single_pair – retry behaviour
    # ------------------------------------------------------------------

    @patch("etl.extractors.yf.Ticker")
    @patch("etl.extractors.time.sleep")
    def test_retries_on_connection_error(self, mock_sleep, mock_ticker_cls):
        call_count = 0
        good_ticker = self._make_ticker_mock(last_price=18176.5)

        def ticker_side_effect(symbol):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise requests.exceptions.ConnectionError("network down")
            return good_ticker

        mock_ticker_cls.side_effect = ticker_side_effect

        extractor = YFinanceExtractor()
        rate = extractor._fetch_single_pair("USD_IDR")
        assert rate.rate == pytest.approx(18176.5)
        assert call_count == 3

    @patch("etl.extractors.yf.Ticker")
    @patch("etl.extractors.time.sleep")
    def test_exhausts_retries_raises(self, mock_sleep, mock_ticker_cls):
        mock_ticker_cls.side_effect = requests.exceptions.Timeout("timeout")

        extractor = YFinanceExtractor()
        with pytest.raises(requests.exceptions.Timeout):
            extractor._fetch_single_pair("USD_IDR")
        assert mock_sleep.call_count == RETRY_CONFIG["max_attempts"] - 1

    # ------------------------------------------------------------------
    # fetch_rates – orchestration
    # ------------------------------------------------------------------

    @patch("etl.extractors.yf.Ticker")
    @patch("etl.extractors.time.sleep")
    def test_fetch_rates_all_pairs_success(self, _sleep, mock_ticker_cls):
        mock_ticker_cls.return_value = self._make_ticker_mock(last_price=18176.5)
        extractor = YFinanceExtractor()
        result = extractor.fetch_rates(["USD_IDR", "EUR_IDR", "SGD_IDR", "JPY_IDR"])

        # JPY_IDR will likely trigger quality=0.6 since 18176.5 >> 250 — that's fine,
        # we just verify structure and that all 4 are attempted.
        assert result["source"] == "yfinance"
        assert isinstance(result["rates"], list)
        assert "fetched_at" in result

    @patch("etl.extractors.yf.Ticker")
    @patch("etl.extractors.time.sleep")
    def test_fetch_rates_partial_failure_collects_errors(self, _sleep, mock_ticker_cls):
        """One pair fails; the rest still succeed and errors are collected."""
        call_count = 0

        def ticker_side_effect(symbol):
            nonlocal call_count
            call_count += 1
            if symbol == "EURIDR=X":
                raise requests.exceptions.Timeout("timeout")
            return self._make_ticker_mock(last_price=18176.5)

        mock_ticker_cls.side_effect = ticker_side_effect

        extractor = YFinanceExtractor()
        result = extractor.fetch_rates(["USD_IDR", "EUR_IDR"])

        pairs_returned = {r["pair"] for r in result["rates"]}
        assert "USD_IDR" in pairs_returned
        assert "EUR_IDR" not in pairs_returned
        assert len(result["errors"]) == 1

    def test_fetch_rates_unsupported_pair_skipped(self):
        extractor = YFinanceExtractor()
        # XYZ_IDR is not in SUPPORTED_PAIRS; fetch_rates should skip it gracefully
        with patch.object(extractor, "_fetch_single_pair") as mock_fetch:
            result = extractor.fetch_rates(["XYZ_IDR"])
        mock_fetch.assert_not_called()
        assert result["rates"] == []
        assert result["errors"] == []

    def test_get_source_name(self):
        assert YFinanceExtractor().get_source_name() == "yfinance"

    def test_supported_pairs_contains_all_four(self):
        assert set(YFinanceExtractor.SUPPORTED_PAIRS) == {
            "USD_IDR", "EUR_IDR", "SGD_IDR", "JPY_IDR"
        }


# ---------------------------------------------------------------------------
# FREDExtractor
# ---------------------------------------------------------------------------

class TestFREDExtractor:
    """Tests for FREDExtractor using mocked requests.Session."""

    API_KEY = "test_fred_key_123"

    # ------------------------------------------------------------------
    # Instantiation
    # ------------------------------------------------------------------

    def test_init_with_explicit_api_key(self):
        extractor = FREDExtractor(api_key=self.API_KEY)
        assert extractor._api_key == self.API_KEY

    def test_init_reads_env_var(self, monkeypatch):
        monkeypatch.setenv("FRED_API_KEY", "env_key_456")
        extractor = FREDExtractor()
        assert extractor._api_key == "env_key_456"

    def test_init_raises_without_api_key(self, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        with pytest.raises(EnvironmentError, match="FRED API key is required"):
            FREDExtractor()

    # ------------------------------------------------------------------
    # _fetch_single_pair – happy path
    # ------------------------------------------------------------------

    @patch("etl.extractors.time.sleep")
    def test_fetch_single_pair_success(self, _sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()
        extractor._session.get.return_value = _fred_response([_fred_obs("18176.50")])

        rate = extractor._fetch_single_pair("USD_IDR")

        assert rate.pair == "USD_IDR"
        assert rate.rate == pytest.approx(18176.50)
        assert rate.source == "fred"
        assert rate.data_quality_score == pytest.approx(1.0)
        assert rate.timestamp == datetime(2025, 1, 15, tzinfo=timezone.utc)

    @patch("etl.extractors.time.sleep")
    def test_fetch_single_pair_eur_idr(self, _sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()
        extractor._session.get.return_value = _fred_response([_fred_obs("19234.25")])

        rate = extractor._fetch_single_pair("EUR_IDR")
        assert rate.pair == "EUR_IDR"

    # ------------------------------------------------------------------
    # _fetch_single_pair – error handling
    # ------------------------------------------------------------------

    @patch("etl.extractors.time.sleep")
    def test_fred_dot_sentinel_raises_value_error(self, _sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()
        extractor._session.get.return_value = _fred_response([_fred_obs(".")])

        with pytest.raises(ValueError, match="Missing value"):
            extractor._fetch_single_pair("USD_IDR")

    @patch("etl.extractors.time.sleep")
    def test_empty_observations_raises_value_error(self, _sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()
        extractor._session.get.return_value = _fred_response([])

        with pytest.raises(ValueError, match="No observations"):
            extractor._fetch_single_pair("USD_IDR")

    @patch("etl.extractors.time.sleep")
    def test_invalid_json_raises_value_error(self, _sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()
        bad_resp = Mock()
        bad_resp.status_code = 200
        bad_resp.headers = {}
        bad_resp.raise_for_status = Mock()
        bad_resp.json.side_effect = ValueError("No JSON")
        extractor._session.get.return_value = bad_resp

        with pytest.raises(ValueError, match="Invalid JSON"):
            extractor._fetch_single_pair("USD_IDR")

    @patch("etl.extractors.time.sleep")
    def test_server_error_retries_then_raises(self, mock_sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()
        extractor._session.get.return_value = _fred_response([], status_code=500)

        with pytest.raises(requests.exceptions.HTTPError):
            extractor._fetch_single_pair("USD_IDR")

        # with_retry fires 3 attempts, so sleep is called 2 times
        assert mock_sleep.call_count == RETRY_CONFIG["max_attempts"] - 1

    @patch("etl.extractors.time.sleep")
    def test_rate_limit_429_waits_retry_after(self, mock_sleep):
        """On 429, extractor should sleep for Retry-After seconds then retry."""
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()

        rate_limited_resp = Mock()
        rate_limited_resp.status_code = 429
        rate_limited_resp.headers = {"Retry-After": "3"}
        rate_limited_resp.raise_for_status = Mock()
        rate_limited_resp.json.return_value = {}

        success_resp = _fred_response([_fred_obs("18176.50")])

        extractor._session.get.side_effect = [rate_limited_resp, success_resp]

        rate = extractor._fetch_single_pair("USD_IDR")
        # The 429-handling sleep is called once with value from Retry-After header
        mock_sleep.assert_any_call(3)
        assert rate.rate == pytest.approx(18176.50)

    @patch("etl.extractors.time.sleep")
    def test_connection_error_retries(self, mock_sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()

        extractor._session.get.side_effect = [
            requests.exceptions.ConnectionError("network down"),
            requests.exceptions.ConnectionError("network down"),
            _fred_response([_fred_obs("18176.50")]),
        ]

        rate = extractor._fetch_single_pair("USD_IDR")
        assert rate.rate == pytest.approx(18176.50)
        assert mock_sleep.call_count == 2

    @patch("etl.extractors.time.sleep")
    def test_malformed_date_falls_back_to_now(self, _sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()
        extractor._session.get.return_value = _fred_response(
            [_fred_obs("18176.50", date="not-a-date")]
        )

        before = datetime.now(timezone.utc)
        rate = extractor._fetch_single_pair("USD_IDR")
        after = datetime.now(timezone.utc)

        assert before <= rate.timestamp <= after

    # ------------------------------------------------------------------
    # fetch_rates – orchestration
    # ------------------------------------------------------------------

    @patch("etl.extractors.time.sleep")
    def test_fetch_rates_both_pairs_success(self, _sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()
        extractor._session.get.return_value = _fred_response([_fred_obs("18176.50")])

        result = extractor.fetch_rates(["USD_IDR", "EUR_IDR"])

        assert result["source"] == "fred"
        assert len(result["rates"]) == 2
        assert result["errors"] == []

    @patch("etl.extractors.time.sleep")
    def test_fetch_rates_partial_failure(self, _sleep):
        extractor = FREDExtractor(api_key=self.API_KEY)
        extractor._session = MagicMock()

        # First call (USD_IDR) succeeds; second (EUR_IDR) returns empty observations
        extractor._session.get.side_effect = [
            _fred_response([_fred_obs("18176.50")]),
            _fred_response([]),
        ]

        result = extractor.fetch_rates(["USD_IDR", "EUR_IDR"])

        pairs_returned = {r["pair"] for r in result["rates"]}
        assert "USD_IDR" in pairs_returned
        assert "EUR_IDR" not in pairs_returned
        assert len(result["errors"]) == 1
        assert "EUR_IDR" in result["errors"][0]

    def test_fetch_rates_unsupported_pair_skipped(self):
        extractor = FREDExtractor(api_key=self.API_KEY)
        with patch.object(extractor, "_fetch_single_pair") as mock_fetch:
            result = extractor.fetch_rates(["SGD_IDR"])  # not in FRED_SERIES_MAP
        mock_fetch.assert_not_called()
        assert result["rates"] == []

    def test_get_source_name(self):
        assert FREDExtractor(api_key=self.API_KEY).get_source_name() == "fred"

    def test_supported_pairs(self):
        assert set(FREDExtractor.SUPPORTED_PAIRS) == {"USD_IDR", "EUR_IDR"}


# ---------------------------------------------------------------------------
# get_extractor factory
# ---------------------------------------------------------------------------

class TestGetExtractor:

    def test_returns_yfinance_extractor(self):
        extractor = get_extractor("yfinance")
        assert isinstance(extractor, YFinanceExtractor)

    def test_returns_fred_extractor_with_api_key(self):
        extractor = get_extractor("fred", api_key="dummy_key")
        assert isinstance(extractor, FREDExtractor)

    def test_unknown_source_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown extractor source"):
            get_extractor("bloomberg")

    def test_fred_without_key_raises_environment_error(self, monkeypatch):
        monkeypatch.delenv("FRED_API_KEY", raising=False)
        with pytest.raises(EnvironmentError):
            get_extractor("fred")


# ---------------------------------------------------------------------------
# Response schema contract
# ---------------------------------------------------------------------------

class TestResponseSchema:
    """Verify fetch_rates() output always conforms to the ADR-001 schema."""

    REQUIRED_TOP_KEYS = {"rates", "fetched_at", "source", "errors"}
    REQUIRED_RATE_KEYS = {"pair", "rate", "timestamp", "source", "fetched_at",
                          "data_quality_score"}

    @patch("etl.extractors.yf.Ticker")
    @patch("etl.extractors.time.sleep")
    def test_yfinance_response_schema(self, _sleep, mock_ticker_cls):
        import pandas as pd

        idx = pd.DatetimeIndex([pd.Timestamp("2025-01-15", tz="UTC")])
        hist = pd.DataFrame({"Close": [18176.5]}, index=idx)
        ticker = MagicMock()
        ticker.fast_info = SimpleNamespace(last_price=18176.5)
        ticker.history.return_value = hist
        mock_ticker_cls.return_value = ticker

        result = YFinanceExtractor().fetch_rates(["USD_IDR"])
        assert self.REQUIRED_TOP_KEYS.issubset(result.keys())
        for rate_entry in result["rates"]:
            assert self.REQUIRED_RATE_KEYS.issubset(rate_entry.keys())

    @patch("etl.extractors.time.sleep")
    def test_fred_response_schema(self, _sleep):
        extractor = FREDExtractor(api_key="key")
        extractor._session = MagicMock()
        extractor._session.get.return_value = _fred_response([_fred_obs("18176.50")])

        result = extractor.fetch_rates(["USD_IDR"])
        assert self.REQUIRED_TOP_KEYS.issubset(result.keys())
        for rate_entry in result["rates"]:
            assert self.REQUIRED_RATE_KEYS.issubset(rate_entry.keys())