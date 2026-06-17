"""
tests/unit/test_validators.py
==============================
Comprehensive unit tests for src/etl/validators.py.

Coverage targets (>80%)
-----------------------
  ValidatorConfig         – env var loading (float, bool, invalid), defaults,
                            explicit overrides, repr
  ValidationResult        – dataclass defaults, add_error/add_warning,
                            is_valid toggling, repr
  BaseValidator           – log_results (INFO / WARNING / ERROR paths)
  ExchangeRateValidator   – all 5 check methods, quality score,
                            full validate() flow, edge cases
  _env_float / _env_bool  – valid, invalid, missing
  Private helpers         – _series_to_dataframe, _resolve_rate_columns,
                            _resolve_timestamp_series

Test strategy
-------------
- All time-dependent tests freeze ``datetime.now`` via ``monkeypatch`` or
  ``unittest.mock.patch`` so results are deterministic.
- No real external calls; pure pandas / numpy data only.
- Each test class covers one logical unit; parametrize for multi-scenario
  cases to keep the suite DRY.
"""

from __future__ import annotations

import pathlib
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Path bootstrap (mirrors conftest.py behaviour for standalone runs)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(pathlib.Path(__file__).parents[2] / "src"))

from src.etl.validators import (
    BaseValidator,
    ExchangeRateValidator,
    ValidationResult,
    ValidatorConfig,
    _env_bool,
    _env_float,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NOW_UTC = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Shared DataFrame / Series fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ohlcv_df() -> pd.DataFrame:
    """Clean yfinance-style OHLCV DataFrame — all checks should pass."""
    return pd.DataFrame(
        {
            "timestamp": [
                "2025-01-13",
                "2025-01-14",
                "2025-01-15",
                "2025-01-16",
                "2025-01-17",
            ],
            "rate_open": [16000.0, 16050.0, 16100.0, 16080.0, 16120.0],
            "rate_high": [16200.0, 16250.0, 16300.0, 16280.0, 16320.0],
            "rate_low": [15900.0, 15950.0, 16000.0, 15980.0, 16020.0],
            "rate_close": [16100.0, 16150.0, 16200.0, 16180.0, 16220.0],
        }
    )


@pytest.fixture
def fred_series() -> pd.Series:
    """Clean FRED monthly Series — chronological, no nulls."""
    return pd.Series(
        {
            "2024-11-01": 16000.0,
            "2024-12-01": 16100.0,
            "2025-01-01": 16200.0,
        }
    )


@pytest.fixture
def validator() -> ExchangeRateValidator:
    """Default validator — uses default config, freshness_hours=2."""
    return ExchangeRateValidator(
        config=ValidatorConfig(
            freshness_hours=2.0,
            anomaly_std_dev=3.0,
            null_threshold=0.05,
            rate_min=0.01,
            rate_max=1_000_000.0,
            fail_on_anomaly=False,
            fail_on_null=True,
        )
    )


def _fresh_df(rate: float = 16000.0) -> pd.DataFrame:
    """
    Single-row DataFrame with timestamp set to NOW_UTC so freshness
    always passes when ``now`` is mocked to ``NOW_UTC``.
    """
    return pd.DataFrame(
        {
            "timestamp": [NOW_UTC.isoformat()],
            "rate_close": [rate],
        }
    )


# ===========================================================================
# _env_float helper
# ===========================================================================


class TestEnvFloat:
    def test_returns_default_when_unset(self, monkeypatch):
        monkeypatch.delenv("_TEST_KEY", raising=False)
        assert _env_float("_TEST_KEY", 42.5) == pytest.approx(42.5)

    def test_reads_valid_float(self, monkeypatch):
        monkeypatch.setenv("_TEST_KEY", "3.14")
        assert _env_float("_TEST_KEY", 0.0) == pytest.approx(3.14)

    def test_falls_back_on_invalid_value(self, monkeypatch):
        monkeypatch.setenv("_TEST_KEY", "not_a_float")
        assert _env_float("_TEST_KEY", 99.0) == pytest.approx(99.0)


# ===========================================================================
# _env_bool helper
# ===========================================================================


class TestEnvBool:
    def test_returns_default_when_unset(self, monkeypatch):
        monkeypatch.delenv("_TEST_BOOL", raising=False)
        assert _env_bool("_TEST_BOOL", True) is True

    @pytest.mark.parametrize("raw", ["true", "True", "TRUE", "1", "yes"])
    def test_truthy_values(self, monkeypatch, raw):
        monkeypatch.setenv("_TEST_BOOL", raw)
        assert _env_bool("_TEST_BOOL", False) is True

    @pytest.mark.parametrize("raw", ["false", "0", "no", "nope", ""])
    def test_falsy_values(self, monkeypatch, raw):
        monkeypatch.setenv("_TEST_BOOL", raw)
        assert _env_bool("_TEST_BOOL", True) is False


# ===========================================================================
# ValidatorConfig
# ===========================================================================


class TestValidatorConfig:
    def test_default_values(self, monkeypatch):
        """All env vars absent → defaults are loaded."""
        for key in (
            "VALIDATOR_NULL_THRESHOLD",
            "VALIDATOR_ANOMALY_STD_DEV",
            "VALIDATOR_FRESHNESS_HOURS",
            "VALIDATOR_RATE_MIN",
            "VALIDATOR_RATE_MAX",
            "VALIDATOR_FAIL_ON_ANOMALY",
            "VALIDATOR_FAIL_ON_NULL",
        ):
            monkeypatch.delenv(key, raising=False)

        cfg = ValidatorConfig()
        assert cfg.null_threshold == pytest.approx(0.05)
        assert cfg.anomaly_std_dev == pytest.approx(3.0)
        assert cfg.freshness_hours == pytest.approx(2.0)
        assert cfg.rate_min == pytest.approx(0.01)
        assert cfg.rate_max == pytest.approx(1_000_000.0)
        assert cfg.fail_on_anomaly is False
        assert cfg.fail_on_null is True

    def test_explicit_overrides_env(self, monkeypatch):
        monkeypatch.setenv("VALIDATOR_RATE_MIN", "999")
        cfg = ValidatorConfig(rate_min=0.5)
        assert cfg.rate_min == pytest.approx(0.5)

    def test_env_var_override(self, monkeypatch):
        monkeypatch.setenv("VALIDATOR_ANOMALY_STD_DEV", "5.0")
        cfg = ValidatorConfig()
        assert cfg.anomaly_std_dev == pytest.approx(5.0)

    def test_repr_contains_key_fields(self):
        cfg = ValidatorConfig(rate_min=1.0, rate_max=500.0)
        r = repr(cfg)
        assert "null_threshold" in r
        assert "rate_min" in r

    def test_fail_on_null_from_env(self, monkeypatch):
        monkeypatch.setenv("VALIDATOR_FAIL_ON_NULL", "false")
        cfg = ValidatorConfig()
        assert cfg.fail_on_null is False


# ===========================================================================
# ValidationResult
# ===========================================================================


class TestValidationResult:
    def test_default_creation(self):
        r = ValidationResult()
        assert r.is_valid is True
        assert r.quality_score == pytest.approx(1.0)
        assert r.errors == []
        assert r.warnings == []
        assert r.checks_passed == {}
        assert r.anomaly_scores == {}
        assert r.details == {}

    def test_add_error_marks_invalid(self):
        r = ValidationResult()
        r.add_error("something broke")
        assert r.is_valid is False
        assert "something broke" in r.errors

    def test_add_warning_does_not_invalidate(self):
        r = ValidationResult()
        r.add_warning("soft issue")
        assert r.is_valid is True
        assert "soft issue" in r.warnings

    def test_multiple_errors_accumulate(self):
        r = ValidationResult()
        r.add_error("err1")
        r.add_error("err2")
        assert len(r.errors) == 2
        assert r.is_valid is False

    def test_repr(self):
        r = ValidationResult(quality_score=0.85)
        text = repr(r)
        assert "ValidationResult" in text
        assert "0.850" in text

    def test_validation_result_quality_score(self):
        r = ValidationResult(quality_score=0.75)
        assert r.quality_score == pytest.approx(0.75)


# ===========================================================================
# BaseValidator.log_results  (tested via ExchangeRateValidator)
# ===========================================================================


class TestBaseValidatorLogResults:
    def test_logs_info_on_clean_result(self, validator, caplog):
        import logging

        result = ValidationResult(is_valid=True, quality_score=1.0)
        with caplog.at_level(logging.INFO):
            validator.log_results(result, source="test")
        assert any("valid=True" in r.message for r in caplog.records)

    def test_logs_error_on_invalid_result(self, validator, caplog):
        import logging

        result = ValidationResult(is_valid=False, quality_score=0.0)
        result.add_error("critical failure")
        with caplog.at_level(logging.ERROR):
            validator.log_results(result)
        assert any("critical failure" in r.message for r in caplog.records)

    def test_logs_warning_on_warnings(self, validator, caplog):
        import logging

        result = ValidationResult()
        result.add_warning("soft issue here")
        with caplog.at_level(logging.WARNING):
            validator.log_results(result, source="yfinance")
        assert any("soft issue here" in r.message for r in caplog.records)


# ===========================================================================
# ExchangeRateValidator — Null Values
# ===========================================================================


class TestValidateNullValues:
    def test_pass_no_nulls(self, validator, ohlcv_df):
        result = ValidationResult()
        ok, score = validator.validate_null_values(ohlcv_df, ["rate_open", "rate_close"], result)
        assert ok is True
        assert score == pytest.approx(1.0)
        assert result.errors == []

    def test_warn_on_nulls_below_threshold(self, validator):
        """Single NaN in 10 cells = 10% ratio but fail_on_null default True →
        ratio <= threshold (5%) NOT met → should warn not error here the
        threshold is 0.05 but ratio is 0.1 so with fail_on_null=True it errors."""
        cfg = ValidatorConfig(null_threshold=0.5, fail_on_null=False)
        v = ExchangeRateValidator(config=cfg)
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-01", "2025-01-02"],
                "rate_close": [16000.0, None],
            }
        )
        result = ValidationResult()
        ok, score = v.validate_null_values(df, ["rate_close"], result)
        assert ok is True
        assert len(result.warnings) == 1
        assert result.errors == []
        assert score < 1.0

    def test_fail_null_values_exceed_threshold(self, validator):
        """50% nulls with fail_on_null=True and threshold=0.05 → hard fail."""
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-01", "2025-01-02"],
                "rate_close": [16000.0, None],
            }
        )
        result = ValidationResult()
        ok, score = validator.validate_null_values(df, ["rate_close"], result)
        assert ok is False
        assert len(result.errors) == 1
        assert score < 1.0

    def test_all_nulls_gives_zero_score(self, validator):
        df = pd.DataFrame(
            {
                "rate_close": [None, None, None],
            }
        )
        result = ValidationResult()
        ok, score = validator.validate_null_values(df, ["rate_close"], result)
        assert score == pytest.approx(0.0)

    def test_empty_cells_returns_one(self, validator):
        """Degenerate: zero total cells → score 1.0, no errors."""
        df = pd.DataFrame({"rate_close": pd.Series([], dtype=float)})
        result = ValidationResult()
        ok, score = validator.validate_null_values(df, ["rate_close"], result)
        assert ok is True
        assert score == pytest.approx(1.0)


# ===========================================================================
# ExchangeRateValidator — Rate Range
# ===========================================================================


class TestValidateRateRange:
    def test_valid_rates(self, validator, ohlcv_df):
        result = ValidationResult()
        ok, score = validator.validate_rate_range(
            ohlcv_df, ["rate_open", "rate_high", "rate_low", "rate_close"], result
        )
        assert ok is True
        assert score == pytest.approx(1.0)
        assert result.errors == []

    def test_negative_rate_hard_fail(self, validator):
        df = pd.DataFrame({"rate_close": [-1.0, 16000.0, 16100.0]})
        result = ValidationResult()
        ok, score = validator.validate_rate_range(df, ["rate_close"], result)
        assert ok is False
        assert any("below minimum" in e for e in result.errors)
        assert score < 1.0

    def test_zero_rate_hard_fail(self, validator):
        df = pd.DataFrame({"rate_close": [0.0, 16000.0]})
        result = ValidationResult()
        ok, score = validator.validate_rate_range(df, ["rate_close"], result)
        assert ok is False

    def test_above_max_hard_fail(self, validator):
        df = pd.DataFrame({"rate_close": [2_000_000.0, 16000.0]})
        result = ValidationResult()
        ok, score = validator.validate_rate_range(df, ["rate_close"], result)
        assert ok is False
        assert any("above maximum" in e for e in result.errors)

    def test_excessive_precision_warns(self, validator):
        """11 decimal places → soft warning, not hard fail."""
        df = pd.DataFrame({"rate_close": [16000.12345678901, 16100.0, 16200.0]})
        result = ValidationResult()
        ok, score = validator.validate_rate_range(df, ["rate_close"], result)
        assert ok is True
        assert any("decimal places" in w for w in result.warnings)

    def test_score_proportional_to_valid_cells(self, validator):
        """2 out of 4 rates invalid → score ≤ 0.5."""
        df = pd.DataFrame({"rate_close": [-1.0, -2.0, 16000.0, 16100.0]})
        result = ValidationResult()
        _, score = validator.validate_rate_range(df, ["rate_close"], result)
        assert score <= 0.5

    @pytest.mark.parametrize("rate", [0.01, 100.0, 16000.0, 999_000.0])
    def test_boundary_rates_valid(self, validator, rate):
        df = pd.DataFrame({"rate_close": [rate, rate + 1, rate + 2]})
        result = ValidationResult()
        ok, _ = validator.validate_rate_range(df, ["rate_close"], result)
        assert ok is True


# ===========================================================================
# ExchangeRateValidator — Date Consistency
# ===========================================================================


class TestValidateDateConsistency:
    def test_valid_chronological_order(self, validator, ohlcv_df):
        result = ValidationResult()
        ok, score = validator.validate_date_consistency(ohlcv_df, result)
        assert ok is True
        assert score == pytest.approx(1.0)
        assert result.errors == []

    def test_out_of_order_hard_fail(self, validator):
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-15", "2025-01-14", "2025-01-13"],
                "rate_close": [16000.0, 16100.0, 16200.0],
            }
        )
        result = ValidationResult()
        ok, score = validator.validate_date_consistency(df, result)
        assert ok is False
        assert score == pytest.approx(0.0)
        assert any("chronological" in e for e in result.errors)

    def test_duplicate_dates_warns_not_fails(self, validator):
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-13", "2025-01-13", "2025-01-14"],
                "rate_close": [16000.0, 16000.0, 16100.0],
            }
        )
        result = ValidationResult()
        ok, score = validator.validate_date_consistency(df, result)
        assert ok is True
        assert any("duplicate" in w for w in result.warnings)
        assert score < 1.0

    def test_no_timestamp_column_skips(self, validator):
        df = pd.DataFrame({"rate_close": [16000.0, 16100.0]})
        result = ValidationResult()
        ok, score = validator.validate_date_consistency(df, result)
        assert ok is True
        assert score == pytest.approx(1.0)
        assert any("no timestamp" in w.lower() for w in result.warnings)

    def test_date_column_also_accepted(self, validator):
        df = pd.DataFrame(
            {
                "date": ["2025-01-13", "2025-01-14", "2025-01-15"],
                "rate_close": [16000.0, 16100.0, 16200.0],
            }
        )
        result = ValidationResult()
        ok, _ = validator.validate_date_consistency(df, result)
        assert ok is True

    def test_details_populated(self, validator, ohlcv_df):
        result = ValidationResult()
        validator.validate_date_consistency(ohlcv_df, result)
        assert "date_min" in result.details
        assert "date_max" in result.details
        assert "date_duplicates" in result.details
        assert "date_count" in result.details


# ===========================================================================
# ExchangeRateValidator — Anomaly Detection
# ===========================================================================


class TestDetectAnomalies:
    def test_no_anomalies_in_stable_data(self, validator, ohlcv_df):
        result = ValidationResult()
        ok, score = validator.detect_anomalies(ohlcv_df, ["rate_open", "rate_close"], result)
        assert ok is True
        assert score == pytest.approx(1.0)
        assert result.anomaly_scores == {}

    def test_anomaly_detected_as_warning(self, validator):
        """Spike 10× larger than others → Z-score >> 3 → warning, not error."""
        # 19 stable values + 1 huge outlier = 20 rows, strong Z-score signal
        stable = [16000.0] * 19
        rates = stable + [500_000.0]
        df = pd.DataFrame(
            {
                "timestamp": [f"2025-01-{i+1:02d}" for i in range(20)],
                "rate_close": rates,
            }
        )
        result = ValidationResult()
        ok, score = validator.detect_anomalies(df, ["rate_close"], result)
        assert ok is True  # fail_on_anomaly=False
        assert len(result.anomaly_scores) >= 1
        assert any("anomalous" in w for w in result.warnings)
        assert score < 1.0

    def test_anomaly_hard_fails_when_configured(self):
        cfg = ValidatorConfig(fail_on_anomaly=True, anomaly_std_dev=2.0)
        v = ExchangeRateValidator(config=cfg)
        stable = [16000.0] * 19
        rates = stable + [500_000.0]
        df = pd.DataFrame({"rate_close": rates})
        result = ValidationResult()
        ok, _ = v.detect_anomalies(df, ["rate_close"], result)
        assert ok is False
        assert result.is_valid is False

    def test_fewer_than_3_rows_skips(self, validator):
        df = pd.DataFrame({"rate_close": [16000.0, 16100.0]})
        result = ValidationResult()
        ok, score = validator.detect_anomalies(df, ["rate_close"], result)
        assert ok is True
        assert score == pytest.approx(1.0)
        assert any("fewer than 3" in w for w in result.warnings)

    def test_constant_column_skips_std_zero(self, validator):
        """All values identical → std = 0 → no Z-score computed → no anomalies."""
        df = pd.DataFrame({"rate_close": [16000.0] * 10})
        result = ValidationResult()
        ok, score = validator.detect_anomalies(df, ["rate_close"], result)
        assert ok is True
        assert result.anomaly_scores == {}

    def test_anomaly_score_range(self, validator):
        """Per-record anomaly scores must be in [0, 1]."""
        stable = [16000.0] * 19
        rates = stable + [500_000.0]
        df = pd.DataFrame({"rate_close": rates})
        result = ValidationResult()
        validator.detect_anomalies(df, ["rate_close"], result)
        for s in result.anomaly_scores.values():
            assert 0.0 <= s <= 1.0

    def test_details_populated(self, validator, ohlcv_df):
        result = ValidationResult()
        validator.detect_anomalies(ohlcv_df, ["rate_close"], result)
        assert "anomaly_count" in result.details
        assert "anomaly_ratio" in result.details


# ===========================================================================
# ExchangeRateValidator — Freshness
# ===========================================================================


class TestValidateFreshness:
    def test_fresh_data_passes(self, validator):
        """Timestamp 30 minutes ago → within 2h threshold → score 1.0."""
        recent = NOW_UTC - timedelta(minutes=30)
        df = pd.DataFrame(
            {
                "timestamp": [recent.isoformat()],
                "rate_close": [16000.0],
            }
        )
        result = ValidationResult()
        with patch(
            "etl.validators.datetime",
            **{"now.return_value": NOW_UTC, "side_effect": lambda *a, **kw: datetime(*a, **kw)},
        ):
            # Patch only datetime.now inside the module
            with patch("etl.validators.datetime") as mock_dt:
                mock_dt.now.return_value = NOW_UTC
                mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
                ok, score = validator.validate_freshness(df, result)
        assert ok is True
        assert result.errors == []

    def test_stale_data_warns(self, validator):
        """Timestamp 5 hours ago with 2h threshold → warning, score < 1."""
        stale = NOW_UTC - timedelta(hours=5)
        df = pd.DataFrame(
            {
                "timestamp": [stale.isoformat()],
                "rate_close": [16000.0],
            }
        )
        result = ValidationResult()
        with patch("etl.validators.datetime") as mock_dt:
            mock_dt.now.return_value = NOW_UTC
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            ok, score = validator.validate_freshness(df, result)
        assert ok is True  # freshness is always soft
        assert any("old" in w for w in result.warnings)
        assert score < 1.0

    def test_no_timestamp_column_skips(self, validator):
        df = pd.DataFrame({"rate_close": [16000.0]})
        result = ValidationResult()
        ok, score = validator.validate_freshness(df, result)
        assert ok is True
        assert score == pytest.approx(1.0)

    def test_details_populated(self, validator):
        recent = NOW_UTC - timedelta(minutes=10)
        df = pd.DataFrame({"timestamp": [recent.isoformat()], "rate_close": [16000.0]})
        result = ValidationResult()
        with patch("etl.validators.datetime") as mock_dt:
            mock_dt.now.return_value = NOW_UTC
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            validator.validate_freshness(df, result)
        assert "freshness_most_recent" in result.details
        assert "freshness_age_hours" in result.details
        assert "freshness_threshold_hours" in result.details

    def test_score_exactly_zero_at_double_threshold(self, validator):
        """Age = 2× threshold → score = 0.0."""
        threshold = validator.config.freshness_hours
        stale = NOW_UTC - timedelta(hours=threshold * 2 + 0.1)
        df = pd.DataFrame({"timestamp": [stale.isoformat()], "rate_close": [16000.0]})
        result = ValidationResult()
        with patch("etl.validators.datetime") as mock_dt:
            mock_dt.now.return_value = NOW_UTC
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            _, score = validator.validate_freshness(df, result)
        assert score == pytest.approx(0.0, abs=1e-6)


# ===========================================================================
# ExchangeRateValidator — Quality Score
# ===========================================================================


class TestCalculateQualityScore:
    def test_perfect_score(self, validator):
        scores = {
            k: 1.0
            for k in (
                "null_check",
                "rate_range",
                "date_consistency",
                "anomaly_detection",
                "freshness",
            )
        }
        assert validator.calculate_quality_score(scores) == pytest.approx(1.0)

    def test_zero_score_all_failed(self, validator):
        scores = {
            k: 0.0
            for k in (
                "null_check",
                "rate_range",
                "date_consistency",
                "anomaly_detection",
                "freshness",
            )
        }
        assert validator.calculate_quality_score(scores) == pytest.approx(0.0)

    def test_average_of_mixed_scores(self, validator):
        scores = {
            "null_check": 1.0,
            "rate_range": 1.0,
            "date_consistency": 0.8,
            "anomaly_detection": 0.9,
            "freshness": 1.0,
        }
        expected = round((1.0 + 1.0 + 0.8 + 0.9 + 1.0) / 5, 4)
        assert validator.calculate_quality_score(scores) == pytest.approx(expected)

    def test_empty_scores_returns_zero(self, validator):
        assert validator.calculate_quality_score({}) == pytest.approx(0.0)

    def test_single_check(self, validator):
        assert validator.calculate_quality_score({"only": 0.6}) == pytest.approx(0.6)

    def test_rounded_to_4_decimals(self, validator):
        scores = {"a": 1 / 3, "b": 1 / 3, "c": 1 / 3}
        result = validator.calculate_quality_score(scores)
        assert len(str(result).split(".")[-1]) <= 4


# ===========================================================================
# ExchangeRateValidator — validate() complete flow
# ===========================================================================


class TestValidateCompleteFlow:
    def test_clean_ohlcv_dataframe_is_valid(self, validator, ohlcv_df):
        """Fully clean data → is_valid=True, quality close to 1."""
        with patch("etl.validators.datetime") as mock_dt:
            # make data appear fresh — 30 min ago
            fresh_ts = NOW_UTC - timedelta(minutes=30)
            mock_dt.now.return_value = NOW_UTC
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            ohlcv_df["timestamp"] = [
                (NOW_UTC - timedelta(hours=4 - i)).isoformat() for i in range(len(ohlcv_df))
            ]
            result = validator.validate(ohlcv_df)

        assert result.is_valid is True
        assert result.quality_score >= 0.5
        assert "null_check" in result.checks_passed
        assert "rate_range" in result.checks_passed
        assert "date_consistency" in result.checks_passed
        assert "anomaly_detection" in result.checks_passed
        assert "freshness" in result.checks_passed

    def test_validate_fred_series(self, validator, fred_series):
        """FRED Series input should be auto-converted and validated."""
        result = validator.validate(fred_series)
        # Series is sorted chronologically → date consistency passes
        assert result.checks_passed.get("date_consistency") is True
        assert result.checks_passed.get("rate_range") is True

    def test_validate_empty_dataframe(self, validator):
        """Empty DataFrame → is_valid=False, quality=0.0."""
        result = validator.validate(pd.DataFrame())
        assert result.is_valid is False
        assert result.quality_score == pytest.approx(0.0)
        assert any("empty" in e.lower() for e in result.errors)

    def test_validate_missing_rate_columns(self, validator):
        """DataFrame with no recognised rate columns → is_valid=False."""
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-01"],
                "unknown_col": [1.0],
            }
        )
        result = validator.validate(df)
        assert result.is_valid is False
        assert any("No rate columns" in e for e in result.errors)

    def test_validate_raises_on_wrong_type(self, validator):
        with pytest.raises(TypeError, match="pd.DataFrame or pd.Series"):
            validator.validate({"not": "a dataframe"})  # type: ignore

    def test_validate_returns_correct_check_keys(self, validator, ohlcv_df):
        result = validator.validate(ohlcv_df)
        expected_keys = {
            "null_check",
            "rate_range",
            "date_consistency",
            "anomaly_detection",
            "freshness",
        }
        assert expected_keys.issubset(result.checks_passed.keys())

    def test_validate_with_negative_rate_fails(self, validator):
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-13", "2025-01-14", "2025-01-15"],
                "rate_close": [-100.0, 16000.0, 16100.0],
            }
        )
        result = validator.validate(df)
        assert result.is_valid is False
        assert result.checks_passed["rate_range"] is False

    def test_validate_with_out_of_order_dates_fails(self, validator):
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-15", "2025-01-14", "2025-01-13"],
                "rate_close": [16000.0, 16100.0, 16200.0],
            }
        )
        result = validator.validate(df)
        assert result.is_valid is False
        assert result.checks_passed["date_consistency"] is False

    def test_details_check_scores_populated(self, validator, ohlcv_df):
        result = validator.validate(ohlcv_df)
        assert "check_scores" in result.details
        check_scores = result.details["check_scores"]
        assert all(0.0 <= v <= 1.0 for v in check_scores.values())

    def test_quality_score_in_valid_range(self, validator, ohlcv_df):
        result = validator.validate(ohlcv_df)
        assert 0.0 <= result.quality_score <= 1.0


# ===========================================================================
# ExchangeRateValidator — private helpers
# ===========================================================================


class TestPrivateHelpers:
    def test_series_to_dataframe_columns(self, validator):
        s = pd.Series({"2025-01-01": 100.0, "2025-01-02": 101.0})
        df = validator._series_to_dataframe(s)
        assert list(df.columns) == ["timestamp", "rate"]
        assert len(df) == 2

    def test_series_to_dataframe_sorted(self, validator):
        """Unsorted index → should come out sorted ascending."""
        s = pd.Series({"2025-01-03": 103.0, "2025-01-01": 101.0, "2025-01-02": 102.0})
        df = validator._series_to_dataframe(s)
        assert df["timestamp"].tolist() == ["2025-01-01", "2025-01-02", "2025-01-03"]

    def test_resolve_rate_columns_ohlcv(self, validator, ohlcv_df):
        cols = validator._resolve_rate_columns(ohlcv_df)
        assert set(cols) == {"rate_open", "rate_high", "rate_low", "rate_close"}

    def test_resolve_rate_columns_fallback(self, validator):
        df = pd.DataFrame({"rate": [1.0], "other": [2.0]})
        cols = validator._resolve_rate_columns(df)
        assert cols == ["rate"]

    def test_resolve_rate_columns_empty_when_none_match(self, validator):
        df = pd.DataFrame({"foo": [1.0], "bar": [2.0]})
        cols = validator._resolve_rate_columns(df)
        assert cols == []

    def test_resolve_timestamp_series_from_timestamp_col(self, validator, ohlcv_df):
        ts = validator._resolve_timestamp_series(ohlcv_df)
        assert ts is not None
        assert ts.name == "timestamp"

    def test_resolve_timestamp_series_from_date_col(self, validator):
        df = pd.DataFrame({"date": ["2025-01-01"], "rate_close": [100.0]})
        ts = validator._resolve_timestamp_series(df)
        assert ts is not None
        assert ts.name == "date"

    def test_resolve_timestamp_series_from_index(self, validator):
        df = pd.DataFrame(
            {"rate_close": [100.0, 101.0]},
            index=pd.to_datetime(["2025-01-01", "2025-01-02"]),
        )
        ts = validator._resolve_timestamp_series(df)
        assert ts is not None

    def test_resolve_timestamp_series_returns_none_for_rangeindex(self, validator):
        df = pd.DataFrame({"rate_close": [100.0]})
        ts = validator._resolve_timestamp_series(df)
        assert ts is None


# ===========================================================================
# ExchangeRateValidator — parametrized edge cases
# ===========================================================================


class TestEdgeCases:
    @pytest.mark.parametrize("n_rows", [3, 10, 50, 100])
    def test_validate_various_dataframe_sizes(self, validator, n_rows):
        """All sizes >= 3 should complete without exception."""
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2025-01-01", periods=n_rows, freq="D").strftime(
                    "%Y-%m-%d"
                ),
                "rate_close": [16000.0 + i for i in range(n_rows)],
            }
        )
        result = validator.validate(df)
        assert isinstance(result, ValidationResult)

    def test_validate_single_row_dataframe(self, validator):
        """Single row — anomaly detection skips (< 3 rows), rest runs."""
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-01"],
                "rate_close": [16000.0],
            }
        )
        result = validator.validate(df)
        assert isinstance(result, ValidationResult)
        assert any("fewer than 3" in w for w in result.warnings)

    def test_validate_series_unsorted_input(self, validator):
        """Series with reversed index → _series_to_dataframe should sort it."""
        s = pd.Series(
            {
                "2025-01-03": 16200.0,
                "2025-01-01": 16000.0,
                "2025-01-02": 16100.0,
            }
        )
        result = validator.validate(s)
        # Date consistency should pass after sort
        assert result.checks_passed.get("date_consistency") is True

    def test_validate_alternative_rate_col(self, validator):
        """DataFrame with 'value' column (not OHLCV) is recognised."""
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-01", "2025-01-02", "2025-01-03"],
                "value": [16000.0, 16100.0, 16200.0],
            }
        )
        result = validator.validate(df)
        assert result.checks_passed.get("rate_range") is True

    def test_validate_with_nulls_and_invalid_range_both_fail(self, validator):
        """Both null check AND rate range fail → is_valid False, 2 errors."""
        df = pd.DataFrame(
            {
                "timestamp": ["2025-01-01", "2025-01-02", "2025-01-03"],
                "rate_close": [None, -1.0, 16000.0],
            }
        )
        result = validator.validate(df)
        assert result.is_valid is False
        # null check triggers (50% null > 5% threshold with fail_on_null=True)
        # rate range triggers (-1 < min)
        assert len(result.errors) >= 2

    def test_config_repr(self):
        cfg = ValidatorConfig(
            null_threshold=0.1,
            anomaly_std_dev=2.5,
            freshness_hours=4.0,
            rate_min=1.0,
            rate_max=500_000.0,
            fail_on_anomaly=True,
            fail_on_null=False,
        )
        r = repr(cfg)
        assert "0.1" in r
        assert "2.5" in r
        assert "True" in r
