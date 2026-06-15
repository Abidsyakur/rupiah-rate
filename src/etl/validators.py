"""
src/etl/validators.py
======================
Data validation layer for the Rupiah Exchange Rate Intelligence pipeline.

Validates exchange rate data from both yfinance (OHLCV DataFrames) and
FRED API (scalar Series) before loading to the database.

Validation pipeline
-------------------
1. Null / missing value check
2. Rate range check
3. Date consistency check (chronological order, duplicates)
4. Anomaly detection (Z-score based)
5. Freshness check
6. Quality score aggregation

Configuration
-------------
All thresholds are loaded from environment variables with sensible defaults.
Set them in your ``.env`` file or shell:

    VALIDATOR_NULL_THRESHOLD=0.05       # max allowed null ratio
    VALIDATOR_ANOMALY_STD_DEV=3.0       # Z-score threshold for anomalies
    VALIDATOR_FRESHNESS_HOURS=2         # max data age in hours
    VALIDATOR_RATE_MIN=0.01             # minimum valid rate
    VALIDATOR_RATE_MAX=1000000          # maximum valid rate
    VALIDATOR_FAIL_ON_ANOMALY=False     # hard-fail on anomalies?
    VALIDATOR_FAIL_ON_NULL=True         # hard-fail on null excess?

Example usage
-------------
    import pandas as pd
    from etl.validators import ExchangeRateValidator

    # --- yfinance OHLCV DataFrame ---
    validator = ExchangeRateValidator()
    df = pd.DataFrame({
        'timestamp': ['2025-01-13', '2025-01-14', '2025-01-15'],
        'rate_open':  [16000.0, 16100.0, 16200.0],
        'rate_high':  [16200.0, 16300.0, 16400.0],
        'rate_low':   [15900.0, 16000.0, 16100.0],
        'rate_close': [16100.0, 16200.0, 16300.0],
    })
    result = validator.validate(df)
    print(result.is_valid, result.quality_score, result.errors)

    # --- FRED monthly Series ---
    series = pd.Series({
        '2025-01-01': 18176.50,
        '2024-12-01': 18175.25,
        '2024-11-01': 18050.00,
    })
    result = validator.validate(series)
    print(result.quality_score, result.anomaly_scores)
"""

from __future__ import annotations

import logging
import math
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _env_float(key: str, default: float) -> float:
    """Read a float from an environment variable, falling back to default."""
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning(
            "Invalid value %r for env var %s — using default %.4f",
            raw, key, default,
        )
        return default


def _env_bool(key: str, default: bool) -> bool:
    """Read a bool from an environment variable (true/1/yes = True)."""
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes")


class ValidatorConfig:
    """
    Centralised, environment-driven validator configuration.

    All values can be overridden at construction time or via environment
    variables.  See module docstring for the full list of env vars.
    """

    def __init__(
        self,
        null_threshold: Optional[float] = None,
        anomaly_std_dev: Optional[float] = None,
        freshness_hours: Optional[float] = None,
        rate_min: Optional[float] = None,
        rate_max: Optional[float] = None,
        fail_on_anomaly: Optional[bool] = None,
        fail_on_null: Optional[bool] = None,
    ) -> None:
        self.null_threshold: float = (
            null_threshold
            if null_threshold is not None
            else _env_float("VALIDATOR_NULL_THRESHOLD", 0.05)
        )
        self.anomaly_std_dev: float = (
            anomaly_std_dev
            if anomaly_std_dev is not None
            else _env_float("VALIDATOR_ANOMALY_STD_DEV", 3.0)
        )
        self.freshness_hours: float = (
            freshness_hours
            if freshness_hours is not None
            else _env_float("VALIDATOR_FRESHNESS_HOURS", 2.0)
        )
        self.rate_min: float = (
            rate_min
            if rate_min is not None
            else _env_float("VALIDATOR_RATE_MIN", 0.01)
        )
        self.rate_max: float = (
            rate_max
            if rate_max is not None
            else _env_float("VALIDATOR_RATE_MAX", 1_000_000.0)
        )
        self.fail_on_anomaly: bool = (
            fail_on_anomaly
            if fail_on_anomaly is not None
            else _env_bool("VALIDATOR_FAIL_ON_ANOMALY", False)
        )
        self.fail_on_null: bool = (
            fail_on_null
            if fail_on_null is not None
            else _env_bool("VALIDATOR_FAIL_ON_NULL", True)
        )

    def __repr__(self) -> str:
        return (
            f"ValidatorConfig("
            f"null_threshold={self.null_threshold}, "
            f"anomaly_std_dev={self.anomaly_std_dev}, "
            f"freshness_hours={self.freshness_hours}, "
            f"rate_min={self.rate_min}, "
            f"rate_max={self.rate_max}, "
            f"fail_on_anomaly={self.fail_on_anomaly}, "
            f"fail_on_null={self.fail_on_null})"
        )


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """
    Aggregated result of a full validation run.

    Attributes
    ----------
    is_valid : bool
        ``True`` if all *hard* checks passed. Soft warnings do not affect
        this flag.
    quality_score : float
        Overall quality of the data, ``0.0`` (worst) – ``1.0`` (perfect).
        Calculated as the unweighted average of all individual check scores.
    errors : List[str]
        Hard failures — data must NOT be loaded if this list is non-empty.
    warnings : List[str]
        Soft issues — data can still be loaded but should be flagged.
    checks_passed : Dict[str, bool]
        Pass/fail result per named check.
    anomaly_scores : Dict[str, float]
        Per-record anomaly severity in ``[0.0, 1.0]`` keyed by record
        identifier (timestamp string or integer index).
    details : Dict[str, Any]
        Arbitrary per-check diagnostic information (counts, statistics, …).
    """

    is_valid: bool = True
    quality_score: float = 1.0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    checks_passed: Dict[str, bool] = field(default_factory=dict)
    anomaly_scores: Dict[str, float] = field(default_factory=dict)
    details: Dict[str, Any] = field(default_factory=dict)

    def add_error(self, msg: str) -> None:
        """Record a hard failure and mark the result invalid."""
        self.errors.append(msg)
        self.is_valid = False

    def add_warning(self, msg: str) -> None:
        """Record a soft warning (does not invalidate the result)."""
        self.warnings.append(msg)

    def __repr__(self) -> str:
        return (
            f"ValidationResult("
            f"is_valid={self.is_valid}, "
            f"quality_score={self.quality_score:.3f}, "
            f"errors={len(self.errors)}, "
            f"warnings={len(self.warnings)})"
        )


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class BaseValidator(ABC):
    """
    Abstract base class for all validators in the pipeline.

    Subclasses must implement :meth:`validate` and may override
    :meth:`log_results` for custom logging.
    """

    def __init__(self, config: Optional[ValidatorConfig] = None) -> None:
        self.config: ValidatorConfig = config or ValidatorConfig()

    @abstractmethod
    def validate(
        self, data: Union[pd.DataFrame, pd.Series]
    ) -> ValidationResult:
        """
        Run all validation checks on *data*.

        Parameters
        ----------
        data:
            Either a pandas ``DataFrame`` (yfinance OHLCV format) or a
            ``Series`` (FRED scalar format, index = date strings).

        Returns
        -------
        ValidationResult
        """

    def log_results(self, result: ValidationResult, source: str = "") -> None:
        """
        Log a ``ValidationResult`` at the appropriate severity level.

        Parameters
        ----------
        result:
            The result to log.
        source:
            Optional label for the data source (e.g. ``"yfinance"``).
        """
        prefix = f"[{source}] " if source else ""
        level = logging.ERROR if not result.is_valid else (
            logging.WARNING if result.warnings else logging.INFO
        )
        logger.log(
            level,
            "%sValidation complete — valid=%s quality=%.3f "
            "errors=%d warnings=%d",
            prefix,
            result.is_valid,
            result.quality_score,
            len(result.errors),
            len(result.warnings),
        )
        for err in result.errors:
            logger.error("%s%s", prefix, err)
        for warn in result.warnings:
            logger.warning("%s%s", prefix, warn)


# ---------------------------------------------------------------------------
# ExchangeRateValidator
# ---------------------------------------------------------------------------

class ExchangeRateValidator(BaseValidator):
    """
    Validates exchange rate data fetched from yfinance or FRED.

    Accepts two data shapes:

    * **DataFrame** (yfinance OHLCV): must have a ``timestamp`` column and
      at least one of ``rate_open``, ``rate_high``, ``rate_low``,
      ``rate_close``.  Internally works on the numeric columns only.
    * **Series** (FRED monthly/annual): index = date strings, values = rates.

    Validation pipeline
    -------------------
    1. ``validate_null_values``    — null / NaN ratio
    2. ``validate_rate_range``     — bounds & precision
    3. ``validate_date_consistency`` — chronological order, duplicates
    4. ``detect_anomalies``        — Z-score outlier detection
    5. ``validate_freshness``      — data age vs. current time
    6. ``calculate_quality_score`` — aggregate all check scores

    Example
    -------
    >>> import pandas as pd
    >>> from etl.validators import ExchangeRateValidator
    >>> validator = ExchangeRateValidator()
    >>> series = pd.Series({"2025-01-15": 18176.50, "2025-01-14": 18175.25})
    >>> result = validator.validate(series)
    >>> result.is_valid
    True
    """

    # Required rate columns for DataFrame input
    _RATE_COLS: List[str] = ["rate_open", "rate_high", "rate_low", "rate_close"]
    _ALT_RATE_COLS: List[str] = ["rate", "value", "close"]

    def __init__(self, config: Optional[ValidatorConfig] = None) -> None:
        super().__init__(config)
        logger.debug("ExchangeRateValidator initialised with %s", self.config)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def validate(
        self, data: Union[pd.DataFrame, pd.Series]
    ) -> ValidationResult:
        """
        Run the full validation pipeline.

        Parameters
        ----------
        data:
            ``pd.DataFrame`` (yfinance OHLCV) or ``pd.Series`` (FRED scalar).

        Returns
        -------
        ValidationResult
            Aggregated result with ``is_valid``, ``quality_score``,
            ``errors``, ``warnings``, ``checks_passed``, and
            ``anomaly_scores``.

        Raises
        ------
        TypeError
            If *data* is neither a ``DataFrame`` nor a ``Series``.
        """
        if isinstance(data, pd.Series):
            data = self._series_to_dataframe(data)
        elif not isinstance(data, pd.DataFrame):
            raise TypeError(
                f"Expected pd.DataFrame or pd.Series, got {type(data).__name__}."
            )

        result = ValidationResult()

        logger.info(
            "Starting validation: shape=%s columns=%s",
            data.shape,
            list(data.columns),
        )

        # Guard: empty input
        if data.empty:
            result.add_error("Input data is empty — nothing to validate.")
            result.quality_score = 0.0
            result.checks_passed = {c: False for c in (
                "null_check", "rate_range", "date_consistency",
                "anomaly_detection", "freshness"
            )}
            return result

        # Resolve rate columns present in this particular DataFrame
        rate_cols = self._resolve_rate_columns(data)
        if not rate_cols:
            result.add_error(
                f"No rate columns found. Expected one of: "
                f"{self._RATE_COLS + self._ALT_RATE_COLS}."
            )
            result.quality_score = 0.0
            return result

        logger.debug("Rate columns resolved: %s", rate_cols)

        check_scores: Dict[str, float] = {}

        # 1 — Null check
        null_ok, null_score = self.validate_null_values(data, rate_cols, result)
        result.checks_passed["null_check"] = null_ok
        check_scores["null_check"] = null_score

        # 2 — Rate range
        range_ok, range_score = self.validate_rate_range(data, rate_cols, result)
        result.checks_passed["rate_range"] = range_ok
        check_scores["rate_range"] = range_score

        # 3 — Date consistency
        date_ok, date_score = self.validate_date_consistency(data, result)
        result.checks_passed["date_consistency"] = date_ok
        check_scores["date_consistency"] = date_score

        # 4 — Anomaly detection
        anomaly_ok, anomaly_score = self.detect_anomalies(data, rate_cols, result)
        result.checks_passed["anomaly_detection"] = anomaly_ok
        check_scores["anomaly_detection"] = anomaly_score

        # 5 — Freshness
        fresh_ok, fresh_score = self.validate_freshness(data, result)
        result.checks_passed["freshness"] = fresh_ok
        check_scores["freshness"] = fresh_score

        # 6 — Quality score
        result.quality_score = self.calculate_quality_score(check_scores)
        result.details["check_scores"] = check_scores

        logger.info(
            "Validation done — is_valid=%s quality=%.3f checks=%s",
            result.is_valid,
            result.quality_score,
            result.checks_passed,
        )
        return result

    # ------------------------------------------------------------------
    # 1. Null / missing values
    # ------------------------------------------------------------------

    def validate_null_values(
        self,
        data: pd.DataFrame,
        rate_cols: List[str],
        result: ValidationResult,
    ) -> tuple[bool, float]:
        """
        Check the ratio of null / NaN values in rate columns.

        Parameters
        ----------
        data:
            The DataFrame to inspect.
        rate_cols:
            List of column names to check.
        result:
            Accumulates errors / warnings in place.

        Returns
        -------
        (passed: bool, score: float)
            ``score`` is ``1.0`` if no nulls, proportionally reduced
            based on the null ratio.
        """
        total_cells = sum(len(data[c]) for c in rate_cols)
        null_cells = sum(data[c].isna().sum() for c in rate_cols)

        if total_cells == 0:
            result.add_warning("Null check: no cells to evaluate.")
            return True, 1.0

        null_ratio = null_cells / total_cells
        score = max(0.0, 1.0 - null_ratio)

        logger.debug(
            "Null check: %d/%d cells null (ratio=%.4f, threshold=%.4f)",
            null_cells, total_cells, null_ratio, self.config.null_threshold,
        )

        result.details["null_count"] = int(null_cells)
        result.details["null_ratio"] = round(null_ratio, 6)

        if null_cells > 0:
            msg = (
                f"Null check: {null_cells} null value(s) found "
                f"(ratio={null_ratio:.2%}, threshold={self.config.null_threshold:.2%})."
            )
            if null_ratio > self.config.null_threshold and self.config.fail_on_null:
                result.add_error(msg)
                return False, score
            else:
                result.add_warning(msg)

        return True, score

    # ------------------------------------------------------------------
    # 2. Rate range
    # ------------------------------------------------------------------

    def validate_rate_range(
        self,
        data: pd.DataFrame,
        rate_cols: List[str],
        result: ValidationResult,
    ) -> tuple[bool, float]:
        """
        Validate that all rates fall within ``[config.rate_min, config.rate_max]``
        and have reasonable decimal precision (≤10 decimal places).

        Hard failure if any rate is zero, negative, or exceeds the maximum.

        Parameters
        ----------
        data, rate_cols, result:
            See :meth:`validate_null_values`.

        Returns
        -------
        (passed: bool, score: float)
        """
        total = 0
        out_of_range = 0
        bad_precision = 0
        passed = True

        for col in rate_cols:
            series = data[col].dropna()
            total += len(series)

            # Out-of-range check (hard fail)
            below = (series <= 0) | (series < self.config.rate_min)
            above = series > self.config.rate_max

            if below.any():
                bad_vals = series[below].tolist()[:5]  # show at most 5
                msg = (
                    f"Rate range [{col}]: {below.sum()} value(s) below "
                    f"minimum ({self.config.rate_min}). Sample: {bad_vals}."
                )
                result.add_error(msg)
                passed = False
                out_of_range += int(below.sum())

            if above.any():
                bad_vals = series[above].tolist()[:5]
                msg = (
                    f"Rate range [{col}]: {above.sum()} value(s) above "
                    f"maximum ({self.config.rate_max}). Sample: {bad_vals}."
                )
                result.add_error(msg)
                passed = False
                out_of_range += int(above.sum())

            # Precision check (soft warning — data is still usable)
            def _decimal_places(v: float) -> int:
                s = f"{v:.15f}".rstrip("0")
                dot = s.find(".")
                return len(s) - dot - 1 if dot != -1 else 0

            excessive = series.apply(lambda v: _decimal_places(v) > 10)
            if excessive.any():
                result.add_warning(
                    f"Rate range [{col}]: {excessive.sum()} value(s) "
                    f"have more than 10 decimal places."
                )
                bad_precision += int(excessive.sum())

        valid_cells = total - out_of_range
        score = valid_cells / total if total > 0 else 1.0

        result.details["rate_range_total"] = total
        result.details["rate_range_out_of_range"] = out_of_range
        result.details["rate_range_bad_precision"] = bad_precision

        logger.debug(
            "Rate range check: %d/%d valid, %d out-of-range, %d bad precision",
            valid_cells, total, out_of_range, bad_precision,
        )
        return passed, score

    # ------------------------------------------------------------------
    # 3. Date consistency
    # ------------------------------------------------------------------

    def validate_date_consistency(
        self,
        data: pd.DataFrame,
        result: ValidationResult,
    ) -> tuple[bool, float]:
        """
        Verify that:

        * A timestamp column exists (``timestamp``, ``date``, or the index).
        * Dates are parseable.
        * Dates are in chronological (ascending) order — **hard fail**.
        * No duplicate dates — **soft warning**.

        Parameters
        ----------
        data, result:
            See :meth:`validate_null_values`.

        Returns
        -------
        (passed: bool, score: float)
        """
        # Locate the date/timestamp column or index
        ts_series = self._resolve_timestamp_series(data)

        if ts_series is None:
            result.add_warning(
                "Date consistency: no timestamp/date column found — skipping."
            )
            return True, 1.0

        # Parse to datetime
        try:
            parsed = pd.to_datetime(ts_series, errors="coerce")
        except Exception as exc:
            result.add_error(f"Date consistency: could not parse timestamps — {exc}.")
            return False, 0.0

        unparseable = parsed.isna().sum()
        if unparseable > 0:
            result.add_error(
                f"Date consistency: {unparseable} unparseable date value(s)."
            )
            return False, 0.0

        passed = True
        score = 1.0

        # Chronological order (hard fail)
        if not parsed.is_monotonic_increasing:
            result.add_error(
                "Date consistency: timestamps are not in chronological "
                "(ascending) order."
            )
            passed = False
            score = 0.0

        # Duplicate detection (soft warning)
        duplicates = parsed.duplicated().sum()
        if duplicates > 0:
            result.add_warning(
                f"Date consistency: {duplicates} duplicate timestamp(s) found."
            )
            score = max(0.0, score - 0.2 * (duplicates / len(parsed)))

        result.details["date_min"] = str(parsed.min())
        result.details["date_max"] = str(parsed.max())
        result.details["date_duplicates"] = int(duplicates)
        result.details["date_count"] = len(parsed)

        logger.debug(
            "Date check: min=%s max=%s duplicates=%d ordered=%s",
            result.details["date_min"],
            result.details["date_max"],
            duplicates,
            passed,
        )
        return passed, score

    # ------------------------------------------------------------------
    # 4. Anomaly detection
    # ------------------------------------------------------------------

    def detect_anomalies(
        self,
        data: pd.DataFrame,
        rate_cols: List[str],
        result: ValidationResult,
    ) -> tuple[bool, float]:
        """
        Detect statistical outliers using Z-scores across all rate columns.

        A record is flagged as anomalous if the absolute Z-score of *any*
        rate value exceeds ``config.anomaly_std_dev``.

        The per-record ``anomaly_score`` in ``[0.0, 1.0]`` is computed as::

            anomaly_score = min(max_abs_z / (anomaly_std_dev * 2), 1.0)

        So a record exactly at the threshold scores 0.5; one at 2× the
        threshold scores 1.0.

        Parameters
        ----------
        data, rate_cols, result:
            See :meth:`validate_null_values`.

        Returns
        -------
        (passed: bool, score: float)
            ``passed`` is ``False`` only if ``config.fail_on_anomaly`` is
            ``True`` AND at least one anomaly was found.
        """
        if len(data) < 3:
            result.add_warning(
                "Anomaly detection: fewer than 3 rows — skipping statistical check."
            )
            return True, 1.0

        # Collect all numeric rate values for Z-score computation
        numeric = data[rate_cols].select_dtypes(include=[np.number])
        if numeric.empty:
            result.add_warning("Anomaly detection: no numeric rate columns to analyse.")
            return True, 1.0

        anomaly_records: Dict[str, float] = {}

        for idx, row in numeric.iterrows():
            max_z = 0.0
            for col in numeric.columns:
                col_vals = numeric[col].dropna()
                if len(col_vals) < 2:
                    continue
                mean = col_vals.mean()
                std = col_vals.std()
                if std == 0:
                    continue
                val = row[col]
                if pd.isna(val):
                    continue
                z = abs((val - mean) / std)
                max_z = max(max_z, z)

            if max_z > self.config.anomaly_std_dev:
                score_val = min(
                    max_z / (self.config.anomaly_std_dev * 2), 1.0
                )
                key = (
                    str(data.index[idx])
                    if not isinstance(idx, (int, np.integer))
                    else str(idx)
                )
                anomaly_records[key] = round(score_val, 4)

        result.anomaly_scores = anomaly_records
        anomaly_count = len(anomaly_records)

        overall_score = max(0.0, 1.0 - (anomaly_count / len(data)))

        result.details["anomaly_count"] = anomaly_count
        result.details["anomaly_ratio"] = round(anomaly_count / len(data), 6)

        logger.debug(
            "Anomaly detection: %d/%d records flagged (std_dev_threshold=%.1f)",
            anomaly_count, len(data), self.config.anomaly_std_dev,
        )

        if anomaly_count > 0:
            msg = (
                f"Anomaly detection: {anomaly_count} anomalous record(s) found "
                f"(Z-score threshold={self.config.anomaly_std_dev}). "
                f"Indices: {list(anomaly_records.keys())[:10]}."
            )
            if self.config.fail_on_anomaly:
                result.add_error(msg)
                return False, overall_score
            else:
                result.add_warning(msg)

        return True, overall_score

    # ------------------------------------------------------------------
    # 5. Freshness check
    # ------------------------------------------------------------------

    def validate_freshness(
        self,
        data: pd.DataFrame,
        result: ValidationResult,
    ) -> tuple[bool, float]:
        """
        Check that the most recent data point is within
        ``config.freshness_hours`` of the current UTC time.

        Soft warning by default — does not set ``is_valid = False``.

        Parameters
        ----------
        data, result:
            See :meth:`validate_null_values`.

        Returns
        -------
        (passed: bool, score: float)
            Score is ``1.0`` if fresh, ``0.0`` if older than 2× the threshold,
            linearly interpolated in between.
        """
        ts_series = self._resolve_timestamp_series(data)

        if ts_series is None:
            result.add_warning(
                "Freshness check: no timestamp column found — skipping."
            )
            return True, 1.0

        try:
            parsed = pd.to_datetime(ts_series, errors="coerce", utc=True)
        except Exception as exc:
            result.add_warning(f"Freshness check: could not parse timestamps — {exc}.")
            return True, 1.0

        valid_parsed = parsed.dropna()
        if valid_parsed.empty:
            result.add_warning("Freshness check: no valid timestamps to evaluate.")
            return True, 1.0

        most_recent: datetime = valid_parsed.max().to_pydatetime()
        now = datetime.now(timezone.utc)
        age_hours = (now - most_recent).total_seconds() / 3600

        threshold = self.config.freshness_hours
        # FRED monthly/annual data is expected to be older; apply 30-day
        # grace for non-hourly data heuristically (if max age > 48h)
        # — the caller should tune VALIDATOR_FRESHNESS_HOURS in .env
        max_age = threshold * 2

        score = max(0.0, min(1.0, 1.0 - (age_hours - threshold) / threshold)) \
            if age_hours > threshold else 1.0

        result.details["freshness_most_recent"] = most_recent.isoformat()
        result.details["freshness_age_hours"] = round(age_hours, 2)
        result.details["freshness_threshold_hours"] = threshold

        logger.debug(
            "Freshness check: most recent=%s age=%.2fh threshold=%.1fh",
            most_recent.isoformat(), age_hours, threshold,
        )

        if age_hours > threshold:
            msg = (
                f"Freshness check: most recent data is {age_hours:.1f}h old "
                f"(threshold={threshold}h)."
            )
            result.add_warning(msg)

        return True, score

    # ------------------------------------------------------------------
    # 6. Quality score
    # ------------------------------------------------------------------

    def calculate_quality_score(
        self, check_scores: Dict[str, float]
    ) -> float:
        """
        Compute the overall quality score as the unweighted average of all
        individual check scores.

        Parameters
        ----------
        check_scores:
            Dictionary mapping check name → score in ``[0.0, 1.0]``.

        Returns
        -------
        float
            A value in ``[0.0, 1.0]``. Returns ``0.0`` if *check_scores*
            is empty.

        Example
        -------
        >>> validator = ExchangeRateValidator()
        >>> validator.calculate_quality_score({
        ...     "null_check": 1.0,
        ...     "rate_range": 1.0,
        ...     "date_consistency": 0.8,
        ...     "anomaly_detection": 0.9,
        ...     "freshness": 1.0,
        ... })
        0.94
        """
        if not check_scores:
            return 0.0
        score = sum(check_scores.values()) / len(check_scores)
        rounded = round(score, 4)
        logger.debug(
            "Quality score: %.4f (from %d checks: %s)",
            rounded, len(check_scores), check_scores,
        )
        return rounded

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        """
        Convert a FRED-style ``pd.Series`` (index=dates, values=rates) into
        a normalised DataFrame with ``timestamp`` and ``rate`` columns, sorted
        chronologically.

        Parameters
        ----------
        series:
            A Series with date-string index and numeric rate values.

        Returns
        -------
        pd.DataFrame
            Columns: ``timestamp``, ``rate``.
        """
        df = series.reset_index()
        df.columns = pd.Index(["timestamp", "rate"])
        df = df.sort_values("timestamp").reset_index(drop=True)
        logger.debug(
            "Converted Series (%d rows) to DataFrame for validation.", len(df)
        )
        return df

    def _resolve_rate_columns(self, data: pd.DataFrame) -> List[str]:
        """Return the subset of known rate columns present in *data*."""
        present = [c for c in self._RATE_COLS if c in data.columns]
        if present:
            return present
        # Fall back to alternative column names
        return [c for c in self._ALT_RATE_COLS if c in data.columns]

    def _resolve_timestamp_series(
        self, data: pd.DataFrame
    ) -> Optional[pd.Series]:
        """
        Return a Series of raw timestamp values from either a ``timestamp``
        column, a ``date`` column, or the DataFrame index (if index looks
        date-like).
        """
        for col in ("timestamp", "date"):
            if col in data.columns:
                return data[col]

        # Try the index if it's not a plain RangeIndex
        if not isinstance(data.index, pd.RangeIndex):
            return pd.Series(data.index.astype(str), name="index_as_ts")

        return None