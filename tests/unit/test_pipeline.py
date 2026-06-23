"""
tests/unit/test_pipeline.py
=============================
Comprehensive unit tests for src/etl/pipeline.py.

Coverage targets (>80%)
-----------------------
  PipelineConfig          - env var loading, defaults, explicit overrides, repr
  ExtractResult / ValidateResult / LoadResult / PipelineResult
                          - dataclass defaults, repr, add_error/add_warning
  BasePipeline            - abstract contract (covered indirectly via EtlPipeline)
  EtlPipeline.validate_inputs - all validation branches
  EtlPipeline.extract     - success, extractor exception, partial errors,
                            timeout warning
  EtlPipeline.validate    - success, per-pair grouping, validator exception,
                            quality averaging, invalid-record exclusion
  EtlPipeline.load        - success, missing FK keys, retry-with-split,
                            exhausted retries, exception during load
  EtlPipeline.run         - full happy path, stage-level error propagation,
                            input validation short-circuit, empty extract,
                            empty validate, session ownership (owned vs
                            caller-provided)
  EtlPipeline.handle_errors - critical vs non-critical, rollback behaviour
  EtlPipeline.log_metrics / _build_metrics - structure and content
  EtlPipeline.generate_report - full report structure, recommendations

Mocking strategy
-----------------
This test suite treats ``ExchangeRateExtractor``, ``ExchangeRateValidator``,
and ``ExchangeRateLoader`` as black boxes (mocked at the boundary) since
their own internal logic is already covered by test_extractors.py,
test_validators.py, and test_loaders.py respectively. These tests focus
exclusively on **pipeline orchestration**: stage sequencing, error
propagation, retry/split logic, metrics, and reporting.

Fixtures
--------
``engine`` / ``session`` come from tests/conftest.py (in-memory SQLite,
schema created once per session, each test wrapped in a rolled-back
transaction for isolation) — used only for tests that exercise the real
``load()`` stage end-to-end with a real ``ExchangeRateLoader``.
"""

from __future__ import annotations

import logging
import sys
import pathlib
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Path bootstrap (mirrors conftest.py behaviour for standalone runs)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(pathlib.Path(__file__).parents[2] / "src"))

from src.etl.loaders import ExchangeRateLoader
from src.etl.loaders import LoadResult as LoaderRunResult
from src.etl.pipeline import (
    BasePipeline,
    EtlPipeline,
    ExtractResult,
    LoadResult,
    PipelineConfig,
    PipelineResult,
    ValidateResult,
)
from src.etl.validators import ExchangeRateValidator, ValidationResult
from src.utils.database import ApiSource, Currency

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIXED_TS = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Builder helpers
# ---------------------------------------------------------------------------

def make_raw_rate(
    pair: str = "USD_IDR",
    rate: float = 16000.0,
    timestamp: datetime = FIXED_TS,
    source: str = "yfinance",
    quality: float = 1.0,
    from_currency_id: int = 1,
    to_currency_id: int = 2,
) -> dict:
    """Build a raw rate dict as returned by ExchangeRateExtractor.fetch_rates().

    Includes ``from_currency_id`` / ``to_currency_id`` so the dict survives
    the pipeline's load-stage FK check without a real currency-lookup step.
    """
    return {
        "pair": pair,
        "rate": rate,
        "timestamp": timestamp.isoformat(),
        "source": source,
        "fetched_at": timestamp.isoformat(),
        "data_quality_score": quality,
        # FK IDs — in production these would be resolved by a currency-lookup
        # step between validate and load; in tests we pre-populate them so the
        # happy-path pipeline run reaches the load stage without a DB lookup.
        "from_currency_id": from_currency_id,
        "to_currency_id": to_currency_id,
    }


def make_extraction_response(
    rates: list | None = None,
    errors: list | None = None,
    source: str = "yfinance",
) -> dict:
    """Build a dict matching ExchangeRateExtractor.fetch_rates()'s return shape."""
    return {
        "rates": rates if rates is not None else [make_raw_rate()],
        "fetched_at": FIXED_TS.isoformat(),
        "source": source,
        "errors": errors if errors is not None else [],
    }


def make_validation_result(
    is_valid: bool = True,
    quality_score: float = 0.95,
    errors: list | None = None,
    warnings: list | None = None,
) -> ValidationResult:
    """Build a ValidationResult as ExchangeRateValidator.validate() would return."""
    return ValidationResult(
        is_valid=is_valid,
        quality_score=quality_score,
        errors=errors or [],
        warnings=warnings or [],
        checks_passed={"null_check": True, "rate_range": True},
        anomaly_scores={},
        details={},
    )


def make_loader_result(
    success: bool = True,
    rows_loaded: int = 1,
    rows_updated: int = 0,
    rows_skipped: int = 0,
    rows_failed: int = 0,
    errors: list | None = None,
) -> LoaderRunResult:
    """Build a loaders.LoadResult as ExchangeRateLoader.load() would return."""
    return LoaderRunResult(
        success=success,
        rows_loaded=rows_loaded,
        rows_updated=rows_updated,
        rows_skipped=rows_skipped,
        rows_failed=rows_failed,
        errors=errors or [],
        warnings=[],
        execution_time_ms=10,
    )


def make_loadable_rate(
    pair: str = "USD_IDR",
    from_id: int = 1,
    to_id: int = 2,
    rate: float = 16000.0,
    timestamp: datetime = FIXED_TS,
    quality_score: float = 0.95,
) -> dict:
    """Build a validated rate dict WITH currency FK ids attached (load-ready)."""
    return {
        "pair": pair,
        "from_currency_id": from_id,
        "to_currency_id": to_id,
        "rate": rate,
        "timestamp": timestamp,
        "quality_score": quality_score,
    }


@pytest.fixture
def mock_extractor():
    """A MagicMock standing in for ExchangeRateExtractor."""
    extractor = MagicMock()
    extractor.fetch_rates.return_value = make_extraction_response()
    return extractor


@pytest.fixture
def mock_validator():
    """A MagicMock standing in for ExchangeRateValidator."""
    validator = MagicMock(spec=ExchangeRateValidator)
    validator.validate.return_value = make_validation_result()
    return validator


@pytest.fixture
def mock_loader():
    """A MagicMock standing in for ExchangeRateLoader."""
    loader = MagicMock(spec=ExchangeRateLoader)
    loader.load.return_value = make_loader_result()
    return loader


@pytest.fixture
def pipeline(mock_extractor, mock_validator, mock_loader):
    """EtlPipeline wired with mocked extractor/validator/loader."""
    config = PipelineConfig(
        batch_size=1000,
        extract_timeout=300,
        validate_quality_threshold=0.7,
        load_retry_count=3,
        stop_on_error=False,
        log_level="DEBUG",
    )
    return EtlPipeline(
        config=config,
        extractor=mock_extractor,
        validator=mock_validator,
        loader=mock_loader,
    )


def _seed_currencies_and_source(session):
    """Insert USD, IDR, yfinance for tests that exercise the real DB load."""
    usd = Currency(code="USD", name="US Dollar", is_active=True)
    idr = Currency(code="IDR", name="Indonesian Rupiah", is_active=True)
    src = ApiSource(source_name="yfinance", is_active=True)
    session.add_all([usd, idr, src])
    session.flush()
    return usd, idr, src


# ===========================================================================
# PipelineConfig
# ===========================================================================

class TestPipelineConfig:

    def test_default_values(self, monkeypatch):
        for key in (
            "PIPELINE_BATCH_SIZE", "PIPELINE_EXTRACT_TIMEOUT",
            "PIPELINE_VALIDATE_QUALITY_THRESHOLD", "PIPELINE_LOAD_RETRY_COUNT",
            "PIPELINE_STOP_ON_ERROR", "PIPELINE_LOG_LEVEL",
        ):
            monkeypatch.delenv(key, raising=False)

        cfg = PipelineConfig()
        assert cfg.batch_size == 1000
        assert cfg.extract_timeout == 300
        assert cfg.validate_quality_threshold == pytest.approx(0.7)
        assert cfg.load_retry_count == 3
        assert cfg.stop_on_error is False
        assert cfg.log_level == "INFO"

    def test_explicit_override_beats_env(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_BATCH_SIZE", "500")
        cfg = PipelineConfig(batch_size=42)
        assert cfg.batch_size == 42

    def test_invalid_float_env_falls_back(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_VALIDATE_QUALITY_THRESHOLD", "not_a_float")
        cfg = PipelineConfig()
        assert cfg.validate_quality_threshold == pytest.approx(0.7)

    def test_stop_on_error_from_env(self, monkeypatch):
        monkeypatch.setenv("PIPELINE_STOP_ON_ERROR", "true")
        cfg = PipelineConfig()
        assert cfg.stop_on_error is True

    def test_repr_contains_key_fields(self):
        cfg = PipelineConfig(batch_size=10)
        r = repr(cfg)
        assert "batch_size" in r
        assert "stop_on_error" in r


# ===========================================================================
# Stage result dataclasses
# ===========================================================================

class TestExtractResult:

    def test_defaults(self):
        r = ExtractResult()
        assert r.success is True
        assert r.records_fetched == 0
        assert r.extract_errors == []
        assert r.raw_rates == []
        assert r.duration_seconds == pytest.approx(0.0)

    def test_repr(self):
        r = ExtractResult(records_fetched=3, extract_errors=["e1"])
        text = repr(r)
        assert "fetched=3" in text
        assert "errors=1" in text


class TestValidateResult:

    def test_defaults(self):
        r = ValidateResult()
        assert r.records_valid == 0
        assert r.records_invalid == 0
        assert r.quality_avg == pytest.approx(0.0)

    def test_repr(self):
        r = ValidateResult(records_valid=2, records_invalid=1, quality_avg=0.85)
        text = repr(r)
        assert "valid=2" in text
        assert "invalid=1" in text


class TestStageLoadResult:
    """Tests for pipeline.LoadResult (distinct from loaders.LoadResult)."""

    def test_defaults(self):
        r = LoadResult()
        assert r.records_loaded == 0
        assert r.records_updated == 0
        assert r.records_skipped == 0
        assert r.retries_used == 0
        assert r.loader_result is None

    def test_repr(self):
        r = LoadResult(records_loaded=5, records_updated=1, retries_used=2)
        text = repr(r)
        assert "loaded=5" in text
        assert "retries=2" in text


class TestPipelineResult:

    def test_result_creation_defaults(self):
        r = PipelineResult()
        assert r.success is True
        assert r.stages == {}
        assert r.total_records == 0
        assert r.processed_records == 0
        assert r.failed_records == 0
        assert r.errors == []
        assert r.warnings == []

    def test_result_success_status_true(self):
        r = PipelineResult(success=True, processed_records=10, total_records=10)
        assert r.success is True

    def test_result_success_status_false(self):
        r = PipelineResult(success=False)
        r.add_error("something broke")
        assert r.success is False
        assert "something broke" in r.errors

    def test_add_error_and_warning(self):
        r = PipelineResult()
        r.add_error("err1")
        r.add_warning("warn1")
        assert r.errors == ["err1"]
        assert r.warnings == ["warn1"]

    def test_repr_contains_counts(self):
        r = PipelineResult(total_records=10, processed_records=8, failed_records=2)
        text = repr(r)
        assert "total=10" in text
        assert "processed=8" in text
        assert "failed=2" in text


# ===========================================================================
# EtlPipeline.validate_inputs
# ===========================================================================

class TestValidateInputs:

    def test_valid_inputs_no_raise(self, pipeline):
        pipeline.validate_inputs(["USD_IDR"], "yfinance", 1)

    def test_empty_pairs_raises(self, pipeline):
        with pytest.raises(ValueError, match="must not be empty"):
            pipeline.validate_inputs([], "yfinance", 1)

    def test_non_list_pairs_raises(self, pipeline):
        with pytest.raises(ValueError, match="must be a list"):
            pipeline.validate_inputs("USD_IDR", "yfinance", 1)  # type: ignore

    def test_non_string_pair_elements_raises(self, pipeline):
        with pytest.raises(ValueError, match="must be a list of strings"):
            pipeline.validate_inputs([123], "yfinance", 1)  # type: ignore

    def test_unknown_source_raises(self, pipeline):
        with pytest.raises(ValueError, match="Unknown source"):
            pipeline.validate_inputs(["USD_IDR"], "bloomberg", 1)

    def test_invalid_source_id_zero_raises(self, pipeline):
        with pytest.raises(ValueError, match="positive integer"):
            pipeline.validate_inputs(["USD_IDR"], "yfinance", 0)

    def test_invalid_source_id_negative_raises(self, pipeline):
        with pytest.raises(ValueError, match="positive integer"):
            pipeline.validate_inputs(["USD_IDR"], "yfinance", -5)

    def test_start_after_end_date_raises(self, pipeline):
        with pytest.raises(ValueError, match="must not be after"):
            pipeline.validate_inputs(
                ["USD_IDR"], "yfinance", 1,
                start_date=date(2025, 2, 1), end_date=date(2025, 1, 1),
            )

    def test_valid_date_range_no_raise(self, pipeline):
        pipeline.validate_inputs(
            ["USD_IDR"], "yfinance", 1,
            start_date=date(2025, 1, 1), end_date=date(2025, 2, 1),
        )


# ===========================================================================
# EtlPipeline.extract
# ===========================================================================

class TestExtractStage:

    def test_extract_success(self, pipeline, mock_extractor):
        mock_extractor.fetch_rates.return_value = make_extraction_response(
            rates=[make_raw_rate(), make_raw_rate(pair="EUR_IDR")]
        )
        result = pipeline.extract(["USD_IDR", "EUR_IDR"], "yfinance")

        assert result.success is True
        assert result.records_fetched == 2
        assert result.extract_errors == []
        assert len(result.raw_rates) == 2

    def test_extract_with_partial_errors(self, pipeline, mock_extractor):
        mock_extractor.fetch_rates.return_value = make_extraction_response(
            rates=[make_raw_rate()],
            errors=["[yfinance] Validation error for EUR_IDR: timeout"],
        )
        result = pipeline.extract(["USD_IDR", "EUR_IDR"], "yfinance")

        assert result.records_fetched == 1
        assert len(result.extract_errors) == 1
        assert result.success is True  # at least one record succeeded

    def test_extract_zero_records_marks_failure(self, pipeline, mock_extractor):
        mock_extractor.fetch_rates.return_value = make_extraction_response(
            rates=[], errors=["all pairs failed"],
        )
        result = pipeline.extract(["USD_IDR"], "yfinance")
        assert result.success is False
        assert result.records_fetched == 0

    def test_extract_extractor_raises_exception(self, pipeline, mock_extractor):
        mock_extractor.fetch_rates.side_effect = ConnectionError("network down")
        result = pipeline.extract(["USD_IDR"], "yfinance")

        assert result.success is False
        assert any("unexpected exception" in e for e in result.extract_errors)
        assert result.records_fetched == 0

    def test_extract_duration_recorded(self, pipeline):
        result = pipeline.extract(["USD_IDR"], "yfinance")
        assert result.duration_seconds >= 0

    def test_extract_timeout_warning_logged(self, pipeline, mock_extractor, caplog):
        """If extract stage exceeds extract_timeout, a warning is logged (non-fatal)."""
        pipeline.config.extract_timeout = -1  # guarantee threshold exceeded
        with caplog.at_level(logging.WARNING):
            result = pipeline.extract(["USD_IDR"], "yfinance")
        assert result.success is True  # timeout is a soft warning, not a failure
        assert any("exceeding" in r.message for r in caplog.records)

    def test_extract_uses_injected_extractor_not_get_extractor(self, pipeline, mock_extractor):
        """Pipeline must use the injected extractor, not call get_extractor()."""
        with patch("src.etl.pipeline.get_extractor") as mock_get:
            pipeline.extract(["USD_IDR"], "yfinance")
            mock_get.assert_not_called()


# ===========================================================================
# EtlPipeline.validate
# ===========================================================================

class TestValidateStage:

    def test_validate_success(self, pipeline, mock_validator):
        mock_validator.validate.return_value = make_validation_result(
            is_valid=True, quality_score=0.95,
        )
        raw = [make_raw_rate()]
        result = pipeline.validate(raw)

        assert result.records_valid == 1
        assert result.records_invalid == 0
        assert result.quality_avg == pytest.approx(0.95)
        assert len(result.validated_rates) == 1
        assert result.validated_rates[0]["quality_score"] == pytest.approx(0.95)

    def test_validate_empty_input(self, pipeline):
        result = pipeline.validate([])
        assert result.records_valid == 0
        assert result.validated_rates == []

    def test_validate_invalid_records_excluded(self, pipeline, mock_validator):
        mock_validator.validate.return_value = make_validation_result(
            is_valid=False, quality_score=0.3, errors=["rate out of range"],
        )
        raw = [make_raw_rate()]
        result = pipeline.validate(raw)

        assert result.records_valid == 0
        assert result.records_invalid == 1
        assert result.validated_rates == []
        assert any("rate out of range" in e for e in result.validation_errors)

    def test_validate_groups_by_pair(self, pipeline, mock_validator):
        """Mixed pairs must trigger one validate() call per distinct pair."""
        raw = [
            make_raw_rate(pair="USD_IDR"),
            make_raw_rate(pair="USD_IDR", timestamp=FIXED_TS + timedelta(hours=1)),
            make_raw_rate(pair="EUR_IDR"),
        ]
        pipeline.validate(raw)
        assert mock_validator.validate.call_count == 2  # USD_IDR, EUR_IDR

    def test_validate_validator_raises_exception(self, pipeline, mock_validator):
        mock_validator.validate.side_effect = RuntimeError("validator crashed")
        raw = [make_raw_rate()]
        result = pipeline.validate(raw)

        assert result.records_invalid == 1
        assert any("unexpected exception" in e for e in result.validation_errors)

    def test_validate_warnings_propagated(self, pipeline, mock_validator):
        mock_validator.validate.return_value = make_validation_result(
            is_valid=True, quality_score=0.8, warnings=["data slightly stale"],
        )
        raw = [make_raw_rate()]
        result = pipeline.validate(raw)
        assert any("data slightly stale" in w for w in result.validation_warnings)

    def test_validate_quality_avg_multiple_pairs(self, pipeline, mock_validator):
        responses = iter([
            make_validation_result(quality_score=1.0),
            make_validation_result(quality_score=0.5),
        ])
        mock_validator.validate.side_effect = lambda df: next(responses)

        raw = [make_raw_rate(pair="USD_IDR"), make_raw_rate(pair="EUR_IDR")]
        result = pipeline.validate(raw)
        assert result.quality_avg == pytest.approx(0.75)

    def test_validate_duration_recorded(self, pipeline):
        result = pipeline.validate([make_raw_rate()])
        assert result.duration_seconds >= 0


# ===========================================================================
# EtlPipeline.load
# ===========================================================================

class TestLoadStage:

    def test_load_success(self, pipeline, mock_loader, session):
        mock_loader.load.return_value = make_loader_result(rows_loaded=1)
        data = [make_loadable_rate()]

        result = pipeline.load(session, data, source_id=1)

        assert result.success is True
        assert result.records_loaded == 1
        assert result.retries_used == 0

    def test_load_empty_data(self, pipeline, session):
        result = pipeline.load(session, [], source_id=1)
        assert result.records_loaded == 0
        assert result.success is True

    def test_load_missing_fk_keys_recorded_as_error(self, pipeline, mock_loader, session):
        """Rows without from_currency_id/to_currency_id must not reach the loader."""
        data = [{"pair": "USD_IDR", "rate": 16000.0, "timestamp": FIXED_TS}]
        result = pipeline.load(session, data, source_id=1)

        assert any("missing from_currency_id" in e for e in result.load_errors)
        mock_loader.load.assert_not_called()

    def test_load_retry_splits_batch_on_failure(self, pipeline, mock_loader, session):
        """A failed batch of 2 should be split into two batches of 1."""
        call_results = iter([
            make_loader_result(success=False, rows_loaded=0, errors=["batch failed"]),
            make_loader_result(success=True, rows_loaded=1),
            make_loader_result(success=True, rows_loaded=1),
        ])
        mock_loader.load.side_effect = lambda *a, **kw: next(call_results)

        data = [make_loadable_rate(timestamp=FIXED_TS),
                make_loadable_rate(timestamp=FIXED_TS + timedelta(hours=1))]

        result = pipeline.load(session, data, source_id=1)

        assert mock_loader.load.call_count == 3  # 1 failed + 2 split retries
        assert result.retries_used >= 1
        assert result.records_loaded == 2

    def test_load_exhausted_retries_single_record_recorded_as_error(self, pipeline, mock_loader, session):
        """A single record that keeps failing must end up in load_errors, not loop forever."""
        mock_loader.load.return_value = make_loader_result(
            success=False, rows_loaded=0, errors=["persistent failure"],
        )
        data = [make_loadable_rate()]

        result = pipeline.load(session, data, source_id=1)

        # Must terminate (not hang) and record the failure
        assert any(
            "persistent failure" in e or "exhausting retries" in e
            for e in result.load_errors
        )

    def test_load_exception_during_load_triggers_retry(self, pipeline, mock_loader, session):
        mock_loader.load.side_effect = ConnectionError("db connection lost")
        data = [make_loadable_rate()]

        result = pipeline.load(session, data, source_id=1)

        assert any("raised an exception" in e for e in result.load_errors)

    def test_load_single_record_permanent_failure_sets_success_false(self, pipeline, mock_loader, session):
        """
        Regression test: a single record that fails (no peers to split with,
        rows_loaded=0) must mark LoadResult.success=False — it must NOT be
        inferred as success merely because no exception was raised.
        """
        mock_loader.load.return_value = make_loader_result(
            success=False, rows_loaded=0, rows_updated=0, rows_skipped=0,
            errors=["disk full"],
        )
        data = [make_loadable_rate()]

        result = pipeline.load(session, data, source_id=1)

        assert result.success is False
        assert result.records_loaded == 0
        assert result.records_updated == 0

    def test_load_duration_recorded(self, pipeline, session):
        result = pipeline.load(session, [make_loadable_rate()], source_id=1)
        assert result.duration_seconds >= 0

    def test_load_stores_loader_result(self, pipeline, mock_loader, session):
        loader_result = make_loader_result(rows_loaded=1)
        mock_loader.load.return_value = loader_result
        result = pipeline.load(session, [make_loadable_rate()], source_id=1)
        assert result.loader_result is loader_result


# ===========================================================================
# EtlPipeline.load — real loader integration (idempotency through pipeline)
# ===========================================================================

class TestLoadIdempotencyViaPipeline:
    """Uses a REAL ExchangeRateLoader (not mocked) against in-memory SQLite
    to verify the pipeline's load() stage produces idempotent results."""

    def test_pipeline_load_idempotency(self, session):
        usd, idr, src = _seed_currencies_and_source(session)
        real_loader = ExchangeRateLoader()
        pipeline_with_real_loader = EtlPipeline(loader=real_loader)

        data = [make_loadable_rate(
            from_id=usd.currency_id, to_id=idr.currency_id, quality_score=0.9,
        )]

        first = pipeline_with_real_loader.load(session, data, source_id=src.source_id)
        second = pipeline_with_real_loader.load(session, data, source_id=src.source_id)

        assert first.records_loaded == 1
        # Same quality score on re-send -> counted as an update (idempotent, no duplicate row)
        assert second.records_updated == 1

        from src.utils.database import ExchangeRate
        assert session.query(ExchangeRate).count() == 1


# ===========================================================================
# EtlPipeline.handle_errors
# ===========================================================================

class TestHandleErrors:

    def test_non_critical_logs_warning(self, pipeline, caplog):
        with caplog.at_level(logging.WARNING):
            pipeline.handle_errors(["minor issue"], critical=False)
        assert any("minor issue" in r.message for r in caplog.records)
        assert all(r.levelno == logging.WARNING for r in caplog.records if "minor issue" in r.message)

    def test_critical_logs_error(self, pipeline, caplog):
        with caplog.at_level(logging.ERROR):
            pipeline.handle_errors(["fatal issue"], critical=True)
        assert any("fatal issue" in r.message for r in caplog.records)

    def test_critical_rolls_back_session(self, pipeline):
        mock_session = MagicMock()
        pipeline.handle_errors(["fatal"], session=mock_session, critical=True)
        mock_session.rollback.assert_called_once()

    def test_non_critical_does_not_roll_back(self, pipeline):
        mock_session = MagicMock()
        pipeline.handle_errors(["minor"], session=mock_session, critical=False)
        mock_session.rollback.assert_not_called()

    def test_rollback_failure_does_not_propagate(self, pipeline, caplog):
        mock_session = MagicMock()
        mock_session.rollback.side_effect = Exception("rollback failed too")
        with caplog.at_level(logging.ERROR):
            pipeline.handle_errors(["fatal"], session=mock_session, critical=True)  # must not raise
        assert any("rollback itself failed" in r.message for r in caplog.records)


# ===========================================================================
# EtlPipeline.run — full orchestration
# ===========================================================================

class TestPipelineRun:

    def test_pipeline_run_success(self, pipeline, mock_extractor, mock_validator, mock_loader, session):
        mock_extractor.fetch_rates.return_value = make_extraction_response(
            rates=[make_raw_rate()]
        )
        mock_validator.validate.return_value = make_validation_result(
            is_valid=True, quality_score=0.95,
        )
        mock_loader.load.return_value = make_loader_result(rows_loaded=1)

        result = pipeline.run(
            currency_pairs=["USD_IDR"], source="yfinance", source_id=1, session=session,
        )

        assert result.success is True
        assert "extract" in result.stages
        assert "validate" in result.stages
        assert "load" in result.stages
        assert result.total_records == 1
        assert result.duration_seconds >= 0

    def test_pipeline_run_with_invalid_inputs_short_circuits(self, pipeline, mock_extractor):
        result = pipeline.run(currency_pairs=[], source="yfinance", source_id=1)

        assert result.success is False
        assert any("Input validation failed" in e for e in result.errors)
        mock_extractor.fetch_rates.assert_not_called()

    def test_pipeline_run_with_extract_error(self, pipeline, mock_extractor):
        mock_extractor.fetch_rates.return_value = make_extraction_response(
            rates=[], errors=["yfinance unreachable"],
        )
        result = pipeline.run(currency_pairs=["USD_IDR"], source="yfinance", source_id=1)

        assert result.success is False
        assert "extract" in result.stages
        assert "validate" not in result.stages  # pipeline stopped after extract

    def test_pipeline_run_with_validate_error(self, pipeline, mock_validator, session):
        mock_validator.validate.return_value = make_validation_result(
            is_valid=False, quality_score=0.1, errors=["hard fail"],
        )
        result = pipeline.run(
            currency_pairs=["USD_IDR"], source="yfinance", source_id=1, session=session,
        )

        assert result.success is False
        assert "validate" in result.stages
        assert "load" not in result.stages  # no loadable rates -> stopped

    def test_pipeline_run_with_load_error(self, pipeline, mock_loader, session):
        mock_loader.load.return_value = make_loader_result(
            success=False, rows_loaded=0, errors=["disk full"],
        )
        result = pipeline.run(
            currency_pairs=["USD_IDR"], source="yfinance", source_id=1, session=session,
        )

        assert "load" in result.stages
        assert result.success is False

    def test_pipeline_extract_multiple_sources(self, mock_validator, mock_loader, session):
        """Run the pipeline twice with two different injected extractors/sources."""
        yf_extractor = MagicMock()
        yf_extractor.fetch_rates.return_value = make_extraction_response(
            rates=[make_raw_rate(pair="USD_IDR", source="yfinance")], source="yfinance",
        )
        fred_extractor = MagicMock()
        fred_extractor.fetch_rates.return_value = make_extraction_response(
            rates=[make_raw_rate(pair="USD_IDR", source="fred")], source="fred",
        )

        yf_pipeline = EtlPipeline(extractor=yf_extractor, validator=mock_validator, loader=mock_loader)
        fred_pipeline = EtlPipeline(extractor=fred_extractor, validator=mock_validator, loader=mock_loader)

        yf_result = yf_pipeline.run(["USD_IDR"], source="yfinance", source_id=1, session=session)
        fred_result = fred_pipeline.run(["USD_IDR"], source="fred", source_id=2, session=session)

        assert yf_result.stages["extract"].records_fetched == 1
        assert fred_result.stages["extract"].records_fetched == 1
        yf_extractor.fetch_rates.assert_called_once()
        fred_extractor.fetch_rates.assert_called_once()

    def test_pipeline_validate_quality_filtering(self, pipeline, mock_validator, session):
        """Low average quality should produce a warning but still proceed."""
        mock_validator.validate.return_value = make_validation_result(
            is_valid=True, quality_score=0.4,  # below default threshold 0.7
        )
        result = pipeline.run(
            currency_pairs=["USD_IDR"], source="yfinance", source_id=1, session=session,
        )
        assert any("below the configured threshold" in w for w in result.warnings)

    def test_pipeline_error_recovery_continues_with_partial_extract_errors(
        self, pipeline, mock_extractor, session,
    ):
        """Non-critical extract errors (some pairs failed) should not abort the run."""
        mock_extractor.fetch_rates.return_value = make_extraction_response(
            rates=[make_raw_rate()],
            errors=["EUR_IDR failed: timeout"],
        )
        result = pipeline.run(
            currency_pairs=["USD_IDR", "EUR_IDR"], source="yfinance", source_id=1, session=session,
        )
        assert "validate" in result.stages  # pipeline continued despite partial error
        assert any("non-critical error" in w for w in result.warnings)

    def test_pipeline_metrics_tracking(self, pipeline, session):
        result = pipeline.run(
            currency_pairs=["USD_IDR"], source="yfinance", source_id=1, session=session,
        )
        metrics = pipeline._build_metrics(result)

        assert "pipeline" in metrics
        assert "extract" in metrics
        assert "validate" in metrics
        assert "load" in metrics
        assert metrics["pipeline"]["total_records"] == result.total_records
        assert "success_rate" in metrics["pipeline"]

    def test_pipeline_logging(self, pipeline, session, caplog):
        with caplog.at_level(logging.INFO):
            pipeline.run(
                currency_pairs=["USD_IDR"], source="yfinance", source_id=1, session=session,
            )
        assert any("Pipeline run started" in r.message for r in caplog.records)
        assert any("Pipeline run finished" in r.message for r in caplog.records)

    def test_pipeline_run_unhandled_exception_caught(self, pipeline, mock_extractor, session):
        mock_extractor.fetch_rates.side_effect = MemoryError("out of memory")
        result = pipeline.run(
            currency_pairs=["USD_IDR"], source="yfinance", source_id=1, session=session,
        )
        # extract() itself catches the exception and returns success=False,
        # so run() sees zero raw_rates and aborts gracefully (no crash).
        assert result.success is False

    def test_pipeline_run_owns_session_when_none_provided(self, pipeline):
        """When no session is passed, the pipeline must open and close its own."""
        with patch("src.utils.database.get_engine") as mock_get_engine, \
             patch("src.utils.database.get_session") as mock_get_session:

            mock_cm = MagicMock()
            mock_cm.__enter__.return_value = MagicMock()
            mock_cm.__exit__.return_value = False
            mock_get_session.return_value = mock_cm

            pipeline.run(currency_pairs=["USD_IDR"], source="yfinance", source_id=1)

            mock_get_engine.assert_called_once()
            mock_get_session.assert_called_once()
            mock_cm.__enter__.assert_called_once()
            mock_cm.__exit__.assert_called_once()


# ===========================================================================
# EtlPipeline.generate_report
# ===========================================================================

class TestGenerateReport:

    def test_report_contains_header(self, pipeline):
        result = PipelineResult(success=True, total_records=5, processed_records=5)
        report = pipeline.generate_report(result)
        assert "ETL Pipeline Report" in report
        assert "SUCCESS" in report

    def test_report_shows_failed_status(self, pipeline):
        result = PipelineResult(success=False)
        report = pipeline.generate_report(result)
        assert "FAILED" in report

    def test_report_includes_stage_details(self, pipeline, session):
        result = pipeline.run(
            currency_pairs=["USD_IDR"], source="yfinance", source_id=1, session=session,
        )
        report = pipeline.generate_report(result)
        assert "Extract" in report
        assert "Validate" in report
        assert "Load" in report

    def test_report_includes_errors_section(self, pipeline):
        result = PipelineResult(success=False)
        result.add_error("something failed")
        report = pipeline.generate_report(result)
        assert "Errors" in report
        assert "something failed" in report

    def test_report_includes_warnings_section(self, pipeline):
        result = PipelineResult()
        result.add_warning("low quality data")
        report = pipeline.generate_report(result)
        assert "Warnings" in report
        assert "low quality data" in report

    def test_report_no_errors_section_when_clean(self, pipeline):
        result = PipelineResult(success=True)
        report = pipeline.generate_report(result)
        assert "--- Errors" not in report

    def test_report_includes_recommendations_on_failure(self, pipeline):
        result = PipelineResult(success=False)
        report = pipeline.generate_report(result)
        assert "Recommendations" in report
        assert "Pipeline failed" in report

    def test_report_no_action_needed_when_healthy(self, pipeline):
        result = PipelineResult(success=True, total_records=1, processed_records=1)
        report = pipeline.generate_report(result)
        assert "No action needed" in report

    def test_report_recommends_on_low_quality(self, pipeline):
        result = PipelineResult(success=True)
        result.stages["validate"] = ValidateResult(quality_avg=0.3)
        report = pipeline.generate_report(result)
        assert "low" in report.lower()

    def test_report_recommends_on_retries(self, pipeline):
        result = PipelineResult(success=True)
        result.stages["load"] = LoadResult(retries_used=2)
        report = pipeline.generate_report(result)
        assert "retr" in report.lower()

    def test_report_success_rate_calculation(self, pipeline):
        result = PipelineResult(total_records=10, processed_records=8)
        report = pipeline.generate_report(result)
        assert "80.0%" in report

    def test_report_zero_total_records_no_division_error(self, pipeline):
        result = PipelineResult(total_records=0, processed_records=0)
        report = pipeline.generate_report(result)  # must not raise ZeroDivisionError
        assert "0.0%" in report


# ===========================================================================
# BasePipeline abstract contract
# ===========================================================================

class TestBasePipelineContract:

    def test_cannot_instantiate_base_directly(self):
        with pytest.raises(TypeError):
            BasePipeline()  # type: ignore

    def test_etl_pipeline_is_a_base_pipeline(self, pipeline):
        assert isinstance(pipeline, BasePipeline)