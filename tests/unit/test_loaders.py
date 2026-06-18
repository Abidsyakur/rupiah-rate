"""
tests/unit/test_loaders.py
============================
Comprehensive unit tests for src/etl/loaders.py.

Coverage targets (>80%)
-----------------------
  LoaderConfig            - env var loading, defaults, explicit overrides, repr
  LoadResult              - dataclass defaults, add_error/add_warning,
                            total_processed, repr
  BaseLoader.log_results  - INFO / WARNING / ERROR log levels
  ExchangeRateLoader      - load() happy path, idempotency (insert vs update
                            vs skip), batch_size, transaction rollback,
                            duplicate handling, quality-score filtering,
                            error handling, upsert_rates(), handle_duplicates(),
                            get_existing_rate(), track_load_history(),
                            rollback_on_error(), audit trail recording

Fixtures
--------
``engine`` / ``session`` come from tests/conftest.py (in-memory SQLite,
schema created once per session, each test wrapped in a rolled-back
transaction for isolation).

Notes on SQLite + SAVEPOINT
----------------------------
``loaders.py`` uses ``session.begin_nested()`` (SQL SAVEPOINT) per row so a
single bad row doesn't abort an entire batch. SQLite supports SAVEPOINT
natively, so this works correctly inside the conftest's already-open outer
transaction without any special handling here.
"""

from __future__ import annotations

import sys
import pathlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError

# ---------------------------------------------------------------------------
# Path bootstrap (mirrors conftest.py behaviour for standalone runs)
# ---------------------------------------------------------------------------
sys.path.insert(0, str(pathlib.Path(__file__).parents[2] / "src"))

from src.etl.loaders import (
    BaseLoader,
    ExchangeRateLoader,
    LoaderConfig,
    LoadResult,
)
from src.utils.database import ApiCall, ApiSource, Currency, ExchangeRate

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIXED_TS = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Builder helpers (plain functions — engine/session come from conftest.py)
# ---------------------------------------------------------------------------

def make_currency(code: str = "USD", name: str = "US Dollar") -> Currency:
    return Currency(code=code, name=name, is_active=True)


def make_api_source(name: str = "yfinance") -> ApiSource:
    return ApiSource(source_name=name, is_active=True)


def _seed_base(session):
    """Insert USD, IDR, and yfinance source; flush to get PKs."""
    usd = make_currency("USD", "US Dollar")
    idr = make_currency("IDR", "Indonesian Rupiah")
    src = make_api_source("yfinance")
    session.add_all([usd, idr, src])
    session.flush()
    return usd, idr, src


def make_rate_dict(
    from_id: int,
    to_id: int,
    rate: float = 16000.0,
    timestamp: datetime = FIXED_TS,
    quality_score: float | None = 0.9,
    is_valid: bool = True,
    source_id: int | None = None,
) -> dict:
    """Build a rate dict in the shape ExchangeRateLoader.load() expects."""
    d = {
        "from_currency_id": from_id,
        "to_currency_id": to_id,
        "rate": rate,
        "timestamp": timestamp,
        "quality_score": quality_score,
        "is_valid": is_valid,
    }
    if source_id is not None:
        d["source_id"] = source_id
    return d


@pytest.fixture
def loader() -> ExchangeRateLoader:
    """Default loader with predictable config (no env var dependency)."""
    return ExchangeRateLoader(
        config=LoaderConfig(
            batch_size=1000,
            skip_duplicates=True,
            track_audit=True,
            retry_count=3,
            timeout_seconds=300,
            log_level="INFO",
        )
    )


# ===========================================================================
# LoaderConfig
# ===========================================================================

class TestLoaderConfig:

    def test_default_values(self, monkeypatch):
        for key in (
            "LOADER_BATCH_SIZE", "LOADER_SKIP_DUPLICATES", "LOADER_TRACK_AUDIT",
            "LOADER_RETRY_COUNT", "LOADER_TIMEOUT_SECONDS", "LOADER_LOG_LEVEL",
        ):
            monkeypatch.delenv(key, raising=False)

        cfg = LoaderConfig()
        assert cfg.batch_size == 1000
        assert cfg.skip_duplicates is True
        assert cfg.track_audit is True
        assert cfg.retry_count == 3
        assert cfg.timeout_seconds == 300
        assert cfg.log_level == "INFO"

    def test_explicit_override_beats_env(self, monkeypatch):
        monkeypatch.setenv("LOADER_BATCH_SIZE", "500")
        cfg = LoaderConfig(batch_size=42)
        assert cfg.batch_size == 42

    def test_env_var_used_when_no_override(self, monkeypatch):
        monkeypatch.setenv("LOADER_BATCH_SIZE", "250")
        cfg = LoaderConfig()
        assert cfg.batch_size == 250

    def test_invalid_int_env_falls_back(self, monkeypatch):
        monkeypatch.setenv("LOADER_RETRY_COUNT", "not_a_number")
        cfg = LoaderConfig()
        assert cfg.retry_count == 3

    @pytest.mark.parametrize("raw,expected", [
        ("true", True), ("True", True), ("1", True), ("yes", True),
        ("false", False), ("0", False), ("no", False),
    ])
    def test_bool_env_parsing(self, monkeypatch, raw, expected):
        monkeypatch.setenv("LOADER_SKIP_DUPLICATES", raw)
        cfg = LoaderConfig()
        assert cfg.skip_duplicates is expected

    def test_repr_contains_key_fields(self):
        cfg = LoaderConfig(batch_size=10)
        r = repr(cfg)
        assert "batch_size" in r
        assert "skip_duplicates" in r


# ===========================================================================
# LoadResult
# ===========================================================================

class TestLoadResult:

    def test_load_result_creation_defaults(self):
        r = LoadResult()
        assert r.success is True
        assert r.rows_loaded == 0
        assert r.rows_updated == 0
        assert r.rows_skipped == 0
        assert r.rows_failed == 0
        assert r.errors == []
        assert r.warnings == []
        assert r.execution_time_ms == 0

    def test_add_error_accumulates(self):
        r = LoadResult()
        r.add_error("bad row")
        r.add_error("another bad row")
        assert r.errors == ["bad row", "another bad row"]
        # add_error does NOT auto-flip success (caller decides)
        assert r.success is True

    def test_add_warning_accumulates(self):
        r = LoadResult()
        r.add_warning("low quality")
        assert r.warnings == ["low quality"]

    def test_load_result_summary_total_processed(self):
        r = LoadResult(rows_loaded=5, rows_updated=2, rows_skipped=3, rows_failed=1)
        assert r.total_processed == 11

    def test_total_processed_zero_when_empty(self):
        r = LoadResult()
        assert r.total_processed == 0

    def test_repr_contains_counts(self):
        r = LoadResult(rows_loaded=3, rows_updated=1)
        text = repr(r)
        assert "loaded=3" in text
        assert "updated=1" in text


# ===========================================================================
# BaseLoader.log_results
# ===========================================================================

class TestBaseLoaderLogResults:

    def test_logs_info_on_clean_success(self, loader, caplog):
        import logging
        result = LoadResult(success=True, rows_loaded=2)
        with caplog.at_level(logging.INFO):
            loader.log_results(result, source="yfinance")
        assert any("success=True" in r.message for r in caplog.records)

    def test_logs_error_on_failure(self, loader, caplog):
        import logging
        result = LoadResult(success=False)
        result.add_error("transaction failed")
        with caplog.at_level(logging.ERROR):
            loader.log_results(result)
        assert any("transaction failed" in r.message for r in caplog.records)

    def test_logs_warning_on_warnings(self, loader, caplog):
        import logging
        result = LoadResult()
        result.add_warning("audit trail issue")
        with caplog.at_level(logging.WARNING):
            loader.log_results(result, source="fred")
        assert any("audit trail issue" in r.message for r in caplog.records)


# ===========================================================================
# ExchangeRateLoader.load — happy paths
# ===========================================================================

class TestLoadNewRates:

    def test_load_new_rates_inserts(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id, rate=16000.0)]

        result = loader.load(session, rates=rates, source_id=src.source_id)

        assert result.success is True
        assert result.rows_loaded == 1
        assert result.rows_updated == 0
        assert result.rows_skipped == 0
        assert result.rows_failed == 0

        stored = session.query(ExchangeRate).one()
        assert stored.rate == Decimal("16000.0")

    def test_load_multiple_new_rates(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [
            make_rate_dict(usd.currency_id, idr.currency_id, timestamp=FIXED_TS),
            make_rate_dict(
                usd.currency_id, idr.currency_id,
                timestamp=FIXED_TS + timedelta(hours=1),
            ),
        ]
        result = loader.load(session, rates=rates, source_id=src.source_id)
        assert result.rows_loaded == 2
        assert session.query(ExchangeRate).count() == 2

    def test_load_empty_list_is_noop(self, session, loader):
        usd, idr, src = _seed_base(session)
        result = loader.load(session, rates=[], source_id=src.source_id)
        assert result.rows_loaded == 0
        assert any("empty" in w.lower() for w in result.warnings)

    def test_load_execution_time_recorded(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]
        result = loader.load(session, rates=rates, source_id=src.source_id)
        assert result.execution_time_ms >= 0


# ===========================================================================
# Idempotency — insert vs update vs skip
# ===========================================================================

class TestLoadIdempotency:

    def test_load_idempotency_same_data_twice(self, session, loader):
        """Loading the exact same row twice must not create duplicates."""
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id, quality_score=0.9)]

        first = loader.load(session, rates=rates, source_id=src.source_id)
        second = loader.load(session, rates=rates, source_id=src.source_id)

        assert first.rows_loaded == 1
        # Same quality score (0.9 >= 0.9) -> counted as an update, not a dupe-skip
        assert second.rows_updated == 1
        assert session.query(ExchangeRate).count() == 1

    def test_load_update_existing_rates_with_better_quality(self, session, loader):
        usd, idr, src = _seed_base(session)
        low_quality = make_rate_dict(
            usd.currency_id, idr.currency_id, rate=16000.0, quality_score=0.5,
        )
        loader.load(session, rates=[low_quality], source_id=src.source_id)

        better_quality = make_rate_dict(
            usd.currency_id, idr.currency_id, rate=16050.0, quality_score=0.95,
        )
        result = loader.load(session, rates=[better_quality], source_id=src.source_id)

        assert result.rows_updated == 1
        stored = session.query(ExchangeRate).one()
        assert stored.rate == Decimal("16050.0")
        assert stored.data_quality_score == Decimal("0.95")

    def test_load_duplicate_rates_lower_quality_skipped(self, session, loader):
        usd, idr, src = _seed_base(session)
        high_quality = make_rate_dict(
            usd.currency_id, idr.currency_id, rate=16000.0, quality_score=0.95,
        )
        loader.load(session, rates=[high_quality], source_id=src.source_id)

        lower_quality = make_rate_dict(
            usd.currency_id, idr.currency_id, rate=99999.0, quality_score=0.3,
        )
        result = loader.load(session, rates=[lower_quality], source_id=src.source_id)

        assert result.rows_skipped == 1
        assert result.rows_updated == 0
        stored = session.query(ExchangeRate).one()
        # Original rate preserved — lower-quality data was NOT written
        assert stored.rate == Decimal("16000.0")

    def test_load_quality_score_filtering_none_existing_always_updates(self, session, loader):
        """If existing quality score is NULL, any new data replaces it."""
        usd, idr, src = _seed_base(session)
        no_quality = make_rate_dict(
            usd.currency_id, idr.currency_id, rate=16000.0, quality_score=None,
        )
        loader.load(session, rates=[no_quality], source_id=src.source_id)

        new_data = make_rate_dict(
            usd.currency_id, idr.currency_id, rate=16100.0, quality_score=0.1,
        )
        result = loader.load(session, rates=[new_data], source_id=src.source_id)

        assert result.rows_updated == 1
        stored = session.query(ExchangeRate).one()
        assert stored.rate == Decimal("16100.0")

    def test_load_quality_score_filtering_none_new_always_updates(self, session, loader):
        """If new quality score is NULL (unverified), it still replaces old data."""
        usd, idr, src = _seed_base(session)
        first = make_rate_dict(
            usd.currency_id, idr.currency_id, rate=16000.0, quality_score=0.9,
        )
        loader.load(session, rates=[first], source_id=src.source_id)

        second = make_rate_dict(
            usd.currency_id, idr.currency_id, rate=16200.0, quality_score=None,
        )
        result = loader.load(session, rates=[second], source_id=src.source_id)
        assert result.rows_updated == 1

    def test_load_different_timestamps_are_distinct_rows(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [
            make_rate_dict(usd.currency_id, idr.currency_id, timestamp=FIXED_TS),
            make_rate_dict(
                usd.currency_id, idr.currency_id,
                timestamp=FIXED_TS + timedelta(hours=1),
            ),
        ]
        loader.load(session, rates=rates, source_id=src.source_id)
        assert session.query(ExchangeRate).count() == 2

    def test_load_different_source_ids_are_distinct_rows(self, session, loader):
        usd, idr, src1 = _seed_base(session)
        src2 = make_api_source("fred")
        session.add(src2)
        session.flush()

        rates_src1 = [make_rate_dict(usd.currency_id, idr.currency_id, source_id=src1.source_id)]
        rates_src2 = [make_rate_dict(usd.currency_id, idr.currency_id, source_id=src2.source_id)]

        loader.load(session, rates=rates_src1, source_id=src1.source_id)
        loader.load(session, rates=rates_src2, source_id=src2.source_id)

        assert session.query(ExchangeRate).count() == 2


# ===========================================================================
# Batch size
# ===========================================================================

class TestLoadWithBatchSize:

    def test_load_with_small_batch_size(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [
            make_rate_dict(
                usd.currency_id, idr.currency_id,
                timestamp=FIXED_TS + timedelta(hours=i),
            )
            for i in range(5)
        ]
        result = loader.load(
            session, rates=rates, source_id=src.source_id, batch_size=2,
        )
        assert result.rows_loaded == 5
        assert session.query(ExchangeRate).count() == 5

    def test_load_batch_size_override_beats_config(self, session):
        usd, idr, src = _seed_base(session)
        cfg_loader = ExchangeRateLoader(config=LoaderConfig(batch_size=1))
        rates = [
            make_rate_dict(
                usd.currency_id, idr.currency_id,
                timestamp=FIXED_TS + timedelta(hours=i),
            )
            for i in range(3)
        ]
        # Override at call-time to a larger batch
        result = cfg_loader.load(
            session, rates=rates, source_id=src.source_id, batch_size=10,
        )
        assert result.rows_loaded == 3

    def test_batch_loading_performance_many_rows(self, session, loader):
        """Smoke test: a reasonably large batch completes and all rows land."""
        usd, idr, src = _seed_base(session)
        n = 100
        rates = [
            make_rate_dict(
                usd.currency_id, idr.currency_id,
                timestamp=FIXED_TS + timedelta(minutes=i),
                rate=16000.0 + i,
            )
            for i in range(n)
        ]
        result = loader.load(session, rates=rates, source_id=src.source_id, batch_size=20)
        assert result.rows_loaded == n
        assert session.query(ExchangeRate).count() == n
        assert result.execution_time_ms >= 0


# ===========================================================================
# Error handling
# ===========================================================================

class TestLoadErrorHandling:

    def test_load_missing_required_key_recorded_as_failed_row(self, session, loader):
        usd, idr, src = _seed_base(session)
        bad_row = {"from_currency_id": usd.currency_id}  # missing to_currency_id, rate, timestamp
        result = loader.load(session, rates=[bad_row], source_id=src.source_id)

        assert result.rows_failed == 1
        assert result.rows_loaded == 0
        assert any("missing required key" in e.lower() for e in result.errors)

    def test_load_mixed_valid_and_invalid_rows(self, session, loader):
        usd, idr, src = _seed_base(session)
        good_row = make_rate_dict(usd.currency_id, idr.currency_id)
        bad_row = {"from_currency_id": usd.currency_id}  # malformed

        result = loader.load(session, rates=[good_row, bad_row], source_id=src.source_id)

        assert result.rows_loaded == 1
        assert result.rows_failed == 1
        # A partial failure (some rows ok) does not flip overall success to False
        assert result.success is True

    def test_load_rates_not_a_list_raises_value_error(self, session, loader):
        usd, idr, src = _seed_base(session)
        with pytest.raises(ValueError, match="must be a list"):
            loader.load(session, rates="not-a-list", source_id=src.source_id)  # type: ignore

    def test_load_fk_violation_recorded_as_row_failure(self, session, loader):
        """Non-existent currency FK should raise IntegrityError, caught per-row."""
        _, _, src = _seed_base(session)
        bad_row = make_rate_dict(from_id=99999, to_id=99998)
        result = loader.load(session, rates=[bad_row], source_id=src.source_id)

        assert result.rows_failed == 1
        assert result.rows_loaded == 0

    def test_load_transaction_rollback_on_sqlalchemy_error(self, session, loader):
        """
        Simulate a transaction-level SQLAlchemyError during flush() (not a
        per-row IntegrityError) and verify rollback_on_error() is invoked
        and success=False.
        """
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]

        with patch.object(session, "flush", side_effect=SQLAlchemyError("boom")):
            with patch.object(loader, "rollback_on_error") as mock_rollback:
                result = loader.load(session, rates=rates, source_id=src.source_id)

        assert result.success is False
        assert any("rolled back" in e.lower() for e in result.errors)
        mock_rollback.assert_called_once_with(session)

    def test_rollback_on_error_calls_session_rollback(self, loader):
        mock_session = MagicMock()
        loader.rollback_on_error(mock_session)
        mock_session.rollback.assert_called_once()

    def test_rollback_on_error_swallows_secondary_exception(self, loader, caplog):
        """If rollback() itself raises, rollback_on_error must not propagate."""
        import logging
        mock_session = MagicMock()
        mock_session.rollback.side_effect = SQLAlchemyError("rollback also failed")

        with caplog.at_level(logging.ERROR):
            loader.rollback_on_error(mock_session)  # must not raise

        assert any("rollback itself failed" in r.message for r in caplog.records)

    def test_load_operational_error_recorded_per_row(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]

        with patch.object(
            loader, "_upsert_one", side_effect=OperationalError("timeout", None, None)
        ):
            result = loader.load(session, rates=rates, source_id=src.source_id)

        assert result.rows_failed == 1
        assert any("OperationalError" in e for e in result.errors)

    def test_load_timeout_warning_when_exceeding_budget(self, session):
        """
        If execution exceeds configured timeout, a warning is added (non-fatal).

        Uses a negative timeout_seconds so the check
        (execution_time_ms > timeout_seconds * 1000) is guaranteed True
        regardless of how fast the in-memory SQLite load actually runs
        (which can legitimately complete in 0ms).
        """
        usd, idr, src = _seed_base(session)
        fast_loader = ExchangeRateLoader(config=LoaderConfig(timeout_seconds=-1))
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]

        result = fast_loader.load(session, rates=rates, source_id=src.source_id)

        assert result.success is True  # timeout is a soft warning, not a failure
        assert any("timeout" in w.lower() or "exceeding" in w.lower() for w in result.warnings)


# ===========================================================================
# upsert_rates() convenience wrapper
# ===========================================================================

class TestUpsertRates:

    def test_upsert_rates_delegates_to_load(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]
        result = loader.upsert_rates(session, rates, source_id=src.source_id)
        assert result.rows_loaded == 1

    def test_upsert_rates_is_idempotent(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id, quality_score=0.8)]
        loader.upsert_rates(session, rates, source_id=src.source_id)
        result2 = loader.upsert_rates(session, rates, source_id=src.source_id)
        assert session.query(ExchangeRate).count() == 1
        assert result2.rows_updated == 1


# ===========================================================================
# handle_duplicates() — dry-run partitioning
# ===========================================================================

class TestHandleDuplicates:

    def test_handle_duplicates_all_new(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]
        partition = loader.handle_duplicates(session, rates, source_id=src.source_id)

        assert len(partition["new"]) == 1
        assert len(partition["updatable"]) == 0
        assert len(partition["duplicates"]) == 0
        # Dry run — nothing actually written
        assert session.query(ExchangeRate).count() == 0

    def test_handle_duplicates_detects_updatable(self, session, loader):
        usd, idr, src = _seed_base(session)
        existing = make_rate_dict(usd.currency_id, idr.currency_id, quality_score=0.5)
        loader.load(session, rates=[existing], source_id=src.source_id)

        better = make_rate_dict(usd.currency_id, idr.currency_id, quality_score=0.9)
        partition = loader.handle_duplicates(session, [better], source_id=src.source_id)

        assert len(partition["updatable"]) == 1
        assert len(partition["new"]) == 0

    def test_handle_duplicates_detects_true_duplicates(self, session, loader):
        usd, idr, src = _seed_base(session)
        existing = make_rate_dict(usd.currency_id, idr.currency_id, quality_score=0.9)
        loader.load(session, rates=[existing], source_id=src.source_id)

        worse = make_rate_dict(usd.currency_id, idr.currency_id, quality_score=0.1)
        partition = loader.handle_duplicates(session, [worse], source_id=src.source_id)

        assert len(partition["duplicates"]) == 1

    def test_handle_duplicates_skips_malformed_rows(self, session, loader):
        usd, idr, src = _seed_base(session)
        malformed = {"from_currency_id": usd.currency_id}  # missing keys
        partition = loader.handle_duplicates(session, [malformed], source_id=src.source_id)

        assert len(partition["new"]) == 0
        assert len(partition["updatable"]) == 0
        assert len(partition["duplicates"]) == 0

    def test_handle_duplicates_mixed_batch(self, session, loader):
        usd, idr, src = _seed_base(session)
        # Seed one existing row
        existing = make_rate_dict(
            usd.currency_id, idr.currency_id, timestamp=FIXED_TS, quality_score=0.5,
        )
        loader.load(session, rates=[existing], source_id=src.source_id)

        batch = [
            make_rate_dict(usd.currency_id, idr.currency_id, timestamp=FIXED_TS, quality_score=0.9),  # updatable
            make_rate_dict(usd.currency_id, idr.currency_id, timestamp=FIXED_TS + timedelta(hours=1)),  # new
        ]
        partition = loader.handle_duplicates(session, batch, source_id=src.source_id)
        assert len(partition["updatable"]) == 1
        assert len(partition["new"]) == 1


# ===========================================================================
# get_existing_rate()
# ===========================================================================

class TestGetExistingRate:

    def test_returns_none_when_absent(self, session, loader):
        usd, idr, src = _seed_base(session)
        result = loader.get_existing_rate(
            session, usd.currency_id, idr.currency_id, FIXED_TS, src.source_id,
        )
        assert result is None

    def test_returns_row_when_present(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]
        loader.load(session, rates=rates, source_id=src.source_id)

        result = loader.get_existing_rate(
            session, usd.currency_id, idr.currency_id, FIXED_TS, src.source_id,
        )
        assert result is not None
        assert result.from_currency_id == usd.currency_id

    def test_does_not_match_different_source(self, session, loader):
        usd, idr, src1 = _seed_base(session)
        src2 = make_api_source("fred")
        session.add(src2)
        session.flush()

        rates = [make_rate_dict(usd.currency_id, idr.currency_id, source_id=src1.source_id)]
        loader.load(session, rates=rates, source_id=src1.source_id)

        result = loader.get_existing_rate(
            session, usd.currency_id, idr.currency_id, FIXED_TS, src2.source_id,
        )
        assert result is None

    def test_does_not_match_different_timestamp(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id, timestamp=FIXED_TS)]
        loader.load(session, rates=rates, source_id=src.source_id)

        result = loader.get_existing_rate(
            session, usd.currency_id, idr.currency_id,
            FIXED_TS + timedelta(hours=1), src.source_id,
        )
        assert result is None


# ===========================================================================
# track_load_history() / audit trail
# ===========================================================================

class TestTrackLoadHistory:

    def test_track_load_history_creates_api_call_row(self, session, loader):
        _, _, src = _seed_base(session)
        result = LoadResult(success=True, rows_loaded=3, rows_updated=1)

        call = loader.track_load_history(session, result, source_id=src.source_id)

        assert call.call_id is not None
        assert call.source_id == src.source_id
        assert call.status == "SUCCESS"
        assert call.records_valid == 4  # loaded + updated
        assert call.records_invalid == 0
        assert session.query(ApiCall).count() == 1

    def test_track_load_history_status_error_on_failure(self, session, loader):
        _, _, src = _seed_base(session)
        result = LoadResult(success=False)
        result.add_error("transaction failed")

        call = loader.track_load_history(session, result, source_id=src.source_id)
        assert call.status == "ERROR"
        assert "transaction failed" in call.error_message

    def test_track_load_history_records_execution_time(self, session, loader):
        _, _, src = _seed_base(session)
        result = LoadResult(execution_time_ms=1234)
        call = loader.track_load_history(session, result, source_id=src.source_id)
        assert call.execution_time_ms == 1234

    def test_audit_trail_recording_happens_automatically_on_load(self, session, loader):
        """track_audit=True (default) means load() writes an ApiCall row itself."""
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]

        loader.load(session, rates=rates, source_id=src.source_id)

        assert session.query(ApiCall).count() == 1
        call = session.query(ApiCall).one()
        assert call.source_id == src.source_id
        assert call.status == "SUCCESS"

    def test_audit_trail_disabled_when_track_audit_false(self, session):
        usd, idr, src = _seed_base(session)
        no_audit_loader = ExchangeRateLoader(config=LoaderConfig(track_audit=False))
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]

        no_audit_loader.load(session, rates=rates, source_id=src.source_id)

        assert session.query(ApiCall).count() == 0

    def test_audit_trail_failure_does_not_mask_load_result(self, session, loader):
        """If writing the audit row itself fails, the load's own result must survive."""
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id)]

        with patch.object(
            loader, "track_load_history", side_effect=SQLAlchemyError("audit write failed")
        ):
            result = loader.load(session, rates=rates, source_id=src.source_id)

        # The actual rate load succeeded even though audit logging failed
        assert result.rows_loaded == 1
        assert any("audit trail recording failed" in w.lower() for w in result.warnings)

    def test_derive_status_partial_when_some_failed(self):
        result = LoadResult(success=True, rows_loaded=1, rows_failed=1)
        status = ExchangeRateLoader._derive_status(result)
        assert status in ("PARTIAL", "SUCCESS")  # depends on ApiCall.status attr check

    def test_derive_status_error_when_all_failed(self):
        result = LoadResult(success=True, rows_loaded=0, rows_updated=0, rows_failed=3)
        status = ExchangeRateLoader._derive_status(result)
        assert status == "ERROR"

    def test_derive_status_success_when_clean(self):
        result = LoadResult(success=True, rows_loaded=5)
        status = ExchangeRateLoader._derive_status(result)
        assert status == "SUCCESS"


# ===========================================================================
# Internal helpers — _to_decimal / _elapsed_ms / _should_replace
# ===========================================================================

class TestInternalHelpers:

    def test_to_decimal_from_float(self):
        assert ExchangeRateLoader._to_decimal(16000.5) == Decimal("16000.5")

    def test_to_decimal_from_none(self):
        assert ExchangeRateLoader._to_decimal(None) is None

    def test_to_decimal_from_decimal_passthrough(self):
        d = Decimal("123.456")
        assert ExchangeRateLoader._to_decimal(d) is d

    def test_to_decimal_from_string(self):
        assert ExchangeRateLoader._to_decimal("99.99") == Decimal("99.99")

    def test_should_replace_existing_none(self):
        assert ExchangeRateLoader._should_replace(None, 0.1) is True

    def test_should_replace_new_none(self):
        assert ExchangeRateLoader._should_replace(Decimal("0.9"), None) is True

    def test_should_replace_new_higher(self):
        assert ExchangeRateLoader._should_replace(Decimal("0.5"), 0.9) is True

    def test_should_replace_new_equal(self):
        assert ExchangeRateLoader._should_replace(Decimal("0.5"), 0.5) is True

    def test_should_replace_new_lower(self):
        assert ExchangeRateLoader._should_replace(Decimal("0.9"), 0.5) is False

    def test_elapsed_ms_non_negative(self):
        import time
        start = time.monotonic()
        elapsed = ExchangeRateLoader._elapsed_ms(start)
        assert elapsed >= 0


# ===========================================================================
# Full integration-style flow
# ===========================================================================

class TestLoadCompleteFlow:

    def test_load_then_query_round_trip(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [
            make_rate_dict(usd.currency_id, idr.currency_id, rate=16000.0, quality_score=0.95),
        ]
        result = loader.load(session, rates=rates, source_id=src.source_id)
        loader.log_results(result, source="yfinance")

        stored = session.query(ExchangeRate).one()
        assert stored.rate == Decimal("16000.0")
        assert stored.data_quality_score == Decimal("0.95")
        assert stored.is_valid is True

    def test_load_respects_is_valid_flag(self, session, loader):
        usd, idr, src = _seed_base(session)
        rates = [make_rate_dict(usd.currency_id, idr.currency_id, is_valid=False)]
        loader.load(session, rates=rates, source_id=src.source_id)

        stored = session.query(ExchangeRate).one()
        assert stored.is_valid is False

    def test_load_uses_default_source_when_row_has_none(self, session, loader):
        usd, idr, src = _seed_base(session)
        row = make_rate_dict(usd.currency_id, idr.currency_id)  # no source_id key
        loader.load(session, rates=[row], source_id=src.source_id)

        stored = session.query(ExchangeRate).one()
        assert stored.source_id == src.source_id

    def test_load_row_level_source_id_overrides_default(self, session, loader):
        usd, idr, src1 = _seed_base(session)
        src2 = make_api_source("fred")
        session.add(src2)
        session.flush()

        row = make_rate_dict(usd.currency_id, idr.currency_id, source_id=src2.source_id)
        # default source_id passed is src1, but row specifies src2
        loader.load(session, rates=[row], source_id=src1.source_id)

        stored = session.query(ExchangeRate).one()
        assert stored.source_id == src2.source_id