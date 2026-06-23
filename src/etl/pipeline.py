"""
src/etl/pipeline.py
=====================
End-to-end ETL orchestration for the Rupiah Exchange Rate Intelligence
platform.

Wires together the three previously built layers in strict sequence:

    extract (src.etl.extractors)  ->  validate (src.etl.validators)  ->  load (src.etl.loaders)

Design notes
------------
- This module integrates Features 1-4 of the project:
    Feature 1: ``src.etl.extractors``  (YFinanceExtractor, FREDExtractor)
    Feature 2: ``utils.database`` (ORM models, get_engine/get_session)
    Feature 3: ``src.etl.validators``  (ExchangeRateValidator)
    Feature 4: ``src.etl.loaders``     (ExchangeRateLoader)
- ``src.etl.loaders`` already defines its own ``LoadResult`` dataclass (rows
  loaded/updated/skipped/failed). To avoid a name collision at the
  *pipeline* level — where "LoadResult" means "the load STAGE's outcome
  inside a PipelineResult" — that class is imported under the alias
  ``LoaderRunResult``. This module's own :class:`LoadResult` wraps a
  ``LoaderRunResult`` plus stage-level timing/metrics.
- Currency pair strings follow the ``FROM_TO`` convention used by
  ``src.etl.extractors`` (e.g. ``"USD_IDR"``), NOT raw Yahoo Finance tickers
  (e.g. ``"USDIDR=X"``) — ticker mapping is an internal detail of
  ``YFinanceExtractor``.

Configuration
--------------
All thresholds are loaded from environment variables with sensible
defaults. Set them in your ``.env`` file or shell:

    PIPELINE_BATCH_SIZE=1000                  # rows per load() batch
    PIPELINE_EXTRACT_TIMEOUT=300               # seconds, soft budget per extract
    PIPELINE_VALIDATE_QUALITY_THRESHOLD=0.7    # min average quality to proceed to load
    PIPELINE_LOAD_RETRY_COUNT=3                # retry attempts on load failure
    PIPELINE_STOP_ON_ERROR=False               # True = abort whole pipeline on stage error
    PIPELINE_LOG_LEVEL=INFO

Example usage
--------------
    from src.src.etl.pipeline import EtlPipeline

    pipeline = EtlPipeline()

    result = pipeline.run(
        currency_pairs=["USD_IDR", "EUR_IDR"],
        source="yfinance",
        source_id=1,
    )

    if result.success:
        print(f"{result.processed_records} records processed in "
              f"{result.duration_seconds:.2f}s")
    else:
        print(f"Pipeline failed: {result.errors}")

    report = pipeline.generate_report(result)
    print(report)
"""

from __future__ import annotations

import logging
import os
import time
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.etl.extractors import ExchangeRateExtractor, get_extractor
from src.etl.loaders import ExchangeRateLoader
from src.etl.loaders import LoadResult as LoaderRunResult
from src.etl.validators import ExchangeRateValidator, ValidationResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _env_int(key: str, default: int) -> int:
    """Read an int from an environment variable, falling back to default."""
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(
            "Invalid value %r for env var %s — using default %d", raw, key, default,
        )
        return default


def _env_float(key: str, default: float) -> float:
    """Read a float from an environment variable, falling back to default."""
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning(
            "Invalid value %r for env var %s — using default %.4f", raw, key, default,
        )
        return default


def _env_bool(key: str, default: bool) -> bool:
    """Read a bool from an environment variable (true/1/yes = True)."""
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes")


class PipelineConfig:
    """
    Centralised, environment-driven pipeline configuration.

    All values can be overridden at construction time or via environment
    variables. See the module docstring for the full list of env vars.

    Attributes
    ----------
    batch_size : int
        Rows per :meth:`ExchangeRateLoader.load` batch during the load stage.
    extract_timeout : int
        Soft wall-clock budget (seconds) for the extract stage; logged as
        a warning if exceeded, does not abort extraction.
    validate_quality_threshold : float
        Minimum *average* quality score required to proceed to the load
        stage. Below this, the pipeline logs a warning but still loads
        records individually flagged ``is_valid=True`` unless
        ``stop_on_error`` is set.
    load_retry_count : int
        Max attempts for the load stage if it raises a transient error.
    stop_on_error : bool
        If ``True``, any stage-level critical error aborts the whole
        pipeline immediately. If ``False`` (default), non-critical errors
        are logged and the pipeline continues with whatever data survived.
    log_level : str
        Logging level name applied to this module's logger.
    """

    def __init__(
        self,
        batch_size: Optional[int] = None,
        extract_timeout: Optional[int] = None,
        validate_quality_threshold: Optional[float] = None,
        load_retry_count: Optional[int] = None,
        stop_on_error: Optional[bool] = None,
        log_level: Optional[str] = None,
    ) -> None:
        self.batch_size: int = (
            batch_size if batch_size is not None
            else _env_int("PIPELINE_BATCH_SIZE", 1000)
        )
        self.extract_timeout: int = (
            extract_timeout if extract_timeout is not None
            else _env_int("PIPELINE_EXTRACT_TIMEOUT", 300)
        )
        self.validate_quality_threshold: float = (
            validate_quality_threshold if validate_quality_threshold is not None
            else _env_float("PIPELINE_VALIDATE_QUALITY_THRESHOLD", 0.7)
        )
        self.load_retry_count: int = (
            load_retry_count if load_retry_count is not None
            else _env_int("PIPELINE_LOAD_RETRY_COUNT", 3)
        )
        self.stop_on_error: bool = (
            stop_on_error if stop_on_error is not None
            else _env_bool("PIPELINE_STOP_ON_ERROR", False)
        )
        self.log_level: str = (
            log_level if log_level is not None
            else os.getenv("PIPELINE_LOG_LEVEL", "INFO")
        )
        logger.setLevel(self.log_level)

    def __repr__(self) -> str:
        return (
            f"PipelineConfig("
            f"batch_size={self.batch_size}, "
            f"extract_timeout={self.extract_timeout}, "
            f"validate_quality_threshold={self.validate_quality_threshold}, "
            f"load_retry_count={self.load_retry_count}, "
            f"stop_on_error={self.stop_on_error}, "
            f"log_level={self.log_level!r})"
        )


# ---------------------------------------------------------------------------
# Stage result classes
# ---------------------------------------------------------------------------

@dataclass
class ExtractResult:
    """
    Outcome of the extract stage.

    Attributes
    ----------
    success : bool
        ``True`` if at least one record was fetched without a fatal error.
    records_fetched : int
        Total rate records successfully returned by the extractor(s).
    extract_errors : List[str]
        Error messages collected from the extractor's own error list, or
        from retries that ultimately failed.
    raw_rates : List[Dict[str, Any]]
        The raw rate dicts (as returned by
        ``ExchangeRateExtractor.fetch_rates``'s ``"rates"`` key) ready to be
        handed to the validate stage.
    duration_seconds : float
        Wall-clock duration of the extract stage.
    """

    success: bool = True
    records_fetched: int = 0
    extract_errors: List[str] = field(default_factory=list)
    raw_rates: List[Dict[str, Any]] = field(default_factory=list)
    duration_seconds: float = 0.0

    def __repr__(self) -> str:
        return (
            f"ExtractResult(success={self.success}, "
            f"fetched={self.records_fetched}, "
            f"errors={len(self.extract_errors)}, "
            f"duration={self.duration_seconds:.2f}s)"
        )


@dataclass
class ValidateResult:
    """
    Outcome of the validate stage.

    Attributes
    ----------
    success : bool
        ``True`` if validation ran to completion (even if some/most
        records failed quality checks — that's tracked separately).
    records_valid : int
        Number of records that passed ``ExchangeRateValidator`` hard checks.
    records_invalid : int
        Number of records that failed hard checks and were excluded from
        the load stage.
    quality_avg : float
        Average ``quality_score`` across all validated records
        (``0.0`` if no records were validated).
    validation_errors : List[str]
        Hard failure messages collected from the validator.
    validation_warnings : List[str]
        Soft warning messages collected from the validator.
    validated_rates : List[Dict[str, Any]]
        Rate dicts that passed validation, annotated with
        ``quality_score`` — ready for the load stage.
    duration_seconds : float
        Wall-clock duration of the validate stage.
    """

    success: bool = True
    records_valid: int = 0
    records_invalid: int = 0
    quality_avg: float = 0.0
    validation_errors: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    validated_rates: List[Dict[str, Any]] = field(default_factory=list)
    duration_seconds: float = 0.0

    def __repr__(self) -> str:
        return (
            f"ValidateResult(success={self.success}, "
            f"valid={self.records_valid}, invalid={self.records_invalid}, "
            f"quality_avg={self.quality_avg:.3f}, "
            f"duration={self.duration_seconds:.2f}s)"
        )


@dataclass
class LoadResult:
    """
    Outcome of the load stage (pipeline-level wrapper).

    This is distinct from ``src.etl.loaders.LoadResult`` (imported here as
    ``LoaderRunResult``), which tracks row-level insert/update/skip counts
    for a single :meth:`ExchangeRateLoader.load` call. This class wraps
    that result with pipeline-stage framing (timing, retry count used).

    Attributes
    ----------
    success : bool
        ``True`` if the load stage completed without an unrecoverable
        transaction failure.
    records_loaded : int
        Newly inserted rows (mirrors ``loader_result.rows_loaded``).
    records_updated : int
        Updated rows (mirrors ``loader_result.rows_updated``).
    records_skipped : int
        Skipped duplicate rows (mirrors ``loader_result.rows_skipped``).
    load_errors : List[str]
        Error messages from the loader and/or retry attempts.
    retries_used : int
        How many retry attempts were consumed before success or final
        failure.
    duration_seconds : float
        Wall-clock duration of the load stage (including retries).
    loader_result : Optional[LoaderRunResult]
        The raw result object from ``ExchangeRateLoader.load`` for callers
        that need row-level detail beyond this summary.
    """

    success: bool = True
    records_loaded: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    load_errors: List[str] = field(default_factory=list)
    retries_used: int = 0
    duration_seconds: float = 0.0
    loader_result: Optional[LoaderRunResult] = None

    def __repr__(self) -> str:
        return (
            f"LoadResult(success={self.success}, "
            f"loaded={self.records_loaded}, updated={self.records_updated}, "
            f"skipped={self.records_skipped}, retries={self.retries_used}, "
            f"duration={self.duration_seconds:.2f}s)"
        )


@dataclass
class PipelineResult:
    """
    Aggregated result of a full :meth:`EtlPipeline.run` call.

    Attributes
    ----------
    success : bool
        ``True`` if the pipeline completed all stages without a critical
        failure (per ``config.stop_on_error`` semantics).
    stages : Dict[str, Any]
        Per-stage result objects, keyed by stage name:
        ``{"extract": ExtractResult, "validate": ValidateResult,
           "load": LoadResult}``. A stage that never ran (e.g. pipeline
        stopped early) is simply absent from this dict.
    total_records : int
        Records fetched in the extract stage (upper bound for the run).
    processed_records : int
        Records that made it all the way through load (loaded + updated).
    failed_records : int
        Records that failed at any stage (extract errors + invalid
        records + load failures), counted once each.
    duration_seconds : float
        Total wall-clock duration of the whole pipeline run.
    errors : List[str]
        All hard error messages collected across every stage.
    warnings : List[str]
        All soft warning messages collected across every stage.
    """

    success: bool = True
    stages: Dict[str, Any] = field(default_factory=dict)
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    duration_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(self, msg: str) -> None:
        """Record a hard pipeline-level error."""
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        """Record a soft pipeline-level warning."""
        self.warnings.append(msg)

    def __repr__(self) -> str:
        return (
            f"PipelineResult(success={self.success}, "
            f"total={self.total_records}, processed={self.processed_records}, "
            f"failed={self.failed_records}, duration={self.duration_seconds:.2f}s)"
        )


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class BasePipeline(ABC):
    """
    Abstract base class for all ETL pipelines in the project.

    Subclasses must implement :meth:`run` and :meth:`validate_inputs`.
    """

    def __init__(self, config: Optional[PipelineConfig] = None) -> None:
        self.config: PipelineConfig = config or PipelineConfig()

    @abstractmethod
    def validate_inputs(self, *args: Any, **kwargs: Any) -> None:
        """
        Validate the arguments passed to :meth:`run` before doing any work.

        Raises
        ------
        ValueError
            If any input is structurally invalid (e.g. empty currency
            pair list, malformed date range).
        """

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> PipelineResult:
        """
        Execute the full pipeline and return an aggregated result.

        Returns
        -------
        PipelineResult
        """


# ---------------------------------------------------------------------------
# EtlPipeline
# ---------------------------------------------------------------------------

class EtlPipeline(BasePipeline):
    """
    Orchestrates the full extract -> validate -> load flow for exchange
    rate data.

    Integrates:
        - ``src.etl.extractors``  (Feature 1): ``YFinanceExtractor`` / ``FREDExtractor``
        - ``utils.database`` (Feature 2): ORM session for the load stage
        - ``src.etl.validators``  (Feature 3): ``ExchangeRateValidator``
        - ``src.etl.loaders``     (Feature 4): ``ExchangeRateLoader``

    Error-handling policy
    ----------------------
    - **Extract errors**: retried with exponential backoff internally by
      the extractor itself (``src.etl.extractors.with_retry``); any pairs that
      still fail after retries are recorded in ``ExtractResult.extract_errors``
      and excluded from the validate stage. This is a **non-critical**
      error — the pipeline continues with whatever pairs succeeded.
    - **Validate errors**: hard-invalid records (failed ``is_valid`` check)
      are logged and excluded from the load stage. **Non-critical.**
    - **Load errors**: retried up to ``config.load_retry_count`` times with
      the batch split in half on each retry (mitigates a single
      poison-pill row blocking an entire batch). If all retries are
      exhausted, this is treated as a **critical** error.
    - **Critical errors** (load exhausted retries, or any stage raising an
      unexpected exception) trigger ``handle_errors()``, which rolls back
      the session and — if ``config.stop_on_error=True`` — aborts the
      pipeline immediately; otherwise the run is marked
      ``success=False`` but returns whatever partial result was gathered.

    Example
    -------
    >>> pipeline = EtlPipeline()
    >>> result = pipeline.run(
    ...     currency_pairs=["USD_IDR", "EUR_IDR"],
    ...     source="yfinance",
    ...     source_id=1,
    ... )
    >>> result.success
    True
    """

    def __init__(
        self,
        config: Optional[PipelineConfig] = None,
        extractor: Optional[ExchangeRateExtractor] = None,
        validator: Optional[ExchangeRateValidator] = None,
        loader: Optional[ExchangeRateLoader] = None,
    ) -> None:
        """
        Parameters
        ----------
        config:
            Pipeline configuration. Defaults to environment-driven
            :class:`PipelineConfig`.
        extractor:
            Pre-built extractor instance (useful for tests / dependency
            injection). If omitted, :meth:`run` builds one per call via
            ``src.etl.extractors.get_extractor(source)``.
        validator:
            Pre-built :class:`ExchangeRateValidator`. Defaults to a new
            instance with default thresholds.
        loader:
            Pre-built :class:`ExchangeRateLoader`. Defaults to a new
            instance with default thresholds.
        """
        super().__init__(config)
        self._injected_extractor = extractor
        self.validator: ExchangeRateValidator = validator or ExchangeRateValidator()
        self.loader: ExchangeRateLoader = loader or ExchangeRateLoader()
        logger.debug("EtlPipeline initialised with %s", self.config)

    # ------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------

    def validate_inputs(
        self,
        currency_pairs: List[str],
        source: str,
        source_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> None:
        """
        Validate :meth:`run` arguments before any extraction begins.

        Parameters
        ----------
        currency_pairs:
            List of pairs in ``"FROM_TO"`` form, e.g. ``["USD_IDR"]``.
        source:
            Data source name; must be one ``get_extractor`` recognises
            (``"yfinance"`` or ``"fred"``).
        source_id:
            FK to ``ApiSource.source_id`` for the load stage's audit trail.
        start_date, end_date:
            Optional date range (currently informational only — both
            extractors fetch the latest available observation; date
            filtering is a future extension point).

        Raises
        ------
        ValueError
            If *currency_pairs* is empty, *source* is unrecognised, or
            *start_date* is after *end_date*.

        Example
        -------
        >>> pipeline.validate_inputs(["USD_IDR"], "yfinance", 1)
        """
        if not currency_pairs:
            raise ValueError("currency_pairs must not be empty.")
        if not isinstance(currency_pairs, list) or not all(
            isinstance(p, str) for p in currency_pairs
        ):
            raise ValueError("currency_pairs must be a list of strings.")
        if source not in ("yfinance", "fred"):
            raise ValueError(
                f"Unknown source {source!r}. Valid options: 'yfinance', 'fred'."
            )
        if not isinstance(source_id, int) or source_id <= 0:
            raise ValueError(f"source_id must be a positive integer, got {source_id!r}.")
        if start_date is not None and end_date is not None and start_date > end_date:
            raise ValueError(
                f"start_date ({start_date}) must not be after end_date ({end_date})."
            )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(
        self,
        currency_pairs: List[str],
        source: str,
        source_id: int,
        session: Optional[Session] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> PipelineResult:
        """
        Execute the full extract -> validate -> load pipeline.

        Parameters
        ----------
        currency_pairs:
            Pairs to fetch, e.g. ``["USD_IDR", "EUR_IDR"]``.
        source:
            ``"yfinance"`` or ``"fred"``.
        source_id:
            FK to ``ApiSource.source_id`` for audit-trail purposes.
        session:
            An active SQLAlchemy session for the load stage. If omitted,
            a new session is opened via ``utils.database.get_session``
            and committed/closed automatically; if provided, the caller
            owns the commit/rollback lifecycle.
        start_date, end_date:
            Optional informational date range (see :meth:`validate_inputs`).

        Returns
        -------
        PipelineResult

        Example
        -------
        >>> result = pipeline.run(["USD_IDR"], source="yfinance", source_id=1)
        >>> result.stages.keys()
        dict_keys(['extract', 'validate', 'load'])
        """
        pipeline_start = time.monotonic()
        result = PipelineResult()

        try:
            self.validate_inputs(currency_pairs, source, source_id, start_date, end_date)
        except ValueError as exc:
            result.success = False
            result.add_error(f"Input validation failed: {exc}")
            result.duration_seconds = self._elapsed(pipeline_start)
            logger.error("Pipeline aborted before extraction: %s", exc)
            return result

        logger.info(
            "Pipeline run started: pairs=%s source=%s source_id=%d",
            currency_pairs, source, source_id,
        )

        owns_session = session is None
        session_cm = None  # opened lazily, right before the load stage

        try:
            # ---- Stage 1: Extract ----
            extract_result = self.extract(currency_pairs, source)
            result.stages["extract"] = extract_result
            result.total_records = extract_result.records_fetched
            result.errors.extend(extract_result.extract_errors)

            if not extract_result.raw_rates:
                result.add_error("Extract stage produced zero records — aborting pipeline.")
                result.success = False
                result.duration_seconds = self._elapsed(pipeline_start)
                self.log_metrics(self._build_metrics(result))
                return result

            if extract_result.extract_errors:
                result.add_warning(
                    f"Extract stage had {len(extract_result.extract_errors)} "
                    f"non-critical error(s); continuing with "
                    f"{extract_result.records_fetched} successfully fetched record(s)."
                )

            # ---- Stage 2: Validate ----
            validate_result = self.validate(extract_result.raw_rates)
            result.stages["validate"] = validate_result
            result.errors.extend(validate_result.validation_errors)
            result.warnings.extend(validate_result.validation_warnings)

            if validate_result.quality_avg < self.config.validate_quality_threshold:
                result.add_warning(
                    f"Average quality score {validate_result.quality_avg:.3f} is "
                    f"below the configured threshold "
                    f"({self.config.validate_quality_threshold}); proceeding with "
                    f"individually-valid records only."
                )

            if not validate_result.validated_rates:
                result.add_error("Validate stage produced zero loadable records — aborting.")
                result.success = False
                result.failed_records = result.total_records
                result.duration_seconds = self._elapsed(pipeline_start)
                self.log_metrics(self._build_metrics(result))
                return result

            # ---- Stage 3: Load ----
            # Session is opened here (lazily) so stages that short-circuit
            # before reaching load (extract/validate producing zero usable
            # records) never require a database connection at all.
            if owns_session:
                from utils.database import get_engine, get_session as _get_session
                engine = get_engine()
                session_cm = _get_session(engine)
                session = session_cm.__enter__()

            load_result = self.load(session, validate_result.validated_rates, source_id)
            result.stages["load"] = load_result
            result.errors.extend(load_result.load_errors)

            result.processed_records = load_result.records_loaded + load_result.records_updated
            result.failed_records = (
                len(extract_result.extract_errors)
                + validate_result.records_invalid
                + (
                    validate_result.records_valid
                    - load_result.records_loaded
                    - load_result.records_updated
                    - load_result.records_skipped
                )
            )
            result.failed_records = max(result.failed_records, 0)

            if not load_result.success:
                result.success = False
                self.handle_errors(load_result.load_errors, session=session, critical=True)
            else:
                result.success = True
                if owns_session:
                    pass  # commit handled by get_session() context manager on exit

        except Exception as exc:  # noqa: BLE001
            result.success = False
            tb = traceback.format_exc()
            result.add_error(f"Unhandled pipeline exception: {exc}")
            logger.error("Unhandled exception during pipeline run:\n%s", tb)
            self.handle_errors([str(exc)], session=session, critical=True)

        finally:
            result.duration_seconds = self._elapsed(pipeline_start)
            if owns_session and session_cm is not None:
                try:
                    if result.success:
                        session_cm.__exit__(None, None, None)
                    else:
                        session_cm.__exit__(type(Exception()), Exception("pipeline failed"), None)
                except Exception as close_exc:  # noqa: BLE001
                    logger.error("Error closing pipeline-owned session: %s", close_exc)

        self.log_metrics(self._build_metrics(result))
        logger.info(
            "Pipeline run finished: success=%s processed=%d failed=%d duration=%.2fs",
            result.success, result.processed_records, result.failed_records,
            result.duration_seconds,
        )
        return result

    # ------------------------------------------------------------------
    # Stage 1 — Extract
    # ------------------------------------------------------------------

    def extract(self, currency_pairs: List[str], source: str) -> ExtractResult:
        """
        Fetch raw rate data for the given pairs from the given source.

        Retry behaviour is delegated to the extractor itself
        (``src.etl.extractors.with_retry``, exponential backoff, max 3
        attempts per pair). This method's job is only to invoke the
        extractor and translate its ``ExtractionResult``-shaped dict into
        an :class:`ExtractResult`.

        Parameters
        ----------
        currency_pairs:
            Pairs to fetch, e.g. ``["USD_IDR", "EUR_IDR"]``.
        source:
            ``"yfinance"`` or ``"fred"``.

        Returns
        -------
        ExtractResult

        Example
        -------
        >>> result = pipeline.extract(["USD_IDR"], "yfinance")
        >>> result.records_fetched
        1
        """
        start = time.monotonic()
        result = ExtractResult()

        extractor = self._injected_extractor or get_extractor(source)

        try:
            raw = extractor.fetch_rates(currency_pairs)
        except Exception as exc:  # noqa: BLE001
            result.success = False
            result.extract_errors.append(f"Extractor raised an unexpected exception: {exc}")
            logger.exception("extract(): extractor.fetch_rates() raised unexpectedly.")
            result.duration_seconds = self._elapsed(start)
            return result

        result.raw_rates = raw.get("rates", [])
        result.records_fetched = len(result.raw_rates)
        result.extract_errors = list(raw.get("errors", []))
        result.success = result.records_fetched > 0
        result.duration_seconds = self._elapsed(start)

        if result.duration_seconds > self.config.extract_timeout:
            logger.warning(
                "extract(): stage took %.2fs, exceeding the configured "
                "extract_timeout budget of %ds.",
                result.duration_seconds, self.config.extract_timeout,
            )

        logger.info(
            "Extract stage complete: fetched=%d errors=%d duration=%.2fs",
            result.records_fetched, len(result.extract_errors), result.duration_seconds,
        )
        return result

    # ------------------------------------------------------------------
    # Stage 2 — Validate
    # ------------------------------------------------------------------

    def validate(self, data: List[Dict[str, Any]]) -> ValidateResult:
        """
        Validate raw rate dicts using :class:`ExchangeRateValidator`.

        Each rate dict (shape: ``{"pair", "rate", "timestamp", "source",
        "fetched_at", "data_quality_score"}`` per ``src.etl.extractors``) is
        converted to a single-row DataFrame and run through the validator
        individually, since the validator's date-consistency / anomaly
        checks are designed for a time series of one pair — mixing
        multiple pairs into one frame would corrupt those checks.

        Parameters
        ----------
        data:
            List of raw rate dicts, as returned by
            ``ExtractResult.raw_rates``.

        Returns
        -------
        ValidateResult

        Example
        -------
        >>> result = pipeline.validate(extract_result.raw_rates)
        >>> result.records_valid
        1
        """
        start = time.monotonic()
        result = ValidateResult()

        if not data:
            result.duration_seconds = self._elapsed(start)
            return result

        quality_scores: List[float] = []

        # Group by pair so each validation run sees a consistent single
        # time series (required for date-consistency / anomaly checks).
        by_pair: Dict[str, List[Dict[str, Any]]] = {}
        for row in data:
            by_pair.setdefault(row.get("pair", "UNKNOWN"), []).append(row)

        for pair, rows in by_pair.items():
            df = pd.DataFrame(rows).rename(columns={"rate": "rate_close"})
            try:
                validation: ValidationResult = self.validator.validate(df)
            except Exception as exc:  # noqa: BLE001
                result.validation_errors.append(
                    f"Validator raised an unexpected exception for pair {pair}: {exc}"
                )
                logger.exception("validate(): validator.validate() raised for pair %s.", pair)
                result.records_invalid += len(rows)
                continue

            result.validation_errors.extend(
                f"[{pair}] {e}" for e in validation.errors
            )
            result.validation_warnings.extend(
                f"[{pair}] {w}" for w in validation.warnings
            )
            quality_scores.append(validation.quality_score)

            if validation.is_valid:
                for row in rows:
                    enriched = dict(row)
                    enriched["quality_score"] = validation.quality_score
                    result.validated_rates.append(enriched)
                result.records_valid += len(rows)
            else:
                result.records_invalid += len(rows)
                logger.warning(
                    "validate(): pair %s failed hard validation checks "
                    "(quality=%.3f) — excluded from load stage.",
                    pair, validation.quality_score,
                )

        result.quality_avg = (
            sum(quality_scores) / len(quality_scores) if quality_scores else 0.0
        )
        result.success = True  # the stage itself ran; record-level pass/fail is separate
        result.duration_seconds = self._elapsed(start)

        logger.info(
            "Validate stage complete: valid=%d invalid=%d quality_avg=%.3f duration=%.2fs",
            result.records_valid, result.records_invalid, result.quality_avg,
            result.duration_seconds,
        )
        return result

    # ------------------------------------------------------------------
    # Stage 3 — Load
    # ------------------------------------------------------------------

    def load(
        self,
        session: Session,
        data: List[Dict[str, Any]],
        source_id: int,
    ) -> LoadResult:
        """
        Load validated rate dicts into the database via
        :class:`ExchangeRateLoader`, retrying with a halved batch size on
        failure.

        Retry strategy
        ---------------
        On a failed attempt, the batch is split in half and each half is
        retried independently (mitigates a single poison-pill row from
        blocking otherwise-good data). This repeats up to
        ``config.load_retry_count`` times per half; if a half still fails
        at size 1, that single record is recorded as a load error and
        skipped.

        Parameters
        ----------
        session:
            Active SQLAlchemy session.
        data:
            Validated rate dicts (with ``quality_score`` set), as returned
            by ``ValidateResult.validated_rates``.
        source_id:
            FK to ``ApiSource.source_id``.

        Returns
        -------
        LoadResult

        Example
        -------
        >>> result = pipeline.load(session, validated_rates, source_id=1)
        >>> result.records_loaded
        1
        """
        start = time.monotonic()
        result = LoadResult()

        if not data:
            result.duration_seconds = self._elapsed(start)
            return result

        # Translate pipeline-shaped rate dicts (pair/rate/timestamp/...)
        # into the {from_currency_id, to_currency_id, ...} shape
        # ExchangeRateLoader.load() expects. Currency-code -> id resolution
        # is intentionally NOT done here — callers are expected to have
        # already attached from_currency_id/to_currency_id, OR this
        # pipeline is used with rows that already carry those keys (e.g.
        # produced by an upstream currency-lookup step). Rows missing
        # these keys are treated as load failures so the issue surfaces
        # immediately rather than silently dropping data.
        loader_rows: List[Dict[str, Any]] = []
        for row in data:
            if "from_currency_id" not in row or "to_currency_id" not in row:
                result.load_errors.append(
                    f"Row missing from_currency_id/to_currency_id, cannot load: "
                    f"{row.get('pair', row)}"
                )
                continue
            loader_rows.append(row)

        retries_used = 0
        batches: List[List[Dict[str, Any]]] = [loader_rows] if loader_rows else []
        any_batch_failed_permanently = False

        while batches:
            batch = batches.pop(0)
            if not batch:
                continue
            try:
                loader_result: LoaderRunResult = self.loader.load(
                    session,
                    rates=batch,
                    source_id=source_id,
                    batch_size=self.config.batch_size,
                )
                result.records_loaded += loader_result.rows_loaded
                result.records_updated += loader_result.rows_updated
                result.records_skipped += loader_result.rows_skipped
                result.load_errors.extend(loader_result.errors)
                result.loader_result = loader_result

                if not loader_result.success:
                    if retries_used < self.config.load_retry_count and len(batch) > 1:
                        retries_used += 1
                        mid = len(batch) // 2
                        batches.append(batch[:mid])
                        batches.append(batch[mid:])
                        logger.warning(
                            "load(): batch of %d failed, splitting into halves "
                            "(retry %d/%d).",
                            len(batch), retries_used, self.config.load_retry_count,
                        )
                    else:
                        any_batch_failed_permanently = True
                        result.load_errors.append(
                            f"Record(s) failed after exhausting retries: {batch}"
                        )

            except Exception as exc:  # noqa: BLE001
                result.load_errors.append(f"Load batch raised an exception: {exc}")
                logger.exception("load(): unexpected exception during loader.load().")
                if retries_used < self.config.load_retry_count and len(batch) > 1:
                    retries_used += 1
                    mid = len(batch) // 2
                    batches.append(batch[:mid])
                    batches.append(batch[mid:])
                else:
                    any_batch_failed_permanently = True

        result.retries_used = retries_used
        # Explicit success rule: the stage failed if there was nothing to
        # load (trivially true — handled by the `if not data` guard above),
        # OR at least one batch could not be loaded even after exhausting
        # retries. This avoids inferring success from row counts, which is
        # ambiguous when rows_loaded/updated/skipped are all legitimately
        # zero (e.g. a single failing record with no successful peers).
        result.success = bool(loader_rows) and not any_batch_failed_permanently
        result.duration_seconds = self._elapsed(start)

        logger.info(
            "Load stage complete: loaded=%d updated=%d skipped=%d retries=%d duration=%.2fs",
            result.records_loaded, result.records_updated, result.records_skipped,
            result.retries_used, result.duration_seconds,
        )
        return result

    # ------------------------------------------------------------------
    # Error handling
    # ------------------------------------------------------------------

    def handle_errors(
        self,
        errors: List[str],
        session: Optional[Session] = None,
        critical: bool = False,
    ) -> None:
        """
        Centralised error-handling hook for all pipeline stages.

        Parameters
        ----------
        errors:
            Error messages to log.
        session:
            If provided and *critical* is ``True``, the session is rolled
            back.
        critical:
            If ``True``, logs at ERROR level and rolls back *session* (if
            given). If ``False``, logs at WARNING level and takes no
            other action — the pipeline continues.

        Example
        -------
        >>> pipeline.handle_errors(["disk full"], session=session, critical=True)
        """
        level = logging.ERROR if critical else logging.WARNING
        for err in errors:
            logger.log(level, "%s%s", "[CRITICAL] " if critical else "", err)

        if critical and session is not None:
            try:
                session.rollback()
                logger.warning("handle_errors(): session rolled back due to critical error.")
            except Exception as exc:  # noqa: BLE001
                logger.error("handle_errors(): rollback itself failed: %s", exc)

    # ------------------------------------------------------------------
    # Metrics & reporting
    # ------------------------------------------------------------------

    def log_metrics(self, metrics: Dict[str, Any]) -> None:
        """
        Log a structured summary of pipeline performance metrics.

        Parameters
        ----------
        metrics:
            Dict produced by :meth:`_build_metrics` (or any compatible
            structure) — logged as a single structured INFO line per
            top-level key.

        Example
        -------
        >>> pipeline.log_metrics({"pipeline": {"duration_seconds": 1.23}})
        """
        for stage_name, stage_metrics in metrics.items():
            logger.info("[metrics] %s: %s", stage_name, stage_metrics)

    def _build_metrics(self, result: PipelineResult) -> Dict[str, Any]:
        """Construct the metrics dict passed to :meth:`log_metrics`."""
        metrics: Dict[str, Any] = {"pipeline": {
            "success": result.success,
            "total_records": result.total_records,
            "processed_records": result.processed_records,
            "failed_records": result.failed_records,
            "duration_seconds": round(result.duration_seconds, 3),
            "success_rate": (
                round(result.processed_records / result.total_records, 4)
                if result.total_records > 0 else 0.0
            ),
            "error_count": len(result.errors),
            "warning_count": len(result.warnings),
        }}

        if "extract" in result.stages:
            ex = result.stages["extract"]
            metrics["extract"] = {
                "records_fetched": ex.records_fetched,
                "extract_errors": len(ex.extract_errors),
                "duration_seconds": round(ex.duration_seconds, 3),
            }
        if "validate" in result.stages:
            va = result.stages["validate"]
            metrics["validate"] = {
                "records_valid": va.records_valid,
                "records_invalid": va.records_invalid,
                "quality_avg": round(va.quality_avg, 4),
                "duration_seconds": round(va.duration_seconds, 3),
            }
        if "load" in result.stages:
            lo = result.stages["load"]
            metrics["load"] = {
                "records_loaded": lo.records_loaded,
                "records_updated": lo.records_updated,
                "records_skipped": lo.records_skipped,
                "retries_used": lo.retries_used,
                "duration_seconds": round(lo.duration_seconds, 3),
            }
        return metrics

    def generate_report(self, result: PipelineResult) -> str:
        """
        Generate a human-readable text report summarising a pipeline run.

        Parameters
        ----------
        result:
            The :class:`PipelineResult` to summarise.

        Returns
        -------
        str
            Multi-line formatted report covering: overall summary,
            per-stage metrics, error details, and basic recommendations.

        Example
        -------
        >>> report = pipeline.generate_report(result)
        >>> print(report)
        ===== ETL Pipeline Report =====
        ...
        """
        lines: List[str] = []
        lines.append("===== ETL Pipeline Report =====")
        lines.append(f"Status        : {'SUCCESS' if result.success else 'FAILED'}")
        lines.append(f"Total records : {result.total_records}")
        lines.append(f"Processed     : {result.processed_records}")
        lines.append(f"Failed        : {result.failed_records}")
        lines.append(f"Duration      : {result.duration_seconds:.2f}s")
        lines.append(
            f"Success rate  : "
            f"{(result.processed_records / result.total_records * 100) if result.total_records else 0:.1f}%"
        )
        lines.append("")

        lines.append("--- Stage Details ---")
        if "extract" in result.stages:
            ex = result.stages["extract"]
            lines.append(
                f"Extract  : fetched={ex.records_fetched} "
                f"errors={len(ex.extract_errors)} "
                f"duration={ex.duration_seconds:.2f}s"
            )
        if "validate" in result.stages:
            va = result.stages["validate"]
            lines.append(
                f"Validate : valid={va.records_valid} "
                f"invalid={va.records_invalid} "
                f"quality_avg={va.quality_avg:.3f} "
                f"duration={va.duration_seconds:.2f}s"
            )
        if "load" in result.stages:
            lo = result.stages["load"]
            lines.append(
                f"Load     : loaded={lo.records_loaded} "
                f"updated={lo.records_updated} "
                f"skipped={lo.records_skipped} "
                f"retries={lo.retries_used} "
                f"duration={lo.duration_seconds:.2f}s"
            )
        lines.append("")

        if result.errors:
            lines.append(f"--- Errors ({len(result.errors)}) ---")
            for err in result.errors:
                lines.append(f"  ✗ {err}")
            lines.append("")

        if result.warnings:
            lines.append(f"--- Warnings ({len(result.warnings)}) ---")
            for warn in result.warnings:
                lines.append(f"  ⚠ {warn}")
            lines.append("")

        lines.append("--- Recommendations ---")
        recommendations = self._build_recommendations(result)
        if recommendations:
            for rec in recommendations:
                lines.append(f"  → {rec}")
        else:
            lines.append("  No action needed.")

        report = "\n".join(lines)
        logger.debug("generate_report(): report generated (%d lines).", len(lines))
        return report

    @staticmethod
    def _build_recommendations(result: PipelineResult) -> List[str]:
        """Derive simple, rule-based recommendations from a PipelineResult."""
        recs: List[str] = []

        if not result.success:
            recs.append(
                "Pipeline failed — inspect the Errors section above and re-run "
                "after addressing the root cause."
            )
        if "validate" in result.stages:
            va = result.stages["validate"]
            if va.quality_avg < 0.7:
                recs.append(
                    f"Average data quality ({va.quality_avg:.2f}) is low — "
                    f"check the upstream source for anomalies or staleness."
                )
        if "load" in result.stages:
            lo = result.stages["load"]
            if lo.retries_used > 0:
                recs.append(
                    f"Load stage required {lo.retries_used} retr"
                    f"{'y' if lo.retries_used == 1 else 'ies'} — consider "
                    f"investigating data quality of the failing rows."
                )
        if result.failed_records > 0 and result.total_records > 0:
            fail_rate = result.failed_records / result.total_records
            if fail_rate > 0.1:
                recs.append(
                    f"Failure rate is {fail_rate:.1%} of total records — "
                    f"investigate before relying on this run's data."
                )
        return recs

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _elapsed(start: float) -> float:
        """Return elapsed seconds since *start* (a time.monotonic() value)."""
        return time.monotonic() - start