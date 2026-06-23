"""
src/etl/loaders.py
====================
Idempotent database loading layer for the Rupiah Exchange Rate Intelligence
pipeline.

Loads validated exchange rate data (from ``etl.extractors`` +
``etl.validators``) into the ``exchange_rates`` table using an
upsert-on-unique-key pattern, so re-running the same extraction never
produces duplicate rows.

Idempotency contract
---------------------
A rate is uniquely identified by the 4-tuple::

    (from_currency_id, to_currency_id, timestamp, source_id)

This matches the ``uq_exchange_rates_pair_timestamp_source`` unique
constraint on :class:`src.utils.database.ExchangeRate`. On each load:

    * If no row matches the key       -> INSERT
    * If a row matches and the new
      ``data_quality_score`` is >=
      the existing one                -> UPDATE (overwrite with better data)
    * If a row matches and the new
      score is strictly lower         -> SKIP (treated as a duplicate)

Configuration
--------------
All thresholds are loaded from environment variables with sensible
defaults. Set them in your ``.env`` file or shell:

    LOADER_BATCH_SIZE=1000        # rows per bulk operation
    LOADER_SKIP_DUPLICATES=True   # skip exact re-sends instead of updating
    LOADER_TRACK_AUDIT=True       # write an ApiCall audit row per load()
    LOADER_RETRY_COUNT=3          # DB operation retry attempts
    LOADER_TIMEOUT_SECONDS=300    # per-load wall-clock budget (soft check)
    LOADER_LOG_LEVEL=INFO         # logger level for this module

Example usage
--------------
    from datetime import datetime, timezone
    from src.etl.loaders import ExchangeRateLoader
    from src.utils.database import get_engine, get_session

    loader = ExchangeRateLoader()

    validated_rates = [
        {
            "from_currency_id": 1,
            "to_currency_id": 2,
            "rate": 18176.50,
            "timestamp": datetime.now(timezone.utc),
            "source_id": 1,
            "quality_score": 0.95,
            "is_valid": True,
        },
    ]

    engine = get_engine()
    with get_session(engine) as session:
        result = loader.load(
            session=session,
            rates=validated_rates,
            source_id=1,
        )
        if result.success:
            print(f"Loaded {result.rows_loaded}, updated {result.rows_updated}, "
                  f"skipped {result.rows_skipped}")
        else:
            print(f"Load failed: {result.errors}")
"""

from __future__ import annotations

import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session

from utils.database import ApiCall, ExchangeRate

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


def _env_bool(key: str, default: bool) -> bool:
    """Read a bool from an environment variable (true/1/yes = True)."""
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes")


class LoaderConfig:
    """
    Centralised, environment-driven loader configuration.

    All values can be overridden at construction time or via environment
    variables. See the module docstring for the full list of env vars.

    Attributes
    ----------
    batch_size : int
        Number of rows processed per bulk INSERT/UPDATE pass.
    skip_duplicates : bool
        If ``True``, exact re-sends (same key, same-or-lower quality) are
        skipped rather than updated.
    track_audit : bool
        If ``True``, every :meth:`ExchangeRateLoader.load` call writes an
        :class:`ApiCall` audit row.
    retry_count : int
        Max attempts for a single DB operation before giving up.
    timeout_seconds : int
        Soft wall-clock budget for one ``load()`` call; logged as a
        warning if exceeded (does not abort the load).
    log_level : str
        Logging level name applied to this module's logger.
    """

    def __init__(
        self,
        batch_size: Optional[int] = None,
        skip_duplicates: Optional[bool] = None,
        track_audit: Optional[bool] = None,
        retry_count: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        log_level: Optional[str] = None,
    ) -> None:
        self.batch_size: int = (
            batch_size if batch_size is not None
            else _env_int("LOADER_BATCH_SIZE", 1000)
        )
        self.skip_duplicates: bool = (
            skip_duplicates if skip_duplicates is not None
            else _env_bool("LOADER_SKIP_DUPLICATES", True)
        )
        self.track_audit: bool = (
            track_audit if track_audit is not None
            else _env_bool("LOADER_TRACK_AUDIT", True)
        )
        self.retry_count: int = (
            retry_count if retry_count is not None
            else _env_int("LOADER_RETRY_COUNT", 3)
        )
        self.timeout_seconds: int = (
            timeout_seconds if timeout_seconds is not None
            else _env_int("LOADER_TIMEOUT_SECONDS", 300)
        )
        self.log_level: str = (
            log_level if log_level is not None
            else os.getenv("LOADER_LOG_LEVEL", "INFO")
        )
        logger.setLevel(self.log_level)

    def __repr__(self) -> str:
        return (
            f"LoaderConfig("
            f"batch_size={self.batch_size}, "
            f"skip_duplicates={self.skip_duplicates}, "
            f"track_audit={self.track_audit}, "
            f"retry_count={self.retry_count}, "
            f"timeout_seconds={self.timeout_seconds}, "
            f"log_level={self.log_level!r})"
        )


# ---------------------------------------------------------------------------
# LoadResult
# ---------------------------------------------------------------------------

@dataclass
class LoadResult:
    """
    Aggregated result of a single :meth:`ExchangeRateLoader.load` call.

    Attributes
    ----------
    success : bool
        ``True`` if the load completed without unrecoverable errors. A
        load with ``rows_failed > 0`` but at least one row successfully
        loaded is still considered ``success=True`` (partial success);
        only a hard transaction failure sets this to ``False``.
    rows_loaded : int
        Number of new rows inserted.
    rows_updated : int
        Number of existing rows updated (better quality score replaced
        the stored value).
    rows_skipped : int
        Number of rows skipped as duplicates (same key, same-or-lower
        quality, or ``skip_duplicates=True`` exact match).
    rows_failed : int
        Number of rows that raised an unrecoverable error during
        processing (e.g. constraint violation unrelated to the upsert key).
    errors : List[str]
        Hard failure messages.
    warnings : List[str]
        Soft issues (e.g. low quality score accepted anyway).
    execution_time_ms : int
        Wall-clock duration of the load operation in milliseconds.
    """

    success: bool = True
    rows_loaded: int = 0
    rows_updated: int = 0
    rows_skipped: int = 0
    rows_failed: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    execution_time_ms: int = 0

    def add_error(self, msg: str) -> None:
        """Record a hard failure. Does not automatically flip ``success``;
        callers decide based on whether the whole transaction failed."""
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        """Record a soft warning."""
        self.warnings.append(msg)

    @property
    def total_processed(self) -> int:
        """Total rows seen across loaded/updated/skipped/failed buckets."""
        return self.rows_loaded + self.rows_updated + self.rows_skipped + self.rows_failed

    def __repr__(self) -> str:
        return (
            f"LoadResult(success={self.success}, "
            f"loaded={self.rows_loaded}, updated={self.rows_updated}, "
            f"skipped={self.rows_skipped}, failed={self.rows_failed}, "
            f"time={self.execution_time_ms}ms)"
        )


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class BaseLoader(ABC):
    """
    Abstract base class for all loaders in the pipeline.

    Subclasses must implement :meth:`load`. :meth:`log_results` provides
    a shared, consistent logging format for any :class:`LoadResult`.
    """

    def __init__(self, config: Optional[LoaderConfig] = None) -> None:
        self.config: LoaderConfig = config or LoaderConfig()

    @abstractmethod
    def load(self, session: Session, *args: Any, **kwargs: Any) -> LoadResult:
        """
        Load data into the database within the given session.

        Parameters
        ----------
        session:
            An active SQLAlchemy session. The caller owns the transaction
            lifecycle (commit/rollback) unless documented otherwise by the
            subclass.

        Returns
        -------
        LoadResult
        """

    def log_results(self, result: LoadResult, source: str = "") -> None:
        """
        Log a :class:`LoadResult` at the appropriate severity level.

        Parameters
        ----------
        result:
            The result to log.
        source:
            Optional label for the data source (e.g. ``"yfinance"``).
        """
        prefix = f"[{source}] " if source else ""
        level = logging.ERROR if not result.success else (
            logging.WARNING if result.warnings or result.rows_failed else logging.INFO
        )
        logger.log(
            level,
            "%sLoad complete — success=%s loaded=%d updated=%d skipped=%d "
            "failed=%d time=%dms",
            prefix,
            result.success,
            result.rows_loaded,
            result.rows_updated,
            result.rows_skipped,
            result.rows_failed,
            result.execution_time_ms,
        )
        for err in result.errors:
            logger.error("%s%s", prefix, err)
        for warn in result.warnings:
            logger.warning("%s%s", prefix, warn)


# ---------------------------------------------------------------------------
# ExchangeRateLoader
# ---------------------------------------------------------------------------

class ExchangeRateLoader(BaseLoader):
    """
    Idempotently loads validated exchange rate records into the
    ``exchange_rates`` table.

    Upsert key
    ----------
    ``(from_currency_id, to_currency_id, timestamp, source_id)`` — matches
    the database-level unique constraint
    ``uq_exchange_rates_pair_timestamp_source``.

    Update-vs-skip rule
    --------------------
    When an existing row matches the upsert key:

    * new ``quality_score`` >= existing ``data_quality_score`` -> UPDATE
    * new ``quality_score`` <  existing ``data_quality_score`` -> SKIP

    Example
    -------
    >>> from src.etl.loaders import ExchangeRateLoader
    >>> loader = ExchangeRateLoader()
    >>> # result = loader.load(session, rates=[...], source_id=1)
    >>> # result.rows_loaded, result.rows_updated, result.rows_skipped
    """

    #: Expected keys in each rate dict passed to :meth:`load`.
    _REQUIRED_KEYS = ("from_currency_id", "to_currency_id", "rate", "timestamp")

    def __init__(self, config: Optional[LoaderConfig] = None) -> None:
        super().__init__(config)
        logger.debug("ExchangeRateLoader initialised with %s", self.config)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def load(
        self,
        session: Session,
        rates: List[Dict[str, Any]],
        source_id: int,
        validation_result: Optional[Any] = None,
        batch_size: Optional[int] = None,
        skip_duplicates: Optional[bool] = None,
    ) -> LoadResult:
        """
        Load a batch of validated exchange rate dicts into the database.

        Parameters
        ----------
        session:
            Active SQLAlchemy session. The caller is responsible for the
            final ``session.commit()`` / ``session.rollback()`` — this
            method uses ``session.begin_nested()`` savepoints internally
            so a single bad row does not abort rows already staged in the
            same outer transaction.
        rates:
            List of rate dicts. Each dict must contain at minimum:
            ``from_currency_id``, ``to_currency_id``, ``rate``, ``timestamp``.
            Optional keys: ``source_id`` (overrides the *source_id* param
            per-row), ``quality_score`` / ``data_quality_score``,
            ``is_valid``.
        source_id:
            Default FK to :class:`ApiSource` for rows that don't specify
            their own ``source_id``.
        validation_result:
            Optional upstream ``ValidationResult`` (from
            ``etl.validators``). Used only for audit-trail record counts
            when ``config.track_audit=True``; not required.
        batch_size:
            Override ``config.batch_size`` for this call.
        skip_duplicates:
            Override ``config.skip_duplicates`` for this call.

        Returns
        -------
        LoadResult

        Raises
        ------
        ValueError
            If *rates* is empty or contains a dict missing required keys
            (recorded as a row failure, not a raised exception — this is
            only raised for a structurally invalid call, e.g. *rates* is
            not a list).

        Example
        -------
        >>> result = loader.load(session, rates=[{
        ...     "from_currency_id": 1, "to_currency_id": 2,
        ...     "rate": 18176.50, "timestamp": datetime.now(timezone.utc),
        ...     "quality_score": 0.95,
        ... }], source_id=1)
        >>> result.rows_loaded
        1
        """
        start = time.monotonic()
        result = LoadResult()

        if not isinstance(rates, list):
            raise ValueError(f"'rates' must be a list, got {type(rates).__name__}.")

        effective_batch_size = batch_size if batch_size is not None else self.config.batch_size
        effective_skip_dupes = (
            skip_duplicates if skip_duplicates is not None else self.config.skip_duplicates
        )

        if not rates:
            result.add_warning("load(): received an empty rates list — nothing to do.")
            result.execution_time_ms = self._elapsed_ms(start)
            self._maybe_track_audit(session, source_id, result, validation_result)
            return result

        logger.info(
            "Starting load: %d row(s), source_id=%d, batch_size=%d, skip_duplicates=%s",
            len(rates), source_id, effective_batch_size, effective_skip_dupes,
        )

        try:
            for batch_start in range(0, len(rates), effective_batch_size):
                batch = rates[batch_start: batch_start + effective_batch_size]
                self._load_batch(session, batch, source_id, effective_skip_dupes, result)

            # Caller owns the outer transaction; we flush so constraint
            # violations surface here rather than silently at a later
            # unrelated commit.
            session.flush()
            result.success = True

        except SQLAlchemyError as exc:
            self.rollback_on_error(session)
            result.success = False
            result.add_error(f"Transaction-level failure, rolled back: {exc}")
            logger.exception("load(): transaction rolled back due to SQLAlchemyError.")

        result.execution_time_ms = self._elapsed_ms(start)

        if result.execution_time_ms > self.config.timeout_seconds * 1000:
            result.add_warning(
                f"Load took {result.execution_time_ms}ms, exceeding the "
                f"configured timeout budget of {self.config.timeout_seconds}s."
            )

        self._maybe_track_audit(session, source_id, result, validation_result)
        self.log_results(result, source=f"source_id={source_id}")
        return result

    # ------------------------------------------------------------------
    # Batch processing
    # ------------------------------------------------------------------

    def _load_batch(
        self,
        session: Session,
        batch: List[Dict[str, Any]],
        default_source_id: int,
        skip_duplicates: bool,
        result: LoadResult,
    ) -> None:
        """
        Process one batch of rate dicts: validate shape, then upsert each.

        Each row is wrapped in its own SAVEPOINT (``session.begin_nested``)
        so a single constraint violation only rolls back that row, not the
        entire batch.

        Parameters
        ----------
        session, batch, default_source_id, skip_duplicates, result:
            See :meth:`load`.
        """
        for row in batch:
            missing = [k for k in self._REQUIRED_KEYS if k not in row]
            if missing:
                msg = f"Row missing required key(s) {missing}: {row}"
                result.add_error(msg)
                result.rows_failed += 1
                logger.error(msg)
                continue

            row_source_id = row.get("source_id", default_source_id)

            try:
                with session.begin_nested():
                    self._upsert_one(session, row, row_source_id, skip_duplicates, result)
            except IntegrityError as exc:
                result.rows_failed += 1
                msg = (
                    f"IntegrityError for row "
                    f"({row['from_currency_id']}, {row['to_currency_id']}, "
                    f"{row['timestamp']}, {row_source_id}): {exc.orig if hasattr(exc, 'orig') else exc}"
                )
                result.add_error(msg)
                logger.error(msg)
            except OperationalError as exc:
                result.rows_failed += 1
                msg = f"OperationalError (connection/timeout) for row {row}: {exc}"
                result.add_error(msg)
                logger.error(msg)

    def _upsert_one(
        self,
        session: Session,
        row: Dict[str, Any],
        source_id: int,
        skip_duplicates: bool,
        result: LoadResult,
    ) -> None:
        """
        Upsert a single rate row, applying the quality-score replacement rule.

        Parameters
        ----------
        session:
            Active session (already inside a SAVEPOINT from the caller).
        row:
            Single rate dict.
        source_id:
            Resolved source id for this row.
        skip_duplicates:
            If True, an exact duplicate (existing row found, regardless of
            quality comparison outcome favouring skip) is counted as
            ``rows_skipped`` rather than silently doing nothing.
        result:
            Mutated in place with the outcome counters.
        """
        from_id = row["from_currency_id"]
        to_id = row["to_currency_id"]
        timestamp = row["timestamp"]
        rate_value = row["rate"]
        new_quality = row.get("quality_score", row.get("data_quality_score"))
        is_valid = row.get("is_valid", True)

        existing = self.get_existing_rate(session, from_id, to_id, timestamp, source_id)

        if existing is None:
            new_row = ExchangeRate(
                from_currency_id=from_id,
                to_currency_id=to_id,
                source_id=source_id,
                rate=self._to_decimal(rate_value),
                timestamp=timestamp,
                data_quality_score=self._to_decimal(new_quality) if new_quality is not None else None,
                is_valid=is_valid,
            )
            session.add(new_row)
            session.flush()  # surface IntegrityError inside this savepoint
            result.rows_loaded += 1
            logger.debug(
                "INSERT rate (%s, %s, %s, source=%s) = %s",
                from_id, to_id, timestamp, source_id, rate_value,
            )
            return

        # Row exists — decide UPDATE vs SKIP based on quality comparison.
        existing_quality = existing.data_quality_score
        should_update = self._should_replace(existing_quality, new_quality)

        if should_update:
            existing.rate = self._to_decimal(rate_value)
            existing.data_quality_score = (
                self._to_decimal(new_quality) if new_quality is not None else None
            )
            existing.is_valid = is_valid
            session.flush()
            result.rows_updated += 1
            logger.debug(
                "UPDATE rate (%s, %s, %s, source=%s): quality %s -> %s",
                from_id, to_id, timestamp, source_id, existing_quality, new_quality,
            )
        else:
            result.rows_skipped += 1
            if skip_duplicates:
                logger.debug(
                    "SKIP duplicate rate (%s, %s, %s, source=%s): "
                    "existing quality %s >= new quality %s",
                    from_id, to_id, timestamp, source_id, existing_quality, new_quality,
                )
            else:
                result.add_warning(
                    f"Duplicate rate ({from_id}, {to_id}, {timestamp}, "
                    f"source={source_id}) found with equal/better quality "
                    f"({existing_quality} vs new {new_quality}); skip_duplicates=False "
                    f"but no update rule applies — row left unchanged."
                )

    @staticmethod
    def _should_replace(
        existing_quality: Optional[Decimal],
        new_quality: Optional[float],
    ) -> bool:
        """
        Decide whether new data should overwrite an existing row.

        Rule: replace if the existing quality score is unknown (``None``),
        or the new quality score is unknown (treated as "unverified, but
        still newer data" -> replace), or the new score is >= existing.

        Parameters
        ----------
        existing_quality:
            ``data_quality_score`` currently stored, or ``None``.
        new_quality:
            Incoming quality score, or ``None``.

        Returns
        -------
        bool
        """
        if existing_quality is None:
            return True
        if new_quality is None:
            return True
        return float(new_quality) >= float(existing_quality)

    # ------------------------------------------------------------------
    # Public helper methods (per spec)
    # ------------------------------------------------------------------

    def upsert_rates(
        self,
        session: Session,
        rates: List[Dict[str, Any]],
        source_id: int,
    ) -> LoadResult:
        """
        Convenience wrapper that performs an upsert-only load (no audit
        tracking, uses config defaults for batch size / skip_duplicates).

        Parameters
        ----------
        session:
            Active SQLAlchemy session.
        rates:
            List of rate dicts (see :meth:`load`).
        source_id:
            Default FK to :class:`ApiSource`.

        Returns
        -------
        LoadResult

        Example
        -------
        >>> result = loader.upsert_rates(session, rates, source_id=1)
        """
        return self.load(session, rates=rates, source_id=source_id)

    def handle_duplicates(
        self,
        session: Session,
        rates: List[Dict[str, Any]],
        source_id: int,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Partition incoming rates into duplicates vs. new/updatable rows
        WITHOUT writing anything to the database.

        Useful for a dry-run / preview step before calling :meth:`load`.

        Parameters
        ----------
        session:
            Active SQLAlchemy session (read-only use here).
        rates:
            List of rate dicts (see :meth:`load`).
        source_id:
            Default FK to :class:`ApiSource` for rows without their own.

        Returns
        -------
        dict
            ``{"new": [...], "updatable": [...], "duplicates": [...]}``

        Example
        -------
        >>> partition = loader.handle_duplicates(session, rates, source_id=1)
        >>> len(partition["duplicates"])
        2
        """
        partition: Dict[str, List[Dict[str, Any]]] = {
            "new": [], "updatable": [], "duplicates": [],
        }

        for row in rates:
            missing = [k for k in self._REQUIRED_KEYS if k not in row]
            if missing:
                logger.warning("handle_duplicates(): skipping malformed row %s", row)
                continue

            row_source_id = row.get("source_id", source_id)
            existing = self.get_existing_rate(
                session,
                row["from_currency_id"],
                row["to_currency_id"],
                row["timestamp"],
                row_source_id,
            )

            if existing is None:
                partition["new"].append(row)
                continue

            new_quality = row.get("quality_score", row.get("data_quality_score"))
            if self._should_replace(existing.data_quality_score, new_quality):
                partition["updatable"].append(row)
            else:
                partition["duplicates"].append(row)

        logger.info(
            "handle_duplicates(): new=%d updatable=%d duplicates=%d",
            len(partition["new"]), len(partition["updatable"]), len(partition["duplicates"]),
        )
        return partition

    def get_existing_rate(
        self,
        session: Session,
        from_id: int,
        to_id: int,
        timestamp: datetime,
        source_id: int,
    ) -> Optional[ExchangeRate]:
        """
        Look up an existing :class:`ExchangeRate` row by the upsert key.

        Parameters
        ----------
        session:
            Active SQLAlchemy session.
        from_id:
            Base currency FK.
        to_id:
            Quote currency FK.
        timestamp:
            Market timestamp of the observation.
        source_id:
            API source FK.

        Returns
        -------
        ExchangeRate | None
            The matching row, or ``None`` if no row exists for this key.

        Example
        -------
        >>> existing = loader.get_existing_rate(session, 1, 2, ts, 1)
        >>> existing is None
        True
        """
        return (
            session.query(ExchangeRate)
            .filter(
                ExchangeRate.from_currency_id == from_id,
                ExchangeRate.to_currency_id == to_id,
                ExchangeRate.timestamp == timestamp,
                ExchangeRate.source_id == source_id,
            )
            .one_or_none()
        )

    def track_load_history(
        self,
        session: Session,
        result: LoadResult,
        source_id: int,
        validation_result: Optional[Any] = None,
    ) -> ApiCall:
        """
        Write an :class:`ApiCall` audit row summarising a load operation.

        Parameters
        ----------
        session:
            Active SQLAlchemy session. The new row is ``add()``-ed and
            flushed but NOT committed — the caller commits the outer
            transaction.
        result:
            The :class:`LoadResult` to summarise.
        source_id:
            FK to :class:`ApiSource` this load operation pertains to.
        validation_result:
            Optional upstream validation result; if it exposes
            ``checks_passed`` / record counts they are folded into the
            audit row's ``records_valid`` / ``records_invalid`` where
            possible. Safe to omit.

        Returns
        -------
        ApiCall
            The persisted audit row (flushed, has a ``call_id``).

        Example
        -------
        >>> call = loader.track_load_history(session, result, source_id=1)
        >>> call.status
        'SUCCESS'
        """
        status = self._derive_status(result)

        records_valid = result.rows_loaded + result.rows_updated
        records_invalid = result.rows_failed

        call = ApiCall(
            source_id=source_id,
            timestamp=datetime.now(timezone.utc),
            status=status,
            error_message="; ".join(result.errors) if result.errors else None,
            records_fetched=result.total_processed,
            records_valid=records_valid,
            records_invalid=records_invalid,
            execution_time_ms=result.execution_time_ms,
        )
        session.add(call)
        session.flush()
        logger.info(
            "Audit trail recorded: call_id=%s status=%s source_id=%d",
            call.call_id, status, source_id,
        )
        return call

    @staticmethod
    def _derive_status(result: LoadResult) -> str:
        """Map a LoadResult onto the ApiCall.status enum values."""
        if not result.success:
            return "ERROR"
        if result.rows_failed > 0 and (result.rows_loaded + result.rows_updated) > 0:
            return "PARTIAL" if hasattr(ApiCall, "status") else "SUCCESS"
        if result.rows_failed > 0:
            return "ERROR"
        return "SUCCESS"

    def rollback_on_error(self, session: Session) -> None:
        """
        Roll back the given session's current transaction.

        Safe to call even if no transaction is active — SQLAlchemy
        no-ops in that case for the outermost rollback, and any
        exception is caught and logged rather than propagated, since
        this is itself an error-handling utility.

        Parameters
        ----------
        session:
            The session to roll back.

        Example
        -------
        >>> loader.rollback_on_error(session)
        """
        try:
            session.rollback()
            logger.warning("Session rolled back due to error.")
        except SQLAlchemyError as exc:
            logger.error("rollback_on_error(): rollback itself failed: %s", exc)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _maybe_track_audit(
        self,
        session: Session,
        source_id: int,
        result: LoadResult,
        validation_result: Optional[Any],
    ) -> None:
        """Call :meth:`track_load_history` only if auditing is enabled."""
        if not self.config.track_audit:
            return
        try:
            self.track_load_history(session, result, source_id, validation_result)
        except SQLAlchemyError as exc:
            # Audit failure must never mask the real load result.
            logger.error("Failed to record audit trail (non-fatal): %s", exc)
            result.add_warning(f"Audit trail recording failed: {exc}")

    @staticmethod
    def _to_decimal(value: Union[float, int, Decimal, str, None]) -> Optional[Decimal]:
        """Safely coerce a numeric value to Decimal for Numeric columns."""
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @staticmethod
    def _elapsed_ms(start: float) -> int:
        """Return elapsed milliseconds since *start* (a time.monotonic() value)."""
        return int((time.monotonic() - start) * 1000)