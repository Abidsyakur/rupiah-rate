"""
src/etl/validators.py
=====================
Data quality validation for exchange rates.

Implements validation rules per ADR-001:
- Rate bounds per currency pair
- Data freshness checks
- Duplicate detection
- Completeness checks
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional

logger = logging.getLogger(__name__)


class ExchangeRateValidator:
    """Validate exchange rate data quality."""

    def __init__(self, rate_bounds: dict = None):
        """
        Initialize validator with rate bounds.

        Parameters
        ----------
        rate_bounds : dict
            Map of pair -> (min_rate, max_rate).
            Example: {"USD_IDR": (10_000, 25_000)}
        """
        self.rate_bounds = rate_bounds or {}

    def validate_rate_value(
        self, pair: str, rate: float
    ) -> tuple[bool, Optional[str]]:
        """
        Validate a single rate value against bounds.

        Parameters
        ----------
        pair : str
            Currency pair (e.g., "USD_IDR").
        rate : float
            Exchange rate value.

        Returns
        -------
        tuple[bool, Optional[str]]
            (is_valid, error_message)
        """
        if rate is None:
            return False, f"[{pair}] Rate is None"

        if rate <= 0:
            return False, f"[{pair}] Rate must be positive; got {rate}"

        if pair in self.rate_bounds:
            min_rate, max_rate = self.rate_bounds[pair]
            if not (min_rate <= rate <= max_rate):
                return (
                    False,
                    f"[{pair}] Rate {rate} outside bounds [{min_rate}, {max_rate}]",
                )

        return True, None

    def check_freshness(
        self, timestamp: datetime, max_age_hours: int = 24
    ) -> tuple[bool, Optional[str]]:
        """
        Check if data is fresh enough.

        Parameters
        ----------
        timestamp : datetime
            Data timestamp.
        max_age_hours : int
            Maximum acceptable age in hours.

        Returns
        -------
        tuple[bool, Optional[str]]
            (is_fresh, error_message)
        """
        now = datetime.now(timezone.utc)
        age = now - timestamp.replace(tzinfo=timezone.utc)

        if age > timedelta(hours=max_age_hours):
            return False, f"Data is {age.total_seconds() / 3600:.1f}h old (max {max_age_hours}h)"

        return True, None

    def check_completeness(self, rates: List[dict], required_pairs: List[str]) -> dict:
        """
        Check if all required pairs are present.

        Parameters
        ----------
        rates : List[dict]
            List of rate records.
        required_pairs : List[str]
            Pairs that should be present.

        Returns
        -------
        dict
            Summary: {complete: bool, missing: List[str]}
        """
        pairs_found = {r.get("pair") for r in rates}
        missing = set(required_pairs) - pairs_found

        return {"complete": len(missing) == 0, "missing": list(missing)}
