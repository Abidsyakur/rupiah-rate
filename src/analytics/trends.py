"""
src/analytics/trends.py
=======================
Exchange rate trend analysis.

Phase 2+ component.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class TrendAnalyzer:
    """Analyze exchange rate trends."""

    @staticmethod
    def calculate_moving_average(
        rates: List[float], window: int
    ) -> List[Optional[float]]:
        """
        Calculate moving average.

        Parameters
        ----------
        rates : List[float]
            Historical rates.
        window : int
            Window size in periods.

        Returns
        -------
        List[Optional[float]]
            Moving average values (None for initial periods).
        """
        if len(rates) < window:
            return [None] * len(rates)

        return pd.Series(rates).rolling(window=window).mean().tolist()

    @staticmethod
    def detect_trend(rates: List[float], period: int = 7) -> str:
        """
        Detect trend direction.

        Parameters
        ----------
        rates : List[float]
            Historical rates.
        period : int
            Period for comparison.

        Returns
        -------
        str
            "uptrend", "downtrend", or "sideways"
        """
        if len(rates) < period + 1:
            return "insufficient_data"

        recent = np.mean(rates[-period:])
        previous = np.mean(rates[-2 * period : -period])

        if recent > previous * 1.01:
            return "uptrend"
        elif recent < previous * 0.99:
            return "downtrend"
        else:
            return "sideways"
