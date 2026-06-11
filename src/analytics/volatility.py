"""
src/analytics/volatility.py
===========================
Exchange rate volatility analysis.

Phase 2+ component.
"""

from __future__ import annotations

import logging
from typing import List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class VolatilityCalculator:
    """Calculate exchange rate volatility."""

    @staticmethod
    def calculate_std_dev(rates: List[float], period: int) -> Optional[float]:
        """
        Calculate standard deviation.

        Parameters
        ----------
        rates : List[float]
            Historical rates.
        period : int
            Period length.

        Returns
        -------
        Optional[float]
            Standard deviation or None if insufficient data.
        """
        if len(rates) < period:
            return None

        return float(np.std(rates[-period:]))

    @staticmethod
    def calculate_bollinger_bands(
        rates: List[float], period: int = 20, std_devs: float = 2.0
    ) -> Optional[dict]:
        """
        Calculate Bollinger Bands.

        Parameters
        ----------
        rates : List[float]
            Historical rates.
        period : int
            Moving average period.
        std_devs : float
            Number of standard deviations.

        Returns
        -------
        Optional[dict]
            Bands: {upper, middle, lower} or None if insufficient data.
        """
        if len(rates) < period:
            return None

        series = pd.Series(rates[-period:])
        ma = series.mean()
        std = series.std()

        return {
            "upper": float(ma + std_devs * std),
            "middle": float(ma),
            "lower": float(ma - std_devs * std),
        }
