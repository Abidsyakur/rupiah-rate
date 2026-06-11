"""
src/analytics/forecasting.py
============================
Exchange rate forecasting models.

Phase 3+ component - placeholder for future development.
"""

import logging

logger = logging.getLogger(__name__)


class ExchangeRateForecaster:
    """Forecast future exchange rates (Phase 3+)."""

    def arima_forecast(self, historical_rates: list, periods: int) -> list:
        """ARIMA forecasting model (TODO: implement)."""
        raise NotImplementedError("Forecasting is planned for Phase 3+")

    def ml_forecast(self, features: dict, periods: int) -> list:
        """ML-based forecasting (TODO: implement)."""
        raise NotImplementedError("ML forecasting is planned for Phase 3+")
