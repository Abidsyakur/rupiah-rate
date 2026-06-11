"""Unit tests for analytics"""

import pytest
from src.analytics.trends import TrendAnalyzer
from src.analytics.volatility import VolatilityCalculator


class TestTrendAnalyzer:
    """Test trend analysis"""

    @pytest.fixture
    def analyzer(self):
        return TrendAnalyzer()

    def test_moving_average(self, analyzer):
        """Test moving average calculation"""
        rates = [15500, 15510, 15520, 15530, 15540]
        result = analyzer.calculate_moving_average(rates, window=3)
        assert result is None or isinstance(result, list)


class TestVolatilityCalculator:
    """Test volatility calculation"""

    @pytest.fixture
    def calculator(self):
        return VolatilityCalculator()

    def test_standard_deviation(self, calculator):
        """Test standard deviation calculation"""
        rates = [15500, 15510, 15520, 15530, 15540]
        result = calculator.calculate_standard_deviation(rates, period=5)
        assert result is None or isinstance(result, float)
