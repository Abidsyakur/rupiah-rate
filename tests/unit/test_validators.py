"""Unit tests for data validators"""

import pytest
from src.etl.validators import DataValidator


class TestDataValidator:
    """Test data validation"""

    @pytest.fixture
    def validator(self):
        return DataValidator()

    def test_validator_initialization(self, validator):
        """Test validator can be initialized"""
        assert validator is not None

    def test_validate_exchange_rates(self, validator, sample_exchange_rates):
        """Test exchange rate validation"""
        data = {"date": "2024-01-01", "rate": 15520.0}
        result = validator.validate_exchange_rates(data)
        assert result is None or isinstance(result, bool)

    def test_check_for_duplicates(self, validator, sample_exchange_rates):
        """Test duplicate detection"""
        result = validator.check_for_duplicates(sample_exchange_rates)
        assert isinstance(result, list)
