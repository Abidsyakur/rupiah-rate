"""Integration tests for complete pipeline"""

import pytest
from src.etl.pipeline import ETLPipeline


class TestETLPipeline:
    """Test complete ETL pipeline"""

    @pytest.fixture
    def pipeline(self):
        # Create mock components
        return ETLPipeline(
            extractor=None,
            transformer=None,
            loader=None,
            validator=None
        )

    def test_pipeline_initialization(self, pipeline):
        """Test pipeline can be initialized"""
        assert pipeline is not None

    @pytest.mark.skip(reason="Requires real database connection")
    def test_daily_pipeline_execution(self, pipeline):
        """Test daily pipeline execution"""
        result = pipeline.run_daily_pipeline("2024-01-15")
        assert isinstance(result, dict)
        assert "status" in result
