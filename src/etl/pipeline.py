"""
src/etl/pipeline.py
===================
Exchange rate extraction pipeline.

Orchestrates the complete flow:
  1. Extract from Yfinance and FRED
  2. Validate data quality
  3. Load into database
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from etl.extractors import ExtractionResult, get_extractor
from etl.loaders import ExchangeRateLoader
from etl.validators import ExchangeRateValidator

logger = logging.getLogger(__name__)


class ExtractionPipeline:
    """Orchestrate exchange rate extraction, validation, and loading."""

    def __init__(
        self,
        loader: ExchangeRateLoader,
        validator: ExchangeRateValidator,
        sources: Optional[List[str]] = None,
    ):
        """
        Initialize pipeline.

        Parameters
        ----------
        loader : ExchangeRateLoader
            Database loader instance.
        validator : ExchangeRateValidator
            Data validator instance.
        sources : Optional[List[str]]
            Data sources to use (default: ["yfinance", "fred"]).
        """
        self.loader = loader
        self.validator = validator
        self.sources = sources or ["yfinance", "fred"]
        self.results: List[ExtractionResult] = []

    def run(self, pairs: List[str]) -> dict:
        """
        Run the complete extraction pipeline.

        Parameters
        ----------
        pairs : List[str]
            Currency pairs to extract (e.g., ["USD_IDR", "EUR_IDR"]).

        Returns
        -------
        dict
            Pipeline execution summary.
        """
        logger.info(f"Starting extraction pipeline for pairs: {pairs}")
        summary = {
            "start_time": datetime.now(timezone.utc).isoformat(),
            "extracted": 0,
            "validated": 0,
            "loaded": 0,
            "errors": [],
        }

        # Extract from each source
        for source_name in self.sources:
            try:
                extractor = get_extractor(source_name)
                logger.info(f"Extracting from {source_name}...")

                result = extractor.fetch_rates(pairs)
                self.results.append(result)

                logger.info(f"Extracted {len(result.rates)} rates from {source_name}")
                summary["extracted"] += len(result.rates)

            except Exception as e:
                logger.error(f"Error extracting from {source_name}: {e}")
                summary["errors"].append(f"{source_name}: {str(e)}")

        # Validate all extracted rates
        all_rates = []
        for result in self.results:
            for rate in result.rates:
                is_valid, error_msg = self.validator.validate_rate_value(
                    rate.pair, rate.rate
                )
                if not is_valid:
                    logger.warning(error_msg)
                    summary["errors"].append(error_msg)
                else:
                    all_rates.append(rate.to_dict())
                    summary["validated"] += 1

        # Load into database
        if all_rates:
            try:
                load_summary = self.loader.upsert_rates(all_rates)
                summary["loaded"] = load_summary["inserted"] + load_summary["updated"]
                logger.info(f"Loaded {summary['loaded']} rates into database")
            except Exception as e:
                logger.error(f"Error loading rates: {e}")
                summary["errors"].append(f"Load error: {str(e)}")

        summary["end_time"] = datetime.now(timezone.utc).isoformat()
        logger.info(f"Pipeline completed: {summary}")
        return summary
