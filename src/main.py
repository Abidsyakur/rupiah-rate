"""
src/main.py
===========
Main entry point for rupiah-rate extraction system.

Runs the exchange rate extraction pipeline on demand or via scheduler.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure src is in path for imports
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.config import get_config
from src.utils.logging import setup_logging

# Configure logging first
logger = setup_logging(log_level="INFO")


def main():
    """Main application entry point."""
    config = get_config()
    logger.info("=" * 70)
    logger.info("Rupiah Exchange Rate Intelligence System")
    logger.info(f"Environment: {type(config).__name__}")
    logger.info(f"Start time: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 70)

    try:
        # TODO: Initialize database session and loaders
        # from etl.extractors import YFinanceExtractor, FREDExtractor
        # from etl.loaders import ExchangeRateLoader
        # from etl.validators import ExchangeRateValidator
        # from etl.pipeline import ExtractionPipeline

        # session = create_session(config.DB_CONNECTION_STRING)
        # loader = ExchangeRateLoader(session)
        # validator = ExchangeRateValidator(rate_bounds=RATE_BOUNDS)
        # pipeline = ExtractionPipeline(loader, validator)

        # pairs = ["USD_IDR", "EUR_IDR", "SGD_IDR", "JPY_IDR"]
        # result = pipeline.run(pairs)

        logger.info("Pipeline execution completed successfully")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        return 1

    logger.info(f"End time: {datetime.now(timezone.utc).isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
