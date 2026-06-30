"""
src/main.py
===========
Main entry point for rupiah-rate extraction system.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure both root and src are in path for imports
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config import get_config
from utils.logging import setup_logging

logger = setup_logging(log_level="INFO")


def main():
    config = get_config()
    logger.info("=" * 70)
    logger.info("Rupiah Exchange Rate Intelligence System")
    logger.info(f"Environment: {type(config).__name__}")
    logger.info(f"Start time: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 70)

    try:
        from etl.pipeline import EtlPipeline
        from etl.extractors import get_extractor

        # 1. Kita buat Adapter untuk menyuntikkan ID mata uang secara otomatis
        class CurrencyEnricher:
            def __init__(self, source_name):
                self.extractor = get_extractor(source_name)
                # Mapping ID sementara (sesuaikan jika ada tabel master currencies di DB)
                self.currency_map = {"USD": 1, "EUR": 2, "SGD": 3, "JPY": 4, "IDR": 5}

            def fetch_rates(self, pairs):
                result = self.extractor.fetch_rates(pairs)
                # Looping data dari internet, lalu tambahkan ID-nya
                for row in result.get("rates", []):
                    pair = row.get("pair", "")
                    if "_" in pair:
                        base, quote = pair.split("_")
                        row["from_currency_id"] = self.currency_map.get(base, 99)
                        row["to_currency_id"] = self.currency_map.get(quote, 5)
                return result

        # 2. Masukkan adapter kita ke dalam pipeline
        enricher = CurrencyEnricher("yfinance")
        pipeline = EtlPipeline(extractor=enricher)

        # 3. Jalankan pipeline
        pairs = ["USD_IDR", "EUR_IDR", "SGD_IDR", "JPY_IDR"]
        result = pipeline.run(
            currency_pairs=pairs, 
            source="yfinance", 
            source_id=1
        )

        if result.success:
            logger.info(f"Pipeline success! Processed {result.processed_records} records.")
        else:
            logger.error("Pipeline finished with errors. Check the logs.")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        return 1

    logger.info(f"End time: {datetime.now(timezone.utc).isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())