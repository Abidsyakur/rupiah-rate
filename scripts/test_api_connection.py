#!/usr/bin/env python3
"""
scripts/test_api_connections.py
================================
Standalone smoke-test for the two external data sources used by the
extraction pipeline (ADR-001):

  1. yfinance  — via the `yfinance` Python library (no API key needed)
  2. FRED API  — via `requests` + `FRED_API_KEY` (the "backdoor" key,
                  per project decision: we keep the raw REST call instead
                  of `fredapi` for full control over retries/timeouts)

This script is intentionally dependency-light and side-effect-free: it does
NOT touch the database. It only prints whether each source is reachable and
what data comes back, so you can confirm API access before wiring up the
full pipeline.

Usage
-----
    # Load FRED_API_KEY from a .env file (if python-dotenv is installed)
    # or export it directly:
    export FRED_API_KEY="your_key_here"

    python scripts/test_api_connections.py

    # Test only one source:
    python scripts/test_api_connections.py --source yfinance
    python scripts/test_api_connections.py --source fred

Exit codes
----------
    0  All requested sources reachable and returned valid data
    1  At least one source failed (see printed errors)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Make src/ importable so we can reuse the real extractor classes
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ---------------------------------------------------------------------------
# Optional: load .env if python-dotenv is available
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore

    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
        print(f"[setup] Loaded environment from {env_path}")
    else:
        print(f"[setup] No .env file found at {env_path} (skipping)")
except ImportError:
    print("[setup] python-dotenv not installed — relying on shell environment "
          "variables only. (pip install python-dotenv to enable .env support)")

print()

from src.etl.extractors import FREDExtractor, YFinanceExtractor  # noqa: E402


# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------

def _print_header(title: str) -> None:
    print("=" * 70)
    print(f" {title}")
    print("=" * 70)


def _print_result(result: dict) -> bool:
    """Print a fetch_rates() result dict. Returns True if fully successful."""
    rates = result.get("rates", [])
    errors = result.get("errors", [])

    if rates:
        print(f"  [OK] {len(rates)} rate(s) fetched:")
        for r in rates:
            print(
                f"       {r['pair']:8s} = {r['rate']:>12.4f}  "
                f"(ts={r['timestamp']}, quality={r['data_quality_score']})"
            )
    else:
        print("  [WARN] No rates returned.")

    if errors:
        print(f"  [ERROR] {len(errors)} error(s):")
        for e in errors:
            print(f"       - {e}")

    return bool(rates) and not errors


# ---------------------------------------------------------------------------
# yfinance test
# ---------------------------------------------------------------------------

def test_yfinance() -> bool:
    """
    Test the yfinance library connection.

    Fetches all four IDR pairs defined in ADR-001 using the real
    ``YFinanceExtractor`` (which wraps ``yf.Ticker``). No API key required —
    yfinance scrapes Yahoo Finance's public endpoints directly.
    """
    _print_header("1. yfinance (pip install yfinance)")

    try:
        import yfinance
        print(f"  yfinance version: {yfinance.__version__}")
    except ImportError:
        print("  [ERROR] 'yfinance' is not installed.")
        print("          Run: pip install yfinance --break-system-packages")
        print()
        return False

    extractor = YFinanceExtractor()
    pairs = extractor.SUPPORTED_PAIRS  # USD_IDR, EUR_IDR, SGD_IDR, JPY_IDR

    print(f"  Testing pairs: {pairs}")
    print()

    try:
        result = extractor.fetch_rates(pairs)
    except Exception as exc:  # noqa: BLE001
        print(f"  [ERROR] fetch_rates() raised an unexpected exception: {exc!r}")
        print()
        return False

    ok = _print_result(result)
    print()
    return ok


# ---------------------------------------------------------------------------
# FRED test
# ---------------------------------------------------------------------------

def test_fred() -> bool:
    """
    Test the FRED API connection using FRED_API_KEY from the environment.

    Per project decision, we use the raw REST endpoint via ``requests``
    (the "backdoor" API key approach) instead of the ``fredapi`` package,
    so this exercises the exact code path the pipeline will use.

    Only ``USD_IDR`` (FRED series ``DEXINUS``) is tested — FRED does not
    publish a direct EUR_IDR series (see extractors.py for details).
    """
    _print_header("2. FRED API (FRED_API_KEY)")

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("  [WARN] FRED_API_KEY is not set in the environment.")
        print("         Set it via .env or: export FRED_API_KEY=your_key_here")
        print("         Get a free key at: "
              "https://fred.stlouisfed.org/docs/api/api_key.html")
        print()
        return False

    masked = api_key[:4] + "..." + api_key[-2:] if len(api_key) > 6 else "****"
    print(f"  Using FRED_API_KEY: {masked}")

    try:
        extractor = FREDExtractor(api_key=api_key)
    except EnvironmentError as exc:
        print(f"  [ERROR] {exc}")
        print()
        return False

    pairs = extractor.SUPPORTED_PAIRS  # currently: ["USD_IDR"]
    print(f"  Testing pairs: {pairs}  (FRED series: CCUSMA02IDM618N, "
          f"frequency={extractor._frequency!r}, "
          f"aggregation_method={extractor._aggregation_method!r})")
    print("  Note: FRED = monthly/annual aggregates (project decision); "
          "yfinance = daily.")
    print()

    try:
        result = extractor.fetch_rates(pairs)
    except Exception as exc:  # noqa: BLE001
        print(f"  [ERROR] fetch_rates() raised an unexpected exception: {exc!r}")
        print()
        return False

    ok = _print_result(result)
    print()
    return ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test connectivity to yfinance and FRED API sources."
    )
    parser.add_argument(
        "--source",
        choices=["yfinance", "fred", "all"],
        default="all",
        help="Which source to test (default: all)",
    )
    args = parser.parse_args()

    results: dict[str, bool] = {}

    if args.source in ("yfinance", "all"):
        results["yfinance"] = test_yfinance()

    if args.source in ("fred", "all"):
        results["fred"] = test_fred()

    _print_header("Summary")
    for source, ok in results.items():
        status = "OK" if ok else "FAILED"
        print(f"  {source:10s} [{status}]")
    print()

    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())