"""
Bronze layer fetch utility.

Pulls raw daily OHLCV data for a single symbol from Yahoo Finance via yfinance.
No cleaning, no type casting beyond what's needed to make it JSON-serializable.
This is intentionally "dumb" — Bronze stores exactly what the source gave us.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import yfinance as yf

logger = logging.getLogger(__name__)


class FetchError(Exception):
    """Raised when a symbol's data cannot be fetched after retries."""
    pass


def fetch_symbol_data(symbol: str, target_date: str) -> dict:
    """
    Fetch one day of OHLCV data for a single symbol.

    Args:
        symbol: Ticker symbol, e.g. "RELIANCE.NS"
        target_date: Date string "YYYY-MM-DD" — the trading day we want data for.

    Returns:
        A dict with the raw fields we'll persist to Bronze. Includes a
        "fetch_status" field so downstream tasks/DAG logic can tell success
        from failure without relying on exceptions crossing task boundaries.

    Raises:
        FetchError: if the API call itself fails (network, symbol not found, etc.)
    """
    try:
        ticker = yf.Ticker(symbol)

        # yfinance's `history` end date is exclusive, so we ask for a 1-day window
        start = target_date
        end_dt = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=1)
        end = end_dt.strftime("%Y-%m-%d")

        hist = ticker.history(start=start, end=end, interval="1d")

        if hist.empty:
            # Not an exception — a real, expected outcome (market holiday, delisted
            # symbol, weekend). We record it rather than raise, so Bronze still
            # gets a file documenting "we tried, nothing came back."
            logger.warning("No data returned for %s on %s (holiday/weekend/bad symbol?)", symbol, target_date)
            return {
                "symbol": symbol,
                "date": target_date,
                "fetch_status": "no_data",
                "fetched_at": datetime.utcnow().isoformat(),
                "data": None,
            }

        row = hist.iloc[0]
        raw_record = {
            "symbol": symbol,
            "date": target_date,
            "fetch_status": "success",
            "fetched_at": datetime.utcnow().isoformat(),
            "data": {
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(row["Volume"]),
                # Dividends/Splits are yfinance extras — keep them, Bronze keeps everything
                "dividends": float(row.get("Dividends", 0.0)),
                "stock_splits": float(row.get("Stock Splits", 0.0)),
            },
        }
        return raw_record

    except Exception as exc:
        logger.error("Failed to fetch %s for %s: %s", symbol, target_date, exc)
        raise FetchError(f"Failed to fetch {symbol} for {target_date}: {exc}") from exc


def fetch_all_symbols(symbols: list[str], target_date: str) -> list[dict]:
    """
    Fetch data for a list of symbols. Does NOT raise on individual symbol
    failure — collects per-symbol results so one bad symbol doesn't kill
    the whole batch. The DAG task decides what to do with failures.
    """
    results = []
    for symbol in symbols:
        try:
            record = fetch_symbol_data(symbol, target_date)
            results.append(record)
        except FetchError as e:
            results.append({
                "symbol": symbol,
                "date": target_date,
                "fetch_status": "error",
                "fetched_at": datetime.utcnow().isoformat(),
                "data": None,
                "error_message": str(e),
            })
    return results