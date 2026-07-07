"""
Bronze layer fetch utility.

Pulls raw daily OHLCV data from Alpha Vantage.
No cleaning, no type casting beyond what's needed to make it JSON-serializable.
This is intentionally "dumb" — Bronze stores exactly what the source gave us.
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)
IST = timezone(timedelta(hours=5, minutes=30), name="IST")
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
TIME_SERIES_KEY = "Time Series (Daily)"

# Alpha Vantage supports Indian equities through BSE-style suffixes. Keep the
# pipeline's public symbols unchanged so existing Silver/Gold joins still work.
PROVIDER_SYMBOL_OVERRIDES = {
    "RELIANCE.NS": "RELIANCE.BSE",
    "TCS.NS": "TCS.BSE",
    "INFY.NS": "INFY.BSE",
    "HDFCBANK.NS": "HDFCBANK.BSE",
    "WIPRO.NS": "WIPRO.BSE",
}


class FetchError(Exception):
    """Raised when a symbol's data cannot be fetched after retries."""
    pass


def _now_ist() -> str:
    return datetime.now(IST).isoformat()


def _provider_symbol(symbol: str) -> str:
    return PROVIDER_SYMBOL_OVERRIDES.get(symbol, symbol)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _empty_record(symbol: str, target_date: str, status: str, message: str | None = None) -> dict:
    record = {
        "symbol": symbol,
        "date": target_date,
        "fetch_status": status,
        "fetched_at": _now_ist(),
        "source": "alpha_vantage",
        "data": None,
    }
    if message:
        record["error_message"] = message
    return record


def _daily_record(symbol: str, target_date: str, row: dict[str, str]) -> dict:
    return {
        "symbol": symbol,
        "date": target_date,
        "fetch_status": "success",
        "fetched_at": _now_ist(),
        "source": "alpha_vantage",
        "data": {
            "open": float(row["1. open"]),
            "high": float(row["2. high"]),
            "low": float(row["3. low"]),
            "close": float(row["4. close"]),
            "volume": int(row["5. volume"]),
            "dividends": 0.0,
            "stock_splits": 0.0,
        },
    }


def _request_daily_series(symbol: str, api_key: str) -> dict[str, Any]:
    provider_symbol = _provider_symbol(symbol)
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": provider_symbol,
        "outputsize": "compact",
        "apikey": api_key,
    }

    response = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if TIME_SERIES_KEY in payload:
        return payload

    message = payload.get("Note") or payload.get("Information") or payload.get("Error Message")
    if message:
        raise FetchError(f"Alpha Vantage returned no daily data for {symbol} ({provider_symbol}): {message}")

    raise FetchError(f"Alpha Vantage response for {symbol} did not include '{TIME_SERIES_KEY}'")


def fetch_symbol_history(symbol: str, start_date: str, end_date: str, api_key: str) -> list[dict]:
    """
    Fetch daily OHLCV records for one symbol over an inclusive date range.

    Alpha Vantage compact daily output returns the latest 100 trading days on
    free keys, which is enough for this pipeline's rolling 31-day backfill.
    """
    try:
        payload = _request_daily_series(symbol, api_key)
        series = payload[TIME_SERIES_KEY]
        start = _parse_date(start_date)
        end = _parse_date(end_date)

        records = []
        for day, row in sorted(series.items()):
            trading_day = _parse_date(day)
            if start <= trading_day <= end:
                records.append(_daily_record(symbol, day, row))

        if not records:
            logger.warning("No Alpha Vantage rows returned for %s between %s and %s", symbol, start_date, end_date)
            return [_empty_record(symbol, end_date, "no_data", "No provider rows in requested date window")]

        return records

    except FetchError:
        raise
    except Exception as exc:
        logger.error("Failed to fetch %s from Alpha Vantage: %s", symbol, exc)
        raise FetchError(f"Failed to fetch {symbol} from Alpha Vantage: {exc}") from exc


def fetch_all_symbols(symbols: list[str], target_date: str, api_key: str) -> list[dict]:
    """Compatibility wrapper for one-day fetches."""
    return fetch_symbols_for_range(symbols, target_date, target_date, api_key)


def fetch_symbols_for_range(symbols: list[str], start_date: str, end_date: str, api_key: str) -> list[dict]:
    """
    Fetch data for all symbols over an inclusive date range. Does NOT raise on
    individual symbol failure, so one bad symbol does not kill the whole batch.
    """
    results = []
    for symbol in symbols:
        try:
            results.extend(fetch_symbol_history(symbol, start_date, end_date, api_key))
        except FetchError as exc:
            results.append(_empty_record(symbol, end_date, "error", str(exc)))
    return results
