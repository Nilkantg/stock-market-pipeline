import pandas as pd

from scripts.bronze import fetch_stocks


def test_fetch_symbol_data_handles_yfinance_multiindex(monkeypatch):
    columns = pd.MultiIndex.from_product(
        [["Open", "High", "Low", "Close", "Adj Close", "Volume"], ["RELIANCE.NS"]]
    )
    hist = pd.DataFrame(
        [[2500.0, 2525.5, 2490.25, 2510.75, 2510.75, 1234567]],
        index=pd.to_datetime(["2026-07-06"]),
        columns=columns,
    )

    def fake_download(*args, **kwargs):
        return hist

    monkeypatch.setattr(fetch_stocks.yf, "download", fake_download)

    record = fetch_stocks.fetch_symbol_data("RELIANCE.NS", "2026-07-06")

    assert record["symbol"] == "RELIANCE.NS"
    assert record["date"] == "2026-07-06"
    assert record["fetch_status"] == "success"
    assert record["fetched_at"].endswith("+05:30")
    assert record["data"] == {
        "open": 2500.0,
        "high": 2525.5,
        "low": 2490.25,
        "close": 2510.75,
        "volume": 1234567,
        "dividends": 0.0,
        "stock_splits": 0.0,
    }
