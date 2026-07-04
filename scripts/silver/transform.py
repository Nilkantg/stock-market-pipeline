"""
Silver layer transformation.

Takes raw Bronze records (as produced by scripts/bronze/fetch_stocks.py) and
produces a clean, typed, validated pandas DataFrame ready for Parquet + BigQuery.

Validation philosophy: we don't silently drop bad records. Every input record
becomes exactly one output row, either:
  - a clean row with is_valid=True, or
  - a flagged row with is_valid=False and a reason in validation_errors,
    with numeric fields set to None rather than a fabricated 0.

This way Silver's row count always matches Bronze's row count for a given
day/symbol set — nothing silently vanishes, which makes reconciliation between
layers trivial (a key thing analysts/interviewers ask about in Medallion setups).
"""

import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_DATA_FIELDS = ["open", "high", "low", "close", "volume"]


def _validate_record(record: dict) -> tuple[bool, list[str]]:
    """
    Run validation checks on a single raw Bronze record.

    Returns:
        (is_valid, list_of_error_reasons)
    """
    errors = []

    fetch_status = record.get("fetch_status")
    if fetch_status != "success":
        errors.append(f"fetch_status was '{fetch_status}', not 'success'")
        return False, errors

    data = record.get("data")
    if not data:
        errors.append("data field missing or null despite success status")
        return False, errors

    for field in REQUIRED_DATA_FIELDS:
        if field not in data or data[field] is None:
            errors.append(f"missing required field: {field}")

    if errors:
        return False, errors

    # Business-logic sanity checks — these catch corrupt/nonsensical values
    # that pass basic type checks but are clearly wrong.
    o, h, l, c, v = data["open"], data["high"], data["low"], data["close"], data["volume"]

    if any(x < 0 for x in [o, h, l, c]):
        errors.append("negative price detected")
    if v < 0:
        errors.append("negative volume detected")
    if h < l:
        errors.append(f"high ({h}) is less than low ({l})")
    if not (l <= o <= h) or not (l <= c <= h):
        # open/close should fall within the day's high/low range
        errors.append(f"open/close outside high-low range (o={o}, c={c}, h={h}, l={l})")

    return (len(errors) == 0), errors


def transform_records(records: list[dict], source: str = "yfinance") -> pd.DataFrame:
    """
    Transform a list of raw Bronze records into a clean Silver DataFrame.

    Adds standardized metadata columns:
      - processed_at: when Silver processing ran (UTC ISO timestamp)
      - source: where the raw data came from
      - data_date: the trading date this record represents
      - is_valid: whether the record passed validation
      - validation_errors: pipe-separated list of reasons if invalid, else None
    """
    processed_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for record in records:
        is_valid, errors = _validate_record(record)
        data = record.get("data") or {}

        row = {
            "symbol": record.get("symbol"),
            "data_date": record.get("date"),
            "open": float(data["open"]) if is_valid else None,
            "high": float(data["high"]) if is_valid else None,
            "low": float(data["low"]) if is_valid else None,
            "close": float(data["close"]) if is_valid else None,
            "volume": int(data["volume"]) if is_valid else None,
            "dividends": float(data.get("dividends", 0.0)) if is_valid else None,
            "stock_splits": float(data.get("stock_splits", 0.0)) if is_valid else None,
            "is_valid": is_valid,
            "validation_errors": " | ".join(errors) if errors else None,
            "source": source,
            "processed_at": processed_at,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Explicit dtypes — don't let pandas guess. This matters because a column
    # of all-None (e.g. every row invalid) would otherwise infer as object/float
    # inconsistently, which breaks Parquet schema stability across daily runs.
    dtype_map = {
        "symbol": "string",
        "data_date": "string",
        "open": "float64",
        "high": "float64",
        "low": "float64",
        "close": "float64",
        "volume": "Int64",  # nullable integer type (capital I) — allows NaN
        "dividends": "float64",
        "stock_splits": "float64",
        "is_valid": "bool",
        "validation_errors": "string",
        "source": "string",
        "processed_at": "string",
    }
    df = df.astype(dtype_map)

    valid_count = df["is_valid"].sum()
    logger.info("Transformed %d records: %d valid, %d flagged invalid", len(df), valid_count, len(df) - valid_count)

    return df


def run_quality_checks(df: pd.DataFrame, min_valid_ratio: float = 0.5) -> tuple[bool, str]:
    """
    Critical data-quality gate. If this fails, the calling DAG task should
    raise and fail the DAG run — we don't want to silently load a Silver
    table where most of the day's data is garbage.

    Checks:
      1. DataFrame isn't empty
      2. At least `min_valid_ratio` of rows are valid
      3. No duplicate (symbol, data_date) pairs
    """
    if df.empty:
        return False, "DataFrame is empty — no records to load"

    valid_ratio = df["is_valid"].sum() / len(df)
    if valid_ratio < min_valid_ratio:
        return False, f"Only {valid_ratio:.0%} of records valid, below threshold {min_valid_ratio:.0%}"

    dupes = df.duplicated(subset=["symbol", "data_date"]).sum()
    if dupes > 0:
        return False, f"Found {dupes} duplicate (symbol, data_date) pairs"

    return True, "All quality checks passed"