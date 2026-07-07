"""
DAG: process_silver_stocks

Reads raw Bronze JSON across the same rolling date window Bronze just wrote
(31 days by default: covers the one-time historical backfill plus every
day's incremental refresh), validates/cleans/types it, writes Parquet to
GCS Silver (one file per date), and loads each date into its own BigQuery
partition. A quality gate fails the DAG if too much of the window is bad.

Architecture position: SECOND DAG in the pipeline. Dataset-triggered off
fetch_bronze_stocks' write_to_bronze task -- runs automatically, no manual
trigger needed once both DAGs are unpaused.
"""

import logging
import os
from datetime import timedelta

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)
IST = pendulum.timezone("Asia/Kolkata")

BUCKET_BRONZE = os.environ.get("GCS_BUCKET_BRONZE", "CHANGE_ME_BRONZE_BUCKET")
BUCKET_SILVER = os.environ.get("GCS_BUCKET_SILVER", "CHANGE_ME_SILVER_BUCKET")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "CHANGE_ME_PROJECT")
BQ_DATASET_SILVER = os.environ.get("BQ_DATASET_SILVER", "silver_stocks")
BQ_TABLE = "daily_prices"
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("STOCK_HISTORY_LOOKBACK_DAYS", "31"))

# Must be byte-for-byte identical to the outlet Dataset in fetch_bronze_stocks.py
BRONZE_STOCKS_DATASET = Dataset(f"gs://{BUCKET_BRONZE}/bronze/stocks/")
SILVER_STOCKS_DATASET = Dataset(f"bq://{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.{BQ_TABLE}")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def alert_on_failure(context):
    ti = context["task_instance"]
    logger.error("TASK FAILED: dag=%s task=%s run_id=%s", ti.dag_id, ti.task_id, context["run_id"])


def _target_date_from_context(context) -> str:
    """Same IST-anchored logic as Bronze, so both DAGs always agree on 'today'."""
    run_end = context.get("data_interval_end") or context["logical_date"]
    return run_end.in_timezone(IST).date().isoformat()


def _date_window_from_context(context, lookback_days: int = DEFAULT_LOOKBACK_DAYS) -> tuple[str, str]:
    end_date = pendulum.parse(_target_date_from_context(context), tz=IST)
    start_date = end_date.subtract(days=lookback_days - 1)
    return start_date.date().isoformat(), end_date.date().isoformat()


@dag(
    dag_id="process_silver_stocks",
    description="Clean and validate Bronze stock data across the rolling window, load into Silver",
    schedule=[BRONZE_STOCKS_DATASET],
    start_date=pendulum.datetime(2026, 6, 1, tz=IST),
    catchup=False,
    default_args=default_args,
    on_failure_callback=alert_on_failure,
    tags=["silver", "transformation", "stocks"],
)
def process_silver_stocks():

    @task
    def read_bronze(**context) -> list[dict]:
        from scripts.silver.io_utils import read_bronze_records

        start_date, end_date = _date_window_from_context(context)
        records = read_bronze_records(BUCKET_BRONZE, start_date, end_date)
        logger.info("Read %d raw records for window %s to %s", len(records), start_date, end_date)
        return records

    @task
    def clean_and_validate(records: list[dict]) -> str:
        """
        Transforms the whole window at once. Returned as JSON for XCom --
        at 5 symbols x ~22 trading days this is a few hundred rows, still
        comfortably small for XCom. A much larger symbol universe would
        need an intermediate GCS write instead.
        """
        from scripts.silver.transform import transform_records

        df = transform_records(records)
        return df.to_json(orient="records")

    @task
    def quality_gate(df_json: str) -> str:
        import pandas as pd

        from scripts.silver.transform import run_quality_checks

        df = pd.read_json(df_json, orient="records")
        passed, message = run_quality_checks(df, min_valid_ratio=0.5)
        logger.info("Quality gate result: %s — %s", passed, message)

        if not passed:
            raise ValueError(f"Silver quality gate failed: {message}")

        return df_json

    @task(outlets=[SILVER_STOCKS_DATASET])
    def write_silver(df_json: str) -> dict:
        import pandas as pd

        from scripts.silver.io_utils import load_to_bigquery, write_parquet_to_gcs

        df = pd.read_json(df_json, orient="records")

        gcs_uris = write_parquet_to_gcs(df, BUCKET_SILVER)
        row_count = load_to_bigquery(df, GCP_PROJECT_ID, BQ_DATASET_SILVER, BQ_TABLE)

        return {"gcs_files_written": len(gcs_uris), "rows_loaded": row_count}

    raw = read_bronze()
    cleaned = clean_and_validate(raw)
    gated = quality_gate(cleaned)
    write_silver(gated)


process_silver_stocks()