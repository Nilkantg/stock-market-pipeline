"""
DAG: process_silver_stocks

Reads raw Bronze JSON for the day, validates/cleans/types it, writes
Parquet to GCS Silver, loads into BigQuery silver_stocks.daily_prices,
and runs a data-quality gate that fails the DAG if quality is too low.

Architecture position: SECOND DAG in the pipeline. Scheduled via Airflow
Dataset trigger (not cron) — it runs automatically whenever
fetch_bronze_stocks' write_to_bronze task completes successfully.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

BUCKET_BRONZE = os.environ.get("GCS_BUCKET_BRONZE", "CHANGE_ME_BRONZE_BUCKET")
BUCKET_SILVER = os.environ.get("GCS_BUCKET_SILVER", "CHANGE_ME_SILVER_BUCKET")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "CHANGE_ME_PROJECT")
BQ_DATASET_SILVER = os.environ.get("BQ_DATASET_SILVER", "silver_stocks")
BQ_TABLE = "daily_prices"

# Must match the Dataset URI declared as an outlet in fetch_bronze_stocks.py —
# Airflow matches Datasets by exact URI string, so this string must be identical
# to BRONZE_STOCKS_DATASET in that file.
BRONZE_STOCKS_DATASET = Dataset(f"gs://{BUCKET_BRONZE}/bronze/stocks/")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def alert_on_failure(context):
    ti = context["task_instance"]
    logger.error("TASK FAILED: dag=%s task=%s run_id=%s", ti.dag_id, ti.task_id, context["run_id"])


@dag(
    dag_id="process_silver_stocks",
    description="Clean and validate Bronze stock data, load into Silver (GCS Parquet + BigQuery)",
    schedule=[BRONZE_STOCKS_DATASET],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=alert_on_failure,
    tags=["silver", "transformation", "stocks"],
)
def process_silver_stocks():

    @task
    def read_bronze(**context) -> list[dict]:
        from scripts.silver.io_utils import read_bronze_records

        target_date = context["ds"]
        records = read_bronze_records(BUCKET_BRONZE, target_date)
        logger.info("Read %d raw records for %s", len(records), target_date)
        return records

    @task
    def clean_and_validate(records: list[dict]) -> str:
        """
        Returns the transformed DataFrame serialized to JSON (records
        orientation) for XCom — small enough (5 symbols/day) that this is
        fine. For a much larger symbol universe, write an intermediate
        Parquet to GCS instead of passing the DataFrame through XCom.
        """
        from scripts.silver.transform import transform_records

        df = transform_records(records)
        return df.to_json(orient="records")

    @task
    def quality_gate(df_json: str) -> str:
        """
        Fails the DAG (raises) if quality checks don't pass. Kept as its
        own task so the Airflow UI shows quality failures distinctly from
        transform-code failures.
        """
        import pandas as pd

        from scripts.silver.transform import run_quality_checks

        df = pd.read_json(df_json, orient="records")
        passed, message = run_quality_checks(df, min_valid_ratio=0.5)
        logger.info("Quality gate result: %s — %s", passed, message)

        if not passed:
            raise ValueError(f"Silver quality gate failed: {message}")

        return df_json

    @task(outlets=[Dataset(f"bq://{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.{BQ_TABLE}")])
    def write_silver(df_json: str, **context) -> dict:
        import pandas as pd

        from scripts.silver.io_utils import load_to_bigquery, write_parquet_to_gcs

        target_date = context["ds"]
        df = pd.read_json(df_json, orient="records")

        gcs_uri = write_parquet_to_gcs(df, BUCKET_SILVER, target_date)
        row_count = load_to_bigquery(df, GCP_PROJECT_ID, BQ_DATASET_SILVER, BQ_TABLE, target_date)

        return {"gcs_uri": gcs_uri, "rows_loaded": row_count, "date": target_date}

    # --- Task chain ---
    raw = read_bronze()
    cleaned = clean_and_validate(raw)
    gated = quality_gate(cleaned)
    write_silver(gated)


process_silver_stocks()