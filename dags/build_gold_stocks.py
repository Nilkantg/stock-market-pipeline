"""
DAG: build_gold_stocks

Builds the three Gold analytics tables from Silver data using SQL executed
directly in BigQuery via BigQueryInsertJobOperator:
  - gold_daily_summary   (OHLCV + daily return % + volume change %)
  - gold_moving_averages (7-day / 30-day close price moving averages)
  - gold_sector_summary  (sector-level average daily return)

Architecture position: THIRD DAG in the pipeline. Dataset-triggered off
process_silver_stocks' write_silver task.

Dependency shape:
    gold_daily_summary --> gold_sector_summary   (sector summary reads from daily_summary)
    gold_moving_averages                          (independent, runs in parallel)
"""

import logging
import os
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

logger = logging.getLogger(__name__)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "CHANGE_ME_PROJECT")
BUCKET_SILVER = os.environ.get("GCS_BUCKET_SILVER", "CHANGE_ME_SILVER_BUCKET")

# Must exactly match the outlet Dataset declared on write_silver in
# process_silver_stocks.py. We use the Silver BigQuery table as the dataset
# marker here (a string URI is all Airflow needs — it doesn't have to be
# a real GCS path).
SILVER_STOCKS_DATASET = Dataset(f"bq://{GCP_PROJECT_ID}.silver_stocks.daily_prices")

SQL_DIR = "/opt/airflow/sql"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def alert_on_failure(context):
    ti = context["task_instance"]
    logger.error("TASK FAILED: dag=%s task=%s run_id=%s", ti.dag_id, ti.task_id, context["run_id"])


def _read_sql(filename: str) -> str:
    """
    Read a .sql file from the mounted sql/ folder and substitute
    {project_id}. Keeping SQL in separate files (not inline strings in the
    DAG) means the queries can be run/tested manually in the BigQuery
    console with the same file used by the DAG.
    """
    path = os.path.join(SQL_DIR, filename)
    with open(path, "r") as f:
        template = f.read()
    return template.format(project_id=GCP_PROJECT_ID)


with dag(
    dag_id="build_gold_stocks",
    description="Build Gold analytics tables from Silver data via BigQuery SQL",
    schedule=[SILVER_STOCKS_DATASET],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=alert_on_failure,
    tags=["gold", "aggregation", "stocks"],
) as dag_obj:

    build_daily_summary = BigQueryInsertJobOperator(
        task_id="build_gold_daily_summary",
        configuration={
            "query": {
                "query": _read_sql("gold_daily_summary.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
        location=os.environ.get("GCP_REGION", "asia-south1"),
    )

    build_moving_averages = BigQueryInsertJobOperator(
        task_id="build_gold_moving_averages",
        configuration={
            "query": {
                "query": _read_sql("gold_moving_averages.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
        location=os.environ.get("GCP_REGION", "asia-south1"),
    )

    build_sector_summary = BigQueryInsertJobOperator(
        task_id="build_gold_sector_summary",
        configuration={
            "query": {
                "query": _read_sql("gold_sector_summary.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
        location=os.environ.get("GCP_REGION", "asia-south1"),
    )

    # gold_sector_summary reads FROM gold_daily_summary, so it must wait.
    # gold_moving_averages is independent -> no dependency edge, runs in parallel.
    build_daily_summary >> build_sector_summary