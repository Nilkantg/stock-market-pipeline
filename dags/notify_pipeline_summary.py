"""
DAG: notify_pipeline_summary

Final stage of the pipeline. Dataset-triggered off all three Gold tables
being rebuilt. Queries row counts and quality stats across Bronze, Silver,
and Gold for today's run, and logs one consolidated summary.

This is intentionally simple (log-based) for the POC. A real production
setup would swap the final task's body for an email/Slack API call --
the task boundary is already in the right place to do that later without
restructuring anything else.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "CHANGE_ME_PROJECT")
BUCKET_BRONZE = os.environ.get("GCS_BUCKET_BRONZE", "CHANGE_ME_BRONZE_BUCKET")

# Trigger off the same Gold tables build_gold_stocks produces. We declare
# these as outlets on build_gold_stocks's three tasks (see 4.3 below) and
# schedule on all of them -- Airflow only fires this DAG once ALL listed
# datasets have been updated since the last run (AND semantics, not OR).
GOLD_DAILY_SUMMARY_DATASET = Dataset(f"bq://{GCP_PROJECT_ID}.gold_stocks.gold_daily_summary")
GOLD_MOVING_AVERAGES_DATASET = Dataset(f"bq://{GCP_PROJECT_ID}.gold_stocks.gold_moving_averages")
GOLD_SECTOR_SUMMARY_DATASET = Dataset(f"bq://{GCP_PROJECT_ID}.gold_stocks.gold_sector_summary")

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def alert_on_failure(context):
    ti = context["task_instance"]
    logger.error("TASK FAILED: dag=%s task=%s run_id=%s", ti.dag_id, ti.task_id, context["run_id"])


@dag(
    dag_id="notify_pipeline_summary",
    description="Summarize row counts and quality across Bronze/Silver/Gold for today's run",
    schedule=[GOLD_DAILY_SUMMARY_DATASET, GOLD_MOVING_AVERAGES_DATASET, GOLD_SECTOR_SUMMARY_DATASET],
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=alert_on_failure,
    tags=["notification", "monitoring"],
)
def notify_pipeline_summary():

    @task
    def count_bronze_files(**context) -> dict:
        from google.cloud import storage

        target_date = context["ds"]
        client = storage.Client()
        prefix = f"bronze/stocks/date={target_date}/"
        blobs = list(client.list_blobs(BUCKET_BRONZE, prefix=prefix))
        return {"layer": "bronze", "date": target_date, "file_count": len(blobs)}

    @task
    def count_silver_rows(**context) -> dict:
        from google.cloud import bigquery

        target_date = context["ds"]
        client = bigquery.Client(project=GCP_PROJECT_ID)
        query = f"""
            SELECT
              COUNT(*) AS total_rows,
              COUNTIF(is_valid) AS valid_rows,
              COUNTIF(NOT is_valid) AS invalid_rows
            FROM `{GCP_PROJECT_ID}.silver_stocks.daily_prices`
            WHERE data_date = '{target_date}'
        """
        result = list(client.query(query).result())[0]
        return {
            "layer": "silver",
            "date": target_date,
            "total_rows": result.total_rows,
            "valid_rows": result.valid_rows,
            "invalid_rows": result.invalid_rows,
        }

    @task
    def count_gold_rows(**context) -> dict:
        from google.cloud import bigquery

        target_date = context["ds"]
        client = bigquery.Client(project=GCP_PROJECT_ID)
        counts = {}
        for table in ["gold_daily_summary", "gold_moving_averages", "gold_sector_summary"]:
            query = f"""
                SELECT COUNT(*) AS row_count
                FROM `{GCP_PROJECT_ID}.gold_stocks.{table}`
                WHERE data_date = '{target_date}'
            """
            result = list(client.query(query).result())[0]
            counts[table] = result.row_count
        return {"layer": "gold", "date": target_date, "table_counts": counts}

    @task
    def log_summary(bronze: dict, silver: dict, gold: dict, **context) -> None:
        """
        Final consolidated log line. Swap this function body for an
        email/Slack call later -- inputs are already the exact summary
        data you'd want in that notification.
        """
        target_date = context["ds"]

        warnings = []
        if bronze["file_count"] < 5:
            warnings.append(f"Bronze only has {bronze['file_count']}/5 expected symbol files")
        if silver["invalid_rows"] > 0:
            warnings.append(f"Silver flagged {silver['invalid_rows']} invalid rows")
        for table, count in gold["table_counts"].items():
            if count == 0:
                warnings.append(f"Gold table {table} has 0 rows for {target_date}")

        logger.info("=" * 60)
        logger.info("PIPELINE RUN SUMMARY — %s", target_date)
        logger.info("=" * 60)
        logger.info("Bronze: %d files written", bronze["file_count"])
        logger.info(
            "Silver: %d total rows (%d valid, %d invalid)",
            silver["total_rows"], silver["valid_rows"], silver["invalid_rows"],
        )
        for table, count in gold["table_counts"].items():
            logger.info("Gold.%s: %d rows", table, count)

        if warnings:
            logger.warning("WARNINGS (%d):", len(warnings))
            for w in warnings:
                logger.warning("  - %s", w)
        else:
            logger.info("No warnings — full pipeline run clean.")
        logger.info("=" * 60)

    bronze_result = count_bronze_files()
    silver_result = count_silver_rows()
    gold_result = count_gold_rows()
    log_summary(bronze_result, silver_result, gold_result)


notify_pipeline_summary()