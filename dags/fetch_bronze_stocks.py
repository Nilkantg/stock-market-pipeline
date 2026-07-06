"""
DAG: fetch_bronze_stocks

Daily batch job: fetch raw OHLCV data for a fixed list of symbols from
Yahoo Finance and land it, untouched, in GCS Bronze.

Schedule: once per day, after Indian market close (market closes 15:30 IST).
We run at 18:00 IST to give Yahoo Finance's data time to settle.

Architecture position: this is the FIRST DAG in the pipeline. It has no
upstream dependency. Phase 2's Silver DAG will depend on this one completing.
"""

import logging
import os
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models import Variable
import pendulum

logger = logging.getLogger(__name__)
IST = pendulum.timezone("Asia/Kolkata")

# --- Config -------------------------------------------------------------
# Symbols are hardcoded for now; Phase 5 moves this to an Airflow Variable
# so it's configurable without a code change.
DEFAULT_SYMBOLS = ["RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "WIPRO.NS"]

BUCKET_BRONZE = os.environ.get("GCS_BUCKET_BRONZE", "CHANGE_ME_BRONZE_BUCKET")

# Abstract "dataset" representing "Bronze stock data for today is ready".
# The Silver DAG schedules itself on this being updated, instead of cron.
BRONZE_STOCKS_DATASET = Dataset(f"gs://{BUCKET_BRONZE}/bronze/stocks/")

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    # Exponential-ish backoff: retry 1 waits 5 min, retry 2 waits ~10 min, etc.
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def alert_on_failure(context):
    """
    Basic failure callback. For now this just logs loudly to the task log
    so it's visible in the Airflow UI. Phase 4 will extend this to an
    email/Slack notification.
    """
    task_instance = context["task_instance"]
    logger.error(
        "TASK FAILED: dag=%s task=%s run_id=%s. Check logs above for root cause.",
        task_instance.dag_id,
        task_instance.task_id,
        context["run_id"],
    )


def _target_date_from_context(context) -> str:
    """
    Use the run's interval end in IST. For the 18:00 IST schedule this resolves
    to the Indian trading date that just closed, and manual runs stay aligned
    with the IST calendar date shown to the operator.
    """
    run_end = context.get("data_interval_end") or context["logical_date"]
    return run_end.in_timezone(IST).date().isoformat()


@dag(
    dag_id="fetch_bronze_stocks",
    description="Fetch daily OHLCV data for tracked symbols and land raw in GCS Bronze",
    schedule="0 18 * * *",  # 18:00 IST, after Indian market close
    start_date=pendulum.datetime(2026, 6, 1, tz=IST),
    catchup=False,  # don't backfill historical runs on first deploy
    default_args=default_args,
    on_failure_callback=alert_on_failure,
    tags=["bronze", "ingestion", "stocks"],
)
def fetch_bronze_stocks():

    @task
    def get_symbol_list() -> list[str]:
        """
        Read the symbol list. Uses an Airflow Variable if one is set
        (Admin -> Variables -> stock_symbols, comma-separated), otherwise
        falls back to DEFAULT_SYMBOLS. This gives us a config override
        without needing a code deploy.
        """
        raw = Variable.get("stock_symbols", default_var=None)
        if raw:
            symbols = [s.strip() for s in raw.split(",") if s.strip()]
            logger.info("Using symbols from Airflow Variable: %s", symbols)
            return symbols
        logger.info("No Airflow Variable set, using default symbols: %s", DEFAULT_SYMBOLS)
        return DEFAULT_SYMBOLS

    @task
    def fetch_data(symbols: list[str], **context) -> list[dict]:
        """
        Fetch OHLCV data for all symbols for this DAG run's logical date.
        Returned list is small (5 symbols worth of floats/ints) so passing
        it via XCom to the next task is fine.
        """
        # Late import: keeps DAG-parse time fast (Airflow parses this file
        # every few seconds; heavy imports at module level slow that down).
        from scripts.bronze.fetch_stocks import fetch_all_symbols

        target_date = _target_date_from_context(context)
        logger.info("Fetching %d symbols for %s", len(symbols), target_date)

        records = fetch_all_symbols(symbols, target_date)

        success_count = sum(1 for r in records if r["fetch_status"] == "success")
        no_data_count = sum(1 for r in records if r["fetch_status"] == "no_data")
        error_count = sum(1 for r in records if r["fetch_status"] == "error")
        logger.info(
            "Fetch summary: %d success, %d no_data, %d error (of %d total)",
            success_count, no_data_count, error_count, len(records),
        )

        # Fail the task (triggering retries) only if EVERY symbol errored —
        # a single bad symbol shouldn't block the whole batch, but if the
        # API is fully down, we want Airflow's retry/alerting to kick in.
        if error_count == len(records):
            raise RuntimeError(f"All {len(records)} symbols failed to fetch — likely an API-wide issue.")

        return records

    @task(outlets=[BRONZE_STOCKS_DATASET])
    def write_to_bronze(records: list[dict], **context) -> list[str]:
        """
        Write fetched records to GCS Bronze, one JSON file per symbol.
        Returns the list of gs:// URIs written (small, so fine for XCom;
        Phase 2's Silver DAG could either re-list the GCS prefix itself
        or, for a tighter coupling, read this XCom via a Dataset/trigger).
        """
        from scripts.bronze.gcs_writer import write_records_to_gcs

        target_date = _target_date_from_context(context)
        paths = write_records_to_gcs(records, BUCKET_BRONZE, target_date)
        logger.info("Wrote %d files to gs://%s/bronze/stocks/date=%s/", len(paths), BUCKET_BRONZE, target_date)
        return paths

    # --- Task dependency wiring (TaskFlow infers this from the call chain) ---
    symbols = get_symbol_list()
    records = fetch_data(symbols)
    write_to_bronze(records)


fetch_bronze_stocks()
