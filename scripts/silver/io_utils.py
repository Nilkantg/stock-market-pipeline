"""
IO utilities for the Silver layer:
  - read raw JSON records from GCS Bronze across a date RANGE (supports the
    31-day rolling backfill + incremental window Bronze now writes each run)
  - write the transformed DataFrame as Parquet to GCS Silver, one file per
    data_date (mirrors Bronze's per-date partitioning)
  - load each date's slice into its own BigQuery partition
"""

import io
import json
import logging
from datetime import date, datetime

import pandas as pd
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def read_bronze_records(bucket_name: str, start_date: str, end_date: str) -> list[dict]:
    """
    List and read all JSON files under bronze/stocks/date=<d>/ for every
    date d in [start_date, end_date] (inclusive), across the whole bucket.

    We list the whole "bronze/stocks/" prefix once (cheap — a few hundred
    small objects at most for this project's scale) and filter client-side
    by parsing the "date=YYYY-MM-DD" segment out of each blob path, rather
    than issuing one list_blobs call per date.
    """
    client = storage.Client()
    prefix = "bronze/stocks/"
    all_blobs = list(client.list_blobs(bucket_name, prefix=prefix))

    if not all_blobs:
        raise FileNotFoundError(
            f"No Bronze files found at all under gs://{bucket_name}/{prefix} — "
            f"did the Bronze DAG run yet?"
        )

    start = _parse_date(start_date)
    end = _parse_date(end_date)

    matched_blobs = []
    for blob in all_blobs:
        # path looks like: bronze/stocks/date=2026-06-15/RELIANCE.NS.json
        parts = blob.name.split("/")
        date_segment = next((p for p in parts if p.startswith("date=")), None)
        if not date_segment:
            continue
        try:
            blob_date = _parse_date(date_segment.replace("date=", ""))
        except ValueError:
            continue
        if start <= blob_date <= end:
            matched_blobs.append(blob)

    if not matched_blobs:
        raise FileNotFoundError(
            f"No Bronze files found in gs://{bucket_name}/{prefix} between "
            f"{start_date} and {end_date} — did the Bronze DAG run for this window?"
        )

    records = []
    for blob in matched_blobs:
        content = blob.download_as_text()
        records.append(json.loads(content))

    logger.info(
        "Read %d Bronze files from gs://%s/%s spanning %s to %s",
        len(records), bucket_name, prefix, start_date, end_date,
    )
    return records


def write_parquet_to_gcs(df: pd.DataFrame, bucket_name: str) -> list[str]:
    """
    Write one Parquet file per distinct data_date in the DataFrame, to
    gs://<bucket>/silver/stocks/date=<data_date>/stocks.parquet

    Splitting by date (instead of one file for the whole window) keeps
    Silver's partitioning scheme identical to Bronze's, and means a
    single day's file can be reprocessed/overwritten independently.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    written_paths = []

    for data_date, day_df in df.groupby("data_date"):
        buffer = io.BytesIO()
        day_df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        blob_path = f"silver/stocks/date={data_date}/stocks.parquet"
        blob = bucket.blob(blob_path)
        blob.upload_from_file(buffer, content_type="application/octet-stream")

        gs_uri = f"gs://{bucket_name}/{blob_path}"
        written_paths.append(gs_uri)
        logger.info("Wrote Parquet (%d rows) to %s", len(day_df), gs_uri)

    return written_paths


def load_to_bigquery(df: pd.DataFrame, project_id: str, dataset: str, table: str) -> int:
    """
    Load the DataFrame into BigQuery, one partition load per distinct
    data_date present. WRITE_TRUNCATE + a partition decorator ($YYYYMMDD)
    replaces only that day's partition each time — safe to re-run for the
    same date (or overlapping backfill window) without duplicating rows.

    Returns total rows loaded across all dates.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"

    schema = [
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("data_date", "DATE"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "INTEGER"),
        bigquery.SchemaField("dividends", "FLOAT"),
        bigquery.SchemaField("stock_splits", "FLOAT"),
        bigquery.SchemaField("is_valid", "BOOLEAN"),
        bigquery.SchemaField("validation_errors", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("processed_at", "TIMESTAMP"),
    ]

    total_rows = 0
    df = df.copy()
    df["data_date"] = pd.to_datetime(df["data_date"]).dt.date

    for data_date, day_df in df.groupby("data_date"):
        partition_suffix = data_date.strftime("%Y%m%d")
        destination = f"{table_ref}${partition_suffix}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(field="data_date"),
            schema=schema,
        )

        job = client.load_table_from_dataframe(day_df, destination, job_config=job_config)
        job.result()  # blocks until load completes, raises on failure

        logger.info("Loaded %d rows into %s", len(day_df), destination)
        total_rows += len(day_df)

    return total_rows