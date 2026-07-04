"""
IO utilities for the Silver layer:
  - read raw JSON records from GCS Bronze for a given date
  - write the transformed DataFrame as Parquet to GCS Silver
  - load the DataFrame into BigQuery silver_stocks.daily_prices
"""

import io
import json
import logging

import pandas as pd
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)


def read_bronze_records(bucket_name: str, target_date: str) -> list[dict]:
    """
    List and read all JSON files under bronze/stocks/date=<target_date>/
    in the given bucket, returning them as a list of dicts.
    """
    client = storage.Client()
    prefix = f"bronze/stocks/date={target_date}/"
    blobs = list(client.list_blobs(bucket_name, prefix=prefix))

    if not blobs:
        raise FileNotFoundError(
            f"No Bronze files found at gs://{bucket_name}/{prefix} — "
            f"did the Bronze DAG run for this date?"
        )

    records = []
    for blob in blobs:
        content = blob.download_as_text()
        records.append(json.loads(content))

    logger.info("Read %d Bronze files from gs://%s/%s", len(records), bucket_name, prefix)
    return records


def write_parquet_to_gcs(df: pd.DataFrame, bucket_name: str, target_date: str) -> str:
    """
    Write the Silver DataFrame as a single Parquet file to
    gs://<bucket>/silver/stocks/date=<target_date>/stocks.parquet
    """
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_path = f"silver/stocks/date={target_date}/stocks.parquet"
    blob = bucket.blob(blob_path)
    blob.upload_from_file(buffer, content_type="application/octet-stream")

    gs_uri = f"gs://{bucket_name}/{blob_path}"
    logger.info("Wrote Parquet (%d rows) to %s", len(df), gs_uri)
    return gs_uri


def load_to_bigquery(df: pd.DataFrame, project_id: str, dataset: str, table: str, target_date: str) -> int:
    """
    Load the DataFrame into BigQuery, replacing only the partition for
    target_date (not the whole table). Uses a partitioned table on
    `data_date` so daily runs never touch other days' data.

    Returns the number of rows loaded.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset}.{table}"

    # WRITE_TRUNCATE with a partition decorator ($YYYYMMDD) replaces only
    # that day's partition — safe to re-run the DAG for the same date
    # without duplicating rows.
    partition_suffix = target_date.replace("-", "")
    destination = f"{table_ref}${partition_suffix}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(field="data_date"),
        schema=[
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
        ],
    )

    # BigQuery client wants native pandas dtypes, not pandas' nullable
    # extension types (Int64/string) for load_table_from_dataframe in
    # some versions — cast defensively.
    load_df = df.copy()
    load_df["data_date"] = pd.to_datetime(load_df["data_date"]).dt.date

    job = client.load_table_from_dataframe(load_df, destination, job_config=job_config)
    job.result()  # blocks until load completes, raises on failure

    logger.info("Loaded %d rows into %s", len(load_df), destination)
    return len(load_df)