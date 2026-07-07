"""
Writes Bronze records to GCS as JSON, one file per symbol per date, under:

  gs://<bucket>/bronze/stocks/date=YYYY-MM-DD/<symbol>.json

One file per symbol (not one big file for the whole batch) so that a partial
re-run or a single-symbol backfill only touches the relevant file, and so
Silver can process files independently / in parallel later.
"""

import json
import logging

from google.cloud import storage

logger = logging.getLogger(__name__)


def write_records_to_gcs(records: list[dict], bucket_name: str, target_date: str | None = None) -> list[str]:
    """
    Write each record as its own JSON file to GCS Bronze.

    Args:
        records: list of dicts from fetch_all_symbols()
        bucket_name: GCS bucket name (no gs:// prefix)
        target_date: Optional "YYYY-MM-DD" fallback partition. When omitted,
        each record's own "date" is used so one run can write many partitions.

    Returns:
        List of gs:// URIs written, for logging / XCom.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    written_paths = []
    for record in records:
        symbol = record["symbol"]
        partition_date = record.get("date") or target_date
        if not partition_date:
            raise ValueError(f"Record for {symbol} has no date and no target_date fallback was provided")

        blob_path = f"bronze/stocks/date={partition_date}/{symbol}.json"
        blob = bucket.blob(blob_path)

        blob.upload_from_string(
            data=json.dumps(record, indent=2),
            content_type="application/json",
        )

        gs_uri = f"gs://{bucket_name}/{blob_path}"
        written_paths.append(gs_uri)
        logger.info("Wrote %s (status=%s)", gs_uri, record["fetch_status"])

    return written_paths
