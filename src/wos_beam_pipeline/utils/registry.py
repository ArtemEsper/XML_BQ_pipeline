"""Registry utilities for hash-based idempotent pipeline processing.

Provides functions to load and update the wos_record_registry and
wos_file_registry BigQuery tables, and to clean up stale rows for
changed records across the 46 WoS content tables.
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


logger = logging.getLogger(__name__)

_RECORD_REGISTRY_TABLE = 'wos_record_registry'
_FILE_REGISTRY_TABLE = 'wos_file_registry'


# ---------------------------------------------------------------------------
# Record registry
# ---------------------------------------------------------------------------

def load_record_registry(bq_dataset: str, project: str) -> Dict[str, str]:
    """Load the uid->hash mapping from wos_record_registry.

    Args:
        bq_dataset: BigQuery dataset in 'project:dataset' or 'dataset' form.
        project: GCP project ID.

    Returns:
        Dict mapping uid (str) -> record_hash (str).
        Returns empty dict if the table does not exist yet.
    """
    client = bigquery.Client(project=project)
    dataset_id = _parse_dataset_id(bq_dataset)
    table_ref = f"{project}.{dataset_id}.{_RECORD_REGISTRY_TABLE}"

    try:
        client.get_table(table_ref)  # raises NotFound if absent
    except NotFound:
        logger.info(
            f"Record registry table {table_ref} does not exist yet — "
            "treating all records as new."
        )
        return {}

    logger.info(f"Loading record registry from {table_ref}")
    query = f"SELECT uid, record_hash FROM `{table_ref}`"
    results = client.query(query).result()
    registry = {row.uid: row.record_hash for row in results}
    logger.info(f"Loaded {len(registry):,} entries from record registry")
    return registry


def update_record_registry(
    new_records: Dict[str, str],
    changed_records: Dict[str, str],
    bq_dataset: str,
    project: str,
    source_file: str,
    ingestion_ts: str
) -> None:
    """Insert new rows and update hashes for changed rows in wos_record_registry.

    Args:
        new_records: Dict mapping uid -> record_hash for brand-new records.
        changed_records: Dict mapping uid -> record_hash for updated records.
        bq_dataset: BigQuery dataset identifier.
        project: GCP project ID.
        source_file: Source GCS file path (stored in registry).
        ingestion_ts: ISO-format UTC timestamp string for this pipeline run.
    """
    client = bigquery.Client(project=project)
    dataset_id = _parse_dataset_id(bq_dataset)
    table_ref = f"{project}.{dataset_id}.{_RECORD_REGISTRY_TABLE}"

    ingestion_dt = datetime.fromisoformat(ingestion_ts)

    # --- Insert new records via streaming insert ---
    if new_records:
        rows_to_insert = [
            {
                'uid': uid,
                'record_hash': record_hash,
                'source_file': source_file,
                'ingested_at': ingestion_dt.isoformat()
            }
            for uid, record_hash in new_records.items()
        ]
        errors = client.insert_rows_json(table_ref, rows_to_insert)
        if errors:
            logger.error(f"Errors inserting new registry rows: {errors}")
        else:
            logger.info(f"Inserted {len(new_records):,} new registry entries")

    # --- Update changed records via DML ---
    if changed_records:
        uids = list(changed_records.keys())
        hashes = list(changed_records.values())

        # Build a VALUES list for the MERGE source
        value_rows = ', '.join(
            f"('{uid}', '{changed_records[uid]}', '{source_file}', '{ingestion_dt.isoformat()}')"
            for uid in uids
        )
        merge_sql = f"""
            MERGE `{table_ref}` T
            USING (
                SELECT uid, record_hash, source_file, ingested_at
                FROM UNNEST([
                    STRUCT<uid STRING, record_hash STRING, source_file STRING, ingested_at TIMESTAMP>
                    {value_rows}
                ])
            ) S
            ON T.uid = S.uid
            WHEN MATCHED THEN UPDATE SET
                record_hash = S.record_hash,
                source_file = S.source_file,
                ingested_at = S.ingested_at
        """
        client.query(merge_sql).result()
        logger.info(f"Updated {len(changed_records):,} changed registry entries")


# ---------------------------------------------------------------------------
# File registry
# ---------------------------------------------------------------------------

def check_file_registry(
    file_path: str,
    file_md5: str,
    bq_dataset: str,
    project: str
) -> bool:
    """Check whether a file has already been processed with the same MD5.

    Args:
        file_path: GCS path of the input file.
        file_md5: MD5 hash of the file (from GCS object metadata).
        bq_dataset: BigQuery dataset identifier.
        project: GCP project ID.

    Returns:
        True if the file was previously processed with the same MD5 (skip it).
        False if it is new or has changed.
    """
    client = bigquery.Client(project=project)
    dataset_id = _parse_dataset_id(bq_dataset)
    table_ref = f"{project}.{dataset_id}.{_FILE_REGISTRY_TABLE}"

    try:
        client.get_table(table_ref)
    except NotFound:
        logger.info("File registry table not found — treating file as new.")
        return False

    query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{table_ref}`
        WHERE file_path = @file_path
          AND file_md5  = @file_md5
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('file_path', 'STRING', file_path),
            bigquery.ScalarQueryParameter('file_md5', 'STRING', file_md5),
        ]
    )
    results = client.query(query, job_config=job_config).result()
    count = next(iter(results)).cnt
    if count > 0:
        logger.info(f"File already processed (same MD5): {file_path}")
        return True
    return False


def register_file(
    file_path: str,
    file_md5: str,
    record_count: int,
    bq_dataset: str,
    project: str,
    ingestion_ts: str
) -> None:
    """Record a processed file in wos_file_registry.

    If an entry for the file already exists (different MD5 = updated file),
    the old row is replaced via MERGE.

    Args:
        file_path: GCS path of the input file.
        file_md5: MD5 hash of the file.
        record_count: Number of records processed from this file.
        bq_dataset: BigQuery dataset identifier.
        project: GCP project ID.
        ingestion_ts: ISO-format UTC timestamp string.
    """
    client = bigquery.Client(project=project)
    dataset_id = _parse_dataset_id(bq_dataset)
    table_ref = f"{project}.{dataset_id}.{_FILE_REGISTRY_TABLE}"
    ingestion_dt = datetime.fromisoformat(ingestion_ts)

    merge_sql = f"""
        MERGE `{table_ref}` T
        USING (
            SELECT
                @file_path  AS file_path,
                @file_md5   AS file_md5,
                @processed_at AS processed_at,
                @record_count AS record_count
        ) S
        ON T.file_path = S.file_path
        WHEN MATCHED THEN UPDATE SET
            file_md5     = S.file_md5,
            processed_at = S.processed_at,
            record_count = S.record_count
        WHEN NOT MATCHED THEN INSERT
            (file_path, file_md5, processed_at, record_count)
        VALUES
            (S.file_path, S.file_md5, S.processed_at, S.record_count)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('file_path', 'STRING', file_path),
            bigquery.ScalarQueryParameter('file_md5', 'STRING', file_md5),
            bigquery.ScalarQueryParameter(
                'processed_at', 'TIMESTAMP', ingestion_dt.isoformat()
            ),
            bigquery.ScalarQueryParameter('record_count', 'INT64', record_count),
        ]
    )
    client.query(merge_sql, job_config=job_config).result()
    logger.info(f"Registered file {file_path} (md5={file_md5}, records={record_count})")


# ---------------------------------------------------------------------------
# Cleanup: delete stale rows for changed records
# ---------------------------------------------------------------------------

def cleanup_changed_records(
    changed_uids: List[str],
    bq_dataset: str,
    table_names: List[str],
    project: str,
    run_ingestion_ts: str
) -> None:
    """Delete old rows for changed records from all 46 WoS content tables.

    For each table, deletes rows WHERE id IN changed_uids AND
    ingestion_ts < run_ingestion_ts (i.e., the rows written in previous
    pipeline runs, not the fresh ones just appended).

    Args:
        changed_uids: List of UIDs whose content changed this run.
        bq_dataset: BigQuery dataset identifier.
        table_names: List of the 46 content table names.
        project: GCP project ID.
        run_ingestion_ts: ISO-format UTC timestamp for this pipeline run.
    """
    if not changed_uids:
        logger.info("No changed UIDs — skipping cleanup.")
        return

    client = bigquery.Client(project=project)
    dataset_id = _parse_dataset_id(bq_dataset)
    ingestion_dt = datetime.fromisoformat(run_ingestion_ts)

    logger.info(
        f"Cleaning up {len(changed_uids):,} changed records from "
        f"{len(table_names)} tables"
    )

    errors = []
    for table_name in table_names:
        table_ref = f"{project}.{dataset_id}.{table_name}"
        sql = f"""
            DELETE FROM `{table_ref}`
            WHERE id IN UNNEST(@changed_uids)
              AND ingestion_ts < @run_ingestion_ts
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter('changed_uids', 'STRING', changed_uids),
                bigquery.ScalarQueryParameter(
                    'run_ingestion_ts', 'TIMESTAMP', ingestion_dt.isoformat()
                ),
            ]
        )
        try:
            result = client.query(sql, job_config=job_config).result()
            logger.debug(f"Cleaned up table {table_name}")
        except Exception as e:
            logger.error(f"Error cleaning up {table_name}: {e}")
            errors.append((table_name, str(e)))

    if errors:
        logger.error(f"Cleanup errors in {len(errors)} tables: {errors}")
    else:
        logger.info("Cleanup completed successfully for all tables")


# ---------------------------------------------------------------------------
# GCS helpers
# ---------------------------------------------------------------------------

def get_file_md5_from_gcs(file_path: str, project: str) -> Optional[str]:
    """Retrieve the MD5 hash of a GCS object from its metadata.

    Args:
        file_path: GCS path (gs://bucket/path/to/file.xml).
        project: GCP project ID.

    Returns:
        Base64-encoded MD5 string from GCS metadata, or None on error.
    """
    from google.cloud import storage

    if not file_path.startswith('gs://'):
        return None

    parts = file_path[5:].split('/', 1)
    bucket_name = parts[0]
    blob_path = parts[1] if len(parts) > 1 else ''

    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    blob = bucket.get_blob(blob_path)
    if blob is None:
        return None
    return blob.md5_hash  # base64-encoded MD5


def read_changed_uids_from_gcs(gcs_prefix: str, project: str) -> List[str]:
    """Read changed UIDs written by WriteToText during the pipeline.

    Args:
        gcs_prefix: GCS prefix used for WriteToText output
                    (e.g., gs://bucket/temp/changed_uids_<ts>).
        project: GCP project ID.

    Returns:
        Flat list of UID strings.
    """
    from google.cloud import storage

    parts = gcs_prefix[5:].split('/', 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''

    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))

    uids = []
    for blob in blobs:
        content = blob.download_as_text()
        for line in content.splitlines():
            line = line.strip()
            if line:
                uids.append(line)

    logger.info(f"Read {len(uids):,} changed UIDs from GCS temp files")
    return uids


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_dataset_id(bq_dataset: str) -> str:
    """Extract bare dataset name from 'project:dataset' or 'dataset' format."""
    if ':' in bq_dataset:
        return bq_dataset.split(':', 1)[1]
    return bq_dataset
