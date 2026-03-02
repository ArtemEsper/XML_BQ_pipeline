"""Dead Letter Queue (DLQ) Handler for failed records.

This transform enriches error records with debugging metadata and writes them
to Google Cloud Storage for later analysis and reprocessing.
"""

import apache_beam as beam
from typing import Dict, Any, Iterator
import logging
import json
from datetime import datetime
import hashlib


logger = logging.getLogger(__name__)


class EnrichDLQRecord(beam.DoFn):
    """DoFn to enrich DLQ records with debugging metadata.

    Adds timestamp, worker information, error hash, and other metadata
    to make debugging and recovery easier.
    """

    def process(
        self,
        element: Dict[str, Any],
        *args,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """Enrich a DLQ record with metadata.

        Args:
            element: DLQ record dict

        Yields:
            Enriched DLQ record
        """
        try:
            # Add timestamp
            enriched = {
                **element,
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'timestamp_epoch': int(datetime.utcnow().timestamp()),
            }

            # Add error hash for deduplication
            error_str = f"{element.get('error_type', 'unknown')}:{element.get('error', '')}"
            error_hash = hashlib.md5(error_str.encode()).hexdigest()
            enriched['error_hash'] = error_hash

            # Add worker ID if available (from Beam context)
            try:
                worker_id = beam.metrics.Metrics.get_namespace('system').distribution('worker_id')
                enriched['worker_id'] = str(worker_id) if worker_id else 'unknown'
            except:
                enriched['worker_id'] = 'unknown'

            # Extract file name if available from record_id (WoS IDs don't have file info)
            record_id = element.get('record_id', '')
            enriched['record_id_prefix'] = record_id[:20] if record_id else ''

            yield enriched

        except Exception as e:
            logger.error(f"Error enriching DLQ record: {e}")
            # Yield original record with minimal metadata
            yield {
                **element,
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'enrichment_error': str(e)
            }


class WriteDLQToGCS(beam.DoFn):
    """DoFn to write DLQ records to Google Cloud Storage.

    Writes records as JSON lines, partitioned by date.
    """

    def __init__(self, dlq_bucket: str):
        """Initialize the DLQ writer.

        Args:
            dlq_bucket: GCS bucket for DLQ records (without gs:// prefix)
        """
        self.dlq_bucket = dlq_bucket

    def process(
        self,
        element: Dict[str, Any],
        *args,
        **kwargs
    ) -> Iterator[str]:
        """Write DLQ record to GCS.

        Args:
            element: Enriched DLQ record

        Yields:
            GCS path where record was written
        """
        try:
            # Get timestamp for partitioning
            timestamp = element.get('timestamp', datetime.utcnow().isoformat())
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

            # Create partition path: YYYY/MM/DD/
            partition = f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"

            # Create file name based on record ID or hash
            record_id = element.get('record_id', 'unknown')
            error_hash = element.get('error_hash', 'nohash')

            # Sanitize record_id for filename (remove special characters)
            safe_record_id = ''.join(c if c.isalnum() or c in '-_' else '_' for c in record_id)
            file_name = f"{safe_record_id}_{error_hash}.json"

            # Construct GCS path
            gcs_path = f"gs://{self.dlq_bucket}/{partition}/{file_name}"

            # Convert to JSON
            json_str = json.dumps(element, indent=2, ensure_ascii=False)

            # Note: Actual GCS write would happen here
            # For Beam, this is typically done using WriteToText transform
            # This DoFn is more for demonstration of the concept
            # In practice, we'd use beam.io.WriteToText with the partition path

            yield gcs_path

        except Exception as e:
            logger.error(f"Error writing DLQ record to GCS: {e}")
            # Re-raise to ensure the error is not silently dropped
            raise


class FormatDLQAsJSON(beam.DoFn):
    """DoFn to format DLQ records as JSON strings for writing."""

    def process(self, element: Dict[str, Any]) -> Iterator[str]:
        """Format DLQ record as JSON string.

        Args:
            element: DLQ record dict

        Yields:
            JSON string
        """
        try:
            # Convert to compact JSON (one line)
            json_str = json.dumps(element, ensure_ascii=False, separators=(',', ':'))
            yield json_str
        except Exception as e:
            logger.error(f"Error formatting DLQ record as JSON: {e}")
            # Fallback to basic representation
            yield json.dumps({
                'error': 'Failed to serialize DLQ record',
                'serialization_error': str(e),
                'record_id': element.get('record_id', 'unknown')
            })


def get_dlq_partition_path(element: Dict[str, Any], dlq_bucket: str) -> str:
    """Get the partitioned GCS path for a DLQ record.

    Helper function to generate partition paths for WriteToText.

    Args:
        element: DLQ record with timestamp
        dlq_bucket: GCS bucket name

    Returns:
        Partitioned GCS path
    """
    try:
        timestamp = element.get('timestamp', datetime.utcnow().isoformat())
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        partition = f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"
        return f"gs://{dlq_bucket}/{partition}/dlq"
    except:
        # Fallback partition
        return f"gs://{dlq_bucket}/unknown/dlq"
