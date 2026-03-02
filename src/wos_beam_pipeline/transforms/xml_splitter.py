"""XML Splitter DoFn for splitting large XML files into individual records.

This transform reads large XML files and splits them into individual <REC> elements
for parallel processing by downstream transforms.
"""

import apache_beam as beam
import lxml.etree as etree
from typing import Iterator, Tuple
import logging
from google.cloud import storage
import io


logger = logging.getLogger(__name__)


class SplitXMLRecords(beam.DoFn):
    """DoFn to split XML file into individual record elements.

    Uses streaming XML parsing (iterparse) to handle large files without
    loading the entire file into memory. Each record is yielded as a tuple
    of (record_id, xml_string).

    This approach is adapted from the generic_parser.py iterparse pattern.
    """

    def __init__(self, record_tag: str, id_tag: str, namespace: str = ''):
        """Initialize the XML splitter.

        Args:
            record_tag: XML tag name that defines a single record (e.g., 'REC')
            id_tag: XML tag name containing the unique identifier
            namespace: XML namespace prefix (default: '')
        """
        self.record_tag = record_tag
        self.id_tag = id_tag
        self.namespace = namespace

    def process(self, gcs_path: str) -> Iterator[Tuple[str, str]]:
        """Process a GCS XML file and yield individual records.

        Args:
            gcs_path: GCS path to XML file (gs://bucket/path/to/file.xml)

        Yields:
            Tuples of (record_id, xml_string)
        """
        logger.info(f"Splitting XML file: {gcs_path}")

        # Download file from GCS
        xml_content = self._download_from_gcs(gcs_path)

        # Stream parse the XML
        record_count = 0
        # lxml iterparse requires Clark notation: {namespace}tag
        if self.namespace:
            full_record_tag = f"{{{self.namespace}}}{self.record_tag}"
        else:
            full_record_tag = self.record_tag

        try:
            # Use iterparse for memory-efficient streaming
            context = etree.iterparse(
                io.BytesIO(xml_content),
                events=('end',),
                tag=full_record_tag,
                remove_comments=True
            )

            for event, elem in context:
                # Extract record ID
                record_id = self._extract_record_id(elem)

                # Convert element to string
                xml_string = etree.tostring(
                    elem,
                    encoding='unicode',
                    method='xml'
                )

                # Yield the record
                yield (record_id, xml_string)

                record_count += 1

                # Critical: Clear element from memory to avoid memory buildup
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

            logger.info(f"Split {record_count} records from {gcs_path}")

        except Exception as e:
            logger.error(f"Error splitting XML file {gcs_path}: {e}")
            raise

    def _download_from_gcs(self, gcs_path: str) -> bytes:
        """Download file from Google Cloud Storage.

        Args:
            gcs_path: GCS path (gs://bucket/path/to/file.xml)

        Returns:
            File contents as bytes
        """
        if not gcs_path.startswith('gs://'):
            raise ValueError(f"Invalid GCS path: {gcs_path}")

        # Parse GCS path
        parts = gcs_path[5:].split('/', 1)
        bucket_name = parts[0]
        blob_path = parts[1] if len(parts) > 1 else ''

        # Download
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        logger.info(f"Downloading from GCS: gs://{bucket_name}/{blob_path}")
        return blob.download_as_bytes()

    def _extract_record_id(self, elem: etree.Element) -> str:
        """Extract the unique identifier from a record element.

        Args:
            elem: XML element representing a record

        Returns:
            Record ID as string

        Raises:
            ValueError: If ID cannot be extracted
        """
        # If the record tag itself contains the ID
        if self.id_tag == self.record_tag:
            if elem.text:
                return elem.text.strip()
            else:
                raise ValueError("Record tag has no text content for ID")

        # Otherwise, find the ID tag within the record
        # lxml find() also requires Clark notation for namespace
        if self.namespace:
            id_seek = f"{{{self.namespace}}}{self.id_tag}"
        else:
            id_seek = self.id_tag
        id_node = elem.find(id_seek)

        if id_node is not None and id_node.text:
            return id_node.text.strip()

        # Try without namespace if not found
        if self.namespace:
            id_node = elem.find(self.id_tag)
            if id_node is not None and id_node.text:
                return id_node.text.strip()

        # Fallback: try to find UID tag (common in WoS data)
        uid_node = elem.find('.//{*}UID')
        if uid_node is not None and uid_node.text:
            return uid_node.text.strip()

        raise ValueError(f"Could not extract ID from record (tag: {self.id_tag})")


class ReadXMLFiles(beam.DoFn):
    """DoFn to read multiple XML files from a GCS pattern.

    This is a helper transform that reads file paths from GCS and
    passes them to SplitXMLRecords.
    """

    def process(self, file_path: str) -> Iterator[str]:
        """Process a file path element.

        Args:
            file_path: GCS path to XML file

        Yields:
            The same file path (pass-through)
        """
        logger.info(f"Processing XML file: {file_path}")
        yield file_path
