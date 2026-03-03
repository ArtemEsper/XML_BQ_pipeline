"""XML Parser DoFn for transforming WoS records into BigQuery rows.

This transform parses individual <REC> XML elements and transforms them into
rows for 48 normalized BigQuery tables using the configuration mapping.

Adapted from ParseNode() function in generic_parser.py.
"""

import apache_beam as beam
import lxml.etree as etree
from typing import Iterator, Dict, Any, Tuple, List
import logging
from apache_beam import pvalue

from ..models import TableList
from ..utils.config_parser import WosConfig


logger = logging.getLogger(__name__)


class ParseXMLRecord(beam.DoFn):
    """DoFn to parse XML record into multiple table rows.

    This is the core parsing transform that converts a single <REC> element
    into rows for up to 48 different normalized tables. It uses the WoS
    configuration to map XML structure to database schema.

    Uses Beam's TaggedOutput to emit rows for different tables.
    """

    # Define output tags for all 48 tables (will be set dynamically)
    OUTPUT_TAG_SUCCESS = 'success'
    OUTPUT_TAG_DLQ = 'dlq'

    def __init__(
        self,
        config: WosConfig,
        record_tag: str,
        namespace: str = '',
        file_number: int = -1,
        parent_tag: str = '',
        ingestion_ts: str = ''
    ):
        """Initialize the XML parser.

        Args:
            config: WosConfig object with lookup dictionaries
            record_tag: XML tag name for records (e.g., 'REC')
            namespace: XML namespace prefix (default: '')
            file_number: Optional file number for tracking (default: -1)
            parent_tag: XML tag wrapping the record collection (e.g., 'records').
                        Used to build config lookup paths, matching the original
                        generic_parser.py -p option (default: '')
            ingestion_ts: ISO-format UTC timestamp injected into all rows (default: '')
        """
        self.config = config
        self.record_tag = record_tag
        self.namespace = namespace
        self.file_number = file_number
        self.parent_tag = parent_tag
        self.ingestion_ts = ingestion_ts

    def process(
        self,
        element: Tuple[str, str, str],
        *args,
        **kwargs
    ) -> Iterator[pvalue.TaggedOutput]:
        """Process a single XML record.

        Args:
            element: Tuple of (record_id, xml_string, record_hash)

        Yields:
            TaggedOutput objects with table rows or DLQ records
        """
        record_id, xml_string, record_hash = element

        try:
            # Parse XML string
            elem = etree.fromstring(xml_string.encode('utf-8'))

            # Initialize table list for this record
            table_list = TableList()
            output_rows = []

            # Build the path for lookups.
            # Config keys follow the structure of the config XML file.
            # If a parent_tag (container element) is given, path = "parent/record"
            # matching the original generic_parser.py -p/-r options.
            # _parse_node strips namespace from child tags, so path uses bare names.
            if self.parent_tag:
                path = f"{self.parent_tag}/{self.record_tag}"
            else:
                path = self.record_tag
            table_path = f"{path}/table"
            valuepath = path + "/"

            # Get the core table name from the lookup
            if table_path not in self.config.table_dict:
                raise ValueError(f"Core table not found in config: {table_path}")

            core_table_name = self.config.table_dict[table_path]

            # Create the core table
            table_list.add_table(
                core_table_name,
                None,
                path,
                self.config.ctr_dict
            )

            # Set the primary key (use the record_id we already extracted)
            id_value = record_id
            table_list.add_identifier(core_table_name, 'id', id_value)

            # Inject pipeline-level fields into the core (root) table row
            if record_hash:
                table_list.add_col(core_table_name, 'record_hash', record_hash)
            if self.ingestion_ts:
                table_list.add_col(core_table_name, 'ingestion_ts', self.ingestion_ts)

            # Handle file number if configured
            file_number_path = f"{path}/file_number"
            if file_number_path in self.config.table_dict:
                file_number_spec = self.config.table_dict[file_number_path]
                table_name, col_name = file_number_spec.split(":", 1)
                table_list.add_col(table_name, col_name, self.file_number)

            # Process attributes of the root element
            attrib_seen = set()
            for attrib_name, attrib_value in elem.attrib.items():
                attrib_path = path + "/" + attrib_name
                if attrib_path in self.config.attrib_dict:
                    table_name, col_name = self.config.attrib_dict[attrib_path].split(":")[:2]
                    table_list.add_col(table_name, col_name, str(attrib_value))
                    attrib_seen.add(attrib_name)

            # Process default attribute values
            for attrib_name, attrib_value_all in self.config.attrib_defaults.get(path, {}).items():
                if attrib_name not in attrib_seen:
                    table_name, col_name, attrib_value = attrib_value_all.split(":")[:3]
                    table_list.add_col(table_name, col_name, str(attrib_value))

            # Process the value of the root element
            if valuepath in self.config.value_dict:
                if elem.text is not None:
                    table_name, col_name = self.config.value_dict[valuepath].split(":", 1)
                    table_list.add_col(table_name, col_name, str(elem.text))

            # Process the children recursively
            for child in elem:
                self._parse_node(
                    child, path, table_list, core_table_name, output_rows
                )

            # Close the primary table
            result = table_list.close_table(core_table_name)
            if result:
                output_rows.append(result)

            # Emit all rows with their table tags
            for table_name, row in output_rows:
                yield pvalue.TaggedOutput(table_name, row)

            logger.debug(f"Successfully parsed record {record_id}: {len(output_rows)} rows")

        except Exception as e:
            # Send to DLQ
            logger.warning(f"Error parsing record {record_id}: {e}")
            dlq_record = {
                'record_id': record_id,
                'xml': xml_string,
                'error': str(e),
                'error_type': type(e).__name__,
                'pipeline_step': 'ParseXMLRecord'
            }
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_DLQ, dlq_record)

    def _parse_node(
        self,
        node: etree.Element,
        path: str,
        table_list: TableList,
        last_opened: str,
        output_rows: List[Tuple[str, Dict[str, Any]]]
    ) -> None:
        """Recursively parse an XML node and its children.

        This is adapted from ParseNode() in generic_parser.py.

        Args:
            node: Current XML element
            path: Current path in the tree
            table_list: TableList managing active tables
            last_opened: Name of the last opened table (parent)
            output_rows: List to append completed rows to
        """
        # Extract tag name (handle namespaces)
        if node.tag.find("}") > -1:
            tag = node.tag.split("}", 1)[1]
        else:
            tag = node.tag

        newpath = path + "/" + tag

        # Check if we need a new table
        table_path = newpath + "/table"
        valuepath = newpath + "/"

        if table_path in self.config.table_dict:
            new_table = True
            table_name = self.config.table_dict[table_path]
            table_list.add_table(
                table_name,
                last_opened,
                newpath,
                self.config.ctr_dict
            )
        else:
            new_table = False
            table_name = last_opened

        # Process attributes
        attrib_seen = set()
        for attrib_name, attrib_value in node.attrib.items():
            attrib_path = newpath + "/" + attrib_name
            if attrib_path in self.config.attrib_dict:
                tbl_name, col_name = self.config.attrib_dict[attrib_path].split(":")[:2]
                table_list.add_col(tbl_name, col_name, str(attrib_value))
                attrib_seen.add(attrib_name)

        # Process default attribute values
        for attrib_name, attrib_value_all in self.config.attrib_defaults.get(newpath, {}).items():
            if attrib_name not in attrib_seen:
                tbl_name, col_name, attrib_value = attrib_value_all.split(":")[:3]
                table_list.add_col(tbl_name, col_name, str(attrib_value))

        # Process value
        if valuepath in self.config.value_dict:
            if node.text is not None:
                tbl_name, col_name = self.config.value_dict[valuepath].split(":", 1)
                table_list.add_col(tbl_name, col_name, str(node.text))

        # Process children recursively
        for child in node:
            self._parse_node(child, newpath, table_list, table_name, output_rows)

        # If we created a new table for this tag, close it
        if new_table:
            result = table_list.close_table(table_name)
            if result:
                output_rows.append(result)
