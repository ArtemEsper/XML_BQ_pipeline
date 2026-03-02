"""Configuration parser for WoS XML processing.

Parses the wos_config.xml file to create lookup dictionaries for table
creation, value mapping, attribute mapping, and counters.

Adapted from ReadConfig() function in generic_parser.py for Beam compatibility.
"""

import lxml.etree as etree
from typing import Dict, Any
from google.cloud import storage
import io
import logging


logger = logging.getLogger(__name__)


class WosConfig:
    """Configuration object holding all lookup dictionaries."""

    def __init__(
        self,
        table_dict: Dict[str, str],
        value_dict: Dict[str, str],
        ctr_dict: Dict[str, str],
        attrib_dict: Dict[str, str],
        attrib_defaults: Dict[str, Dict[str, str]]
    ):
        """Initialize configuration.

        Args:
            table_dict: Maps XML paths to table names
            value_dict: Maps XML paths to column specifications
            ctr_dict: Maps XML paths to counter specifications
            attrib_dict: Maps XML attribute paths to column specifications
            attrib_defaults: Maps XML paths to default attribute values
        """
        self.table_dict = table_dict
        self.value_dict = value_dict
        self.ctr_dict = ctr_dict
        self.attrib_dict = attrib_dict
        self.attrib_defaults = attrib_defaults

    def __repr__(self):
        return (f"WosConfig(tables={len(self.table_dict)}, "
                f"values={len(self.value_dict)}, "
                f"ctrs={len(self.ctr_dict)}, "
                f"attribs={len(self.attrib_dict)})")


def load_config_from_gcs(gcs_path: str) -> WosConfig:
    """Load configuration from Google Cloud Storage.

    Args:
        gcs_path: GCS path (gs://bucket/path/to/config.xml)

    Returns:
        WosConfig object

    Raises:
        ValueError: If GCS path is invalid
    """
    if not gcs_path.startswith('gs://'):
        raise ValueError(f"Invalid GCS path: {gcs_path}")

    # Parse GCS path
    parts = gcs_path[5:].split('/', 1)
    bucket_name = parts[0]
    blob_path = parts[1] if len(parts) > 1 else ''

    logger.info(f"Loading config from GCS: {gcs_path}")

    # Download config file
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_bytes()

    # Parse XML
    return parse_config_xml(io.BytesIO(content), namespace='')


def load_config_from_file(file_path: str, namespace: str = '') -> WosConfig:
    """Load configuration from local file.

    Args:
        file_path: Path to local config.xml file
        namespace: XML namespace (default: '')

    Returns:
        WosConfig object
    """
    logger.info(f"Loading config from file: {file_path}")
    with open(file_path, 'rb') as f:
        return parse_config_xml(f, namespace)


def parse_config_xml(xml_source: Any, namespace: str = '') -> WosConfig:
    """Parse configuration XML and build lookup dictionaries.

    This is the core function adapted from ReadConfig() in generic_parser.py.
    Instead of writing to global dictionaries, it returns a structured object.

    Args:
        xml_source: File-like object or path to XML
        namespace: XML namespace prefix (default: '')

    Returns:
        WosConfig object with all lookup dictionaries
    """
    # Initialize lookup dictionaries
    table_dict = {}
    value_dict = {}
    ctr_dict = {}
    attrib_dict = {}
    attrib_defaults = {}

    # Parse XML
    root = etree.parse(xml_source).getroot()

    # Recursively build dictionaries
    _read_config_node(
        root, "", namespace,
        table_dict, value_dict, ctr_dict, attrib_dict, attrib_defaults
    )

    logger.info(f"Config parsed: {len(table_dict)} tables, "
                f"{len(value_dict)} values, {len(ctr_dict)} counters")

    return WosConfig(table_dict, value_dict, ctr_dict, attrib_dict, attrib_defaults)


def _read_config_node(
    node: etree.Element,
    path: str,
    namespace: str,
    table_dict: Dict[str, str],
    value_dict: Dict[str, str],
    ctr_dict: Dict[str, str],
    attrib_dict: Dict[str, str],
    attrib_defaults: Dict[str, Dict[str, str]]
) -> None:
    """Recursively read configuration node and populate lookup dictionaries.

    This function goes through the config file, reading each tag and attribute
    to create the needed lookup tables. All tags and attributes are recorded
    by full path, so name reuse shouldn't be a problem.

    Args:
        node: Current XML element
        path: Current path in the tree
        namespace: XML namespace prefix
        table_dict: Output - maps paths to table names
        value_dict: Output - maps paths to column specifications
        ctr_dict: Output - maps paths to counter specifications
        attrib_dict: Output - maps attribute paths to column specifications
        attrib_defaults: Output - maps paths to default attribute values
    """
    newpath = path + node.tag + "/"

    # Write the value lookup for the tag
    if node.text is not None:
        if str(node.text).strip() > '':
            value_dict[f"{namespace}{newpath}"] = node.text

    # Go through the attributes in the config file
    # Specialized ones like table and ctr_id go into their own lookups,
    # the rest go into the attribute lookup
    for attrib_name, attrib_value_all in node.attrib.items():
        attrib_value = ':'.join(attrib_value_all.split(':')[:2])

        attrib_path = newpath + attrib_name
        if attrib_name == "table":
            table_dict[f"{namespace}{attrib_path}"] = attrib_value
        elif attrib_name == "ctr_id":
            ctr_dict[f"{namespace}{attrib_path}"] = attrib_value
        elif attrib_name == "file_number":
            # Store file_number spec in table_dict so ParseXMLRecord can find
            # it at "path/file_number" and inject self.file_number into the row.
            table_dict[f"{namespace}{attrib_path}"] = attrib_value
        else:
            attrib_dict[f"{namespace}{attrib_path}"] = attrib_value

            # Providing a third tuple item specifies the default value for that attribute
            # If the attribute isn't found in the data, use the default value instead
            if len(attrib_value_all.split(':')) == 3:
                path_key = f"{namespace}{newpath}".strip('/')
                defaults = attrib_defaults.setdefault(path_key, {})
                defaults[attrib_name] = attrib_value_all

    # Recurse for the children of the node
    for child in node:
        _read_config_node(
            child, newpath, namespace,
            table_dict, value_dict, ctr_dict, attrib_dict, attrib_defaults
        )
