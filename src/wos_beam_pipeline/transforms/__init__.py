"""Transforms package for WoS Beam Pipeline.

Contains all Apache Beam DoFn transforms for the pipeline.
"""

from .xml_splitter import SplitXMLRecords, ReadXMLFiles
from .xml_parser import ParseXMLRecord
from .schema_validator import ValidateSchema
from .dlq_handler import EnrichDLQRecord, WriteDLQToGCS, FormatDLQAsJSON
from .dedup import FilterUnchangedRecords

__all__ = [
    'SplitXMLRecords',
    'ReadXMLFiles',
    'ParseXMLRecord',
    'ValidateSchema',
    'EnrichDLQRecord',
    'WriteDLQToGCS',
    'FormatDLQAsJSON',
    'FilterUnchangedRecords',
]
