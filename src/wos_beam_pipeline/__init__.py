"""WoS XML to BigQuery Beam Pipeline.

Production-ready Google Cloud Dataflow pipeline for processing Web of Science
XML data into BigQuery with comprehensive error handling, monitoring, and
infrastructure automation.
"""

__version__ = "1.0.0"
__author__ = "Web of Science Data Team"

from .models import Column, Table, TableList
from .utils import WosConfig, load_config_from_file, load_config_from_gcs

__all__ = [
    # Models
    'Column',
    'Table',
    'TableList',
    # Utils
    'WosConfig',
    'load_config_from_file',
    'load_config_from_gcs',
]
