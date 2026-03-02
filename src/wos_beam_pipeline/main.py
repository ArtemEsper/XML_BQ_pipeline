"""Main Dataflow pipeline for WoS XML to BigQuery processing.

This pipeline reads XML files from GCS, parses them using the WoS configuration,
validates the data, and writes to BigQuery with DLQ for failed records.
"""

import apache_beam as beam
import apache_beam.io.fileio
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import argparse
import logging
import json
from typing import Dict, Any

from .transforms import (
    SplitXMLRecords,
    ParseXMLRecord,
    ValidateSchema,
    EnrichDLQRecord,
    FormatDLQAsJSON
)
from .utils import load_config_from_file, load_config_from_gcs, SchemaGenerator


logger = logging.getLogger(__name__)


def parse_arguments(argv=None):
    """Parse command-line arguments.

    Returns:
        Tuple of (known_args, pipeline_args)
    """
    parser = argparse.ArgumentParser(
        description='WoS XML to BigQuery Dataflow Pipeline'
    )

    parser.add_argument(
        '--input_pattern',
        required=True,
        help='GCS pattern for input XML files (e.g., gs://bucket/path/*.xml)'
    )

    parser.add_argument(
        '--config_path',
        required=True,
        help='GCS path to wos_config.xml (e.g., gs://bucket/path/wos_config.xml)'
    )

    parser.add_argument(
        '--schema_path',
        required=True,
        help='GCS path to all_schemas.json (e.g., gs://bucket/path/all_schemas.json)'
    )

    parser.add_argument(
        '--bq_dataset',
        required=True,
        help='BigQuery dataset ID (e.g., project:dataset or dataset)'
    )

    parser.add_argument(
        '--dlq_bucket',
        required=True,
        help='GCS bucket for dead letter queue (without gs:// prefix)'
    )

    parser.add_argument(
        '--record_tag',
        default='REC',
        help='XML tag for individual records (default: REC)'
    )

    parser.add_argument(
        '--id_tag',
        default='UID',
        help='XML tag containing unique identifier (default: UID)'
    )

    parser.add_argument(
        '--namespace',
        default='',
        help='XML namespace (default: empty string)'
    )

    parser.add_argument(
        '--parent_tag',
        default='records',
        help='XML tag wrapping the record collection, used for config path lookups (default: records)'
    )

    parser.add_argument(
        '--file_number',
        type=int,
        default=-1,
        help='File number for tracking (default: -1)'
    )

    parser.add_argument(
        '--bq_write_disposition',
        default='WRITE_APPEND',
        choices=['WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'],
        help='BigQuery write disposition (default: WRITE_APPEND)'
    )

    return parser.parse_known_args(argv)


def run(argv=None):
    """Main pipeline execution function.

    Args:
        argv: Command-line arguments
    """
    # Parse arguments
    known_args, pipeline_args = parse_arguments(argv)

    # Configure pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    logger.info("Starting WoS XML to BigQuery Pipeline")
    logger.info(f"Input pattern: {known_args.input_pattern}")
    logger.info(f"Config path: {known_args.config_path}")
    logger.info(f"BigQuery dataset: {known_args.bq_dataset}")
    logger.info(f"DLQ bucket: {known_args.dlq_bucket}")

    # Load configuration (this will be a side input in the actual pipeline)
    # For now, load it here to pass to transforms
    if known_args.config_path.startswith('gs://'):
        config = load_config_from_gcs(known_args.config_path)
    else:
        config = load_config_from_file(known_args.config_path, known_args.namespace)

    logger.info(f"Loaded config: {config}")

    # Load schemas from GCS or local path
    if known_args.schema_path.startswith('gs://'):
        from google.cloud import storage as gcs
        client = gcs.Client()
        blob = gcs.Blob.from_string(known_args.schema_path, client=client)
        schemas = json.loads(blob.download_as_text())
    else:
        with open(known_args.schema_path, 'r') as f:
            schemas = json.load(f)

    logger.info(f"Loaded schemas for {len(schemas)} tables")

    # Define the pipeline
    with beam.Pipeline(options=pipeline_options) as p:

        # Step 1: Read XML file paths from GCS pattern
        xml_files = (
            p
            | 'MatchXMLFiles' >> beam.io.fileio.MatchFiles(known_args.input_pattern)
            | 'GetFilePath' >> beam.Map(lambda x: x.path)
        )

        # Step 2: Split XML files into individual records
        records = (
            xml_files
            | 'SplitXMLRecords' >> beam.ParDo(
                SplitXMLRecords(
                    record_tag=known_args.record_tag,
                    id_tag=known_args.id_tag,
                    namespace=known_args.namespace
                )
            )
        )

        # Step 3: Parse XML records into table rows
        parsed = (
            records
            | 'ParseXMLRecords' >> beam.ParDo(
                ParseXMLRecord(
                    config=config,
                    record_tag=known_args.record_tag,
                    namespace=known_args.namespace,
                    file_number=known_args.file_number,
                    parent_tag=known_args.parent_tag
                )
            ).with_outputs()
        )

        # Get the DLQ from parsing step
        parse_dlq = parsed[ParseXMLRecord.OUTPUT_TAG_DLQ]

        # Step 4: Process each table's output
        # Get all table names from schemas
        table_names = list(schemas.keys())

        logger.info(f"Processing {len(table_names)} tables")

        all_dlq_records = [parse_dlq]

        for table_name in table_names:
            # Get rows for this table.
            # DoOutputsTuple does not support the 'in' operator reliably, so
            # always use [] which returns an empty PCollection for unused tags.
            table_rows = parsed[table_name]

            # Filter rows to only include schema-defined fields.
            # The parser may produce extra fields (e.g. from config attributes not
            # in the schema); BQ rejects unknown fields by default.
            schema_fields = frozenset(f['name'] for f in schemas[table_name])
            table_rows = (
                table_rows
                | f'FilterSchema_{table_name}' >> beam.Map(
                    lambda row, fields=schema_fields: {k: v for k, v in row.items() if k in fields}
                )
            )

            # Write to BigQuery
            _ = (
                table_rows
                | f'WriteToBigQuery_{table_name}' >> WriteToBigQuery(
                    table=f"{known_args.bq_dataset}.{table_name}",
                    schema={'fields': schemas[table_name]},
                    write_disposition=getattr(
                        BigQueryDisposition,
                        known_args.bq_write_disposition
                    ),
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                    custom_gcs_temp_location=f"gs://{known_args.dlq_bucket}/temp"
                )
            )

        # Step 5: Handle DLQ records
        # Combine all DLQ sources
        combined_dlq = (
            all_dlq_records
            | 'FlattenDLQ' >> beam.Flatten()
        )

        # Enrich DLQ records
        enriched_dlq = (
            combined_dlq
            | 'EnrichDLQ' >> beam.ParDo(EnrichDLQRecord())
        )

        # Format as JSON and write to GCS
        _ = (
            enriched_dlq
            | 'FormatDLQAsJSON' >> beam.ParDo(FormatDLQAsJSON())
            | 'WriteDLQToGCS' >> WriteToText(
                file_path_prefix=f"gs://{known_args.dlq_bucket}/failed_records/",
                file_name_suffix='.jsonl',
                shard_name_template='_SSSSS-of-NNNNN',
                num_shards=10
            )
        )

    logger.info("Pipeline completed successfully")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    run()
