"""Schema Validator DoFn for validating rows against BigQuery schemas.

This transform validates rows before writing to BigQuery, ensuring data quality
and catching schema mismatches early. Invalid rows are sent to DLQ.
"""

import apache_beam as beam
from typing import Dict, Any, Iterator
import logging
from apache_beam import pvalue
from datetime import datetime
import json


logger = logging.getLogger(__name__)


class ValidateSchema(beam.DoFn):
    """DoFn to validate rows against BigQuery table schemas.

    Performs type coercion and validation. Rows that fail validation
    are sent to the DLQ with error details.
    """

    OUTPUT_TAG_VALID = 'valid'
    OUTPUT_TAG_DLQ = 'dlq'

    def __init__(self, schema_dict: Dict[str, list]):
        """Initialize the schema validator.

        Args:
            schema_dict: Dict mapping table names to BigQuery schema field lists
        """
        self.schema_dict = schema_dict

    def process(
        self,
        element: Dict[str, Any],
        table_name: str,
        *args,
        **kwargs
    ) -> Iterator[pvalue.TaggedOutput]:
        """Validate a row against its table schema.

        Args:
            element: Row dict to validate
            table_name: Name of the target table

        Yields:
            TaggedOutput with validated row or DLQ error
        """
        try:
            # Get schema for this table
            if table_name not in self.schema_dict:
                raise ValueError(f"Schema not found for table: {table_name}")

            schema = self.schema_dict[table_name]

            # Validate and coerce row
            validated_row = self._validate_row(element, schema, table_name)

            # Emit validated row
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_VALID, validated_row)

        except Exception as e:
            logger.warning(f"Schema validation failed for {table_name}: {e}")

            # Send to DLQ
            dlq_record = {
                'table_name': table_name,
                'row': element,
                'error': str(e),
                'error_type': type(e).__name__,
                'pipeline_step': 'ValidateSchema'
            }
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_DLQ, dlq_record)

    def _validate_row(
        self,
        row: Dict[str, Any],
        schema: list,
        table_name: str
    ) -> Dict[str, Any]:
        """Validate and coerce a row according to schema.

        Args:
            row: Input row
            schema: BigQuery schema field list
            table_name: Table name for error messages

        Returns:
            Validated and type-coerced row

        Raises:
            ValueError: If validation fails
        """
        validated = {}

        # Create field map for quick lookup
        field_map = {field['name']: field for field in schema}

        # Check required fields
        for field in schema:
            field_name = field['name']
            field_type = field['type']
            field_mode = field['mode']

            # Check if required field is missing
            if field_mode == 'REQUIRED' and field_name not in row:
                raise ValueError(f"Required field missing: {field_name}")

            # Get value (may be None for nullable fields)
            value = row.get(field_name)

            # Coerce type
            if value is not None:
                try:
                    validated[field_name] = self._coerce_type(value, field_type)
                except Exception as e:
                    raise ValueError(
                        f"Type coercion failed for {field_name}: {value} -> {field_type}: {e}"
                    )
            else:
                # Null value for nullable field
                if field_mode == 'REQUIRED':
                    raise ValueError(f"Required field has null value: {field_name}")
                validated[field_name] = None

        # Check for extra fields not in schema
        extra_fields = set(row.keys()) - set(field_map.keys())
        if extra_fields:
            logger.warning(
                f"Table {table_name} has extra fields not in schema: {extra_fields}"
            )
            # Include extra fields in output (BigQuery will ignore them)
            for field_name in extra_fields:
                validated[field_name] = row[field_name]

        return validated

    @staticmethod
    def _coerce_type(value: Any, field_type: str) -> Any:
        """Coerce a value to the specified BigQuery type.

        Args:
            value: Input value
            field_type: BigQuery type (STRING, INTEGER, FLOAT, etc.)

        Returns:
            Type-coerced value

        Raises:
            ValueError: If coercion fails
        """
        # Handle None
        if value is None:
            return None

        # STRING - always coercible
        if field_type == 'STRING':
            return str(value)

        # INTEGER
        elif field_type == 'INTEGER':
            if isinstance(value, int):
                return value
            elif isinstance(value, str):
                # Try to parse as int
                return int(value)
            else:
                raise ValueError(f"Cannot coerce {type(value)} to INTEGER")

        # FLOAT/NUMERIC
        elif field_type in ('FLOAT', 'NUMERIC'):
            if isinstance(value, (int, float)):
                return float(value)
            elif isinstance(value, str):
                return float(value)
            else:
                raise ValueError(f"Cannot coerce {type(value)} to {field_type}")

        # BOOLEAN
        elif field_type == 'BOOLEAN':
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                lower_val = value.lower()
                if lower_val in ('true', '1', 'yes', 't', 'y'):
                    return True
                elif lower_val in ('false', '0', 'no', 'f', 'n'):
                    return False
                else:
                    raise ValueError(f"Cannot coerce '{value}' to BOOLEAN")
            elif isinstance(value, int):
                return bool(value)
            else:
                raise ValueError(f"Cannot coerce {type(value)} to BOOLEAN")

        # DATE
        elif field_type == 'DATE':
            if isinstance(value, str):
                # Try to parse common date formats
                # BigQuery expects YYYY-MM-DD format
                # For now, just validate it's a string
                # More sophisticated parsing could be added
                return value
            else:
                raise ValueError(f"Cannot coerce {type(value)} to DATE")

        # TIMESTAMP
        elif field_type == 'TIMESTAMP':
            if isinstance(value, str):
                # BigQuery expects ISO 8601 format
                return value
            else:
                raise ValueError(f"Cannot coerce {type(value)} to TIMESTAMP")

        # Unknown type - pass through
        else:
            logger.warning(f"Unknown field type {field_type}, passing through")
            return value
