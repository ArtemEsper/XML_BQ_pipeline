"""Unit tests for schema generator."""

import pytest
from wos_beam_pipeline.utils import SchemaGenerator


class TestSchemaGenerator:
    """Test schema generator functions."""

    def test_type_mapping(self):
        """Test PostgreSQL to BigQuery type mapping."""
        generator = SchemaGenerator("dummy.sql")

        # Test column parsing
        field = generator._parse_column_definition(
            "id character varying NOT NULL"
        )

        assert field is not None
        assert field['name'] == 'id'
        assert field['type'] == 'STRING'
        assert field['mode'] == 'REQUIRED'

    def test_integer_type(self):
        """Test integer type mapping."""
        generator = SchemaGenerator("dummy.sql")

        field = generator._parse_column_definition(
            "file_number integer NOT NULL"
        )

        assert field['name'] == 'file_number'
        assert field['type'] == 'INTEGER'
        assert field['mode'] == 'REQUIRED'

    def test_nullable_field(self):
        """Test nullable field parsing."""
        generator = SchemaGenerator("dummy.sql")

        field = generator._parse_column_definition(
            "optional_field character varying"
        )

        assert field['mode'] == 'NULLABLE'
