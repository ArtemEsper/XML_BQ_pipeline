"""Schema generator for converting PostgreSQL DDL to BigQuery schemas.

This utility parses the wos_schema_final.sql file and generates BigQuery
TableSchema JSON files for use in both Terraform and runtime validation.
"""

import re
import json
from typing import Dict, List, Any
from pathlib import Path
import logging


logger = logging.getLogger(__name__)


# Type mapping from PostgreSQL to BigQuery
PG_TO_BQ_TYPE_MAP = {
    'character varying': 'STRING',
    'integer': 'INTEGER',
    'bigint': 'INTEGER',
    'smallint': 'INTEGER',
    'numeric': 'NUMERIC',
    'decimal': 'NUMERIC',
    'real': 'FLOAT',
    'double precision': 'FLOAT',
    'boolean': 'BOOLEAN',
    'date': 'DATE',
    'timestamp': 'TIMESTAMP',
    'timestamp without time zone': 'TIMESTAMP',
    'timestamp with time zone': 'TIMESTAMP',
    'text': 'STRING',
}


class SchemaGenerator:
    """Generator for BigQuery schemas from PostgreSQL DDL."""

    def __init__(self, sql_file_path: str):
        """Initialize schema generator.

        Args:
            sql_file_path: Path to PostgreSQL schema SQL file
        """
        self.sql_file_path = sql_file_path
        self.schemas: Dict[str, List[Dict[str, Any]]] = {}

    def parse_sql(self) -> Dict[str, List[Dict[str, Any]]]:
        """Parse SQL file and extract table schemas.

        Returns:
            Dict mapping table names to BigQuery schema field lists
        """
        logger.info(f"Parsing SQL file: {self.sql_file_path}")

        with open(self.sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Extract CREATE TABLE statements
        create_table_pattern = r'CREATE TABLE\s+(?:public\.)?(\w+)\s*\((.*?)\);'
        matches = re.finditer(create_table_pattern, sql_content, re.DOTALL | re.IGNORECASE)

        for match in matches:
            table_name = match.group(1)
            columns_text = match.group(2)

            fields = self._parse_columns(columns_text)
            self.schemas[table_name] = fields

            logger.debug(f"Parsed table {table_name}: {len(fields)} fields")

        logger.info(f"Parsed {len(self.schemas)} tables")
        return self.schemas

    def _parse_columns(self, columns_text: str) -> List[Dict[str, Any]]:
        """Parse column definitions from CREATE TABLE statement.

        Args:
            columns_text: Column definitions text

        Returns:
            List of BigQuery field definitions
        """
        fields = []

        # Split by comma, but be careful with CONSTRAINT clauses
        lines = columns_text.split('\n')
        current_col = ""

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Skip CONSTRAINT lines
            if line.upper().startswith('CONSTRAINT') or line.upper().startswith('PRIMARY KEY'):
                continue

            # Accumulate multiline column definitions
            if current_col:
                current_col += " " + line
            else:
                current_col = line

            # If line ends with comma or we're at the last line
            if current_col.endswith(',') or not line.endswith(','):
                # Remove trailing comma
                col_def = current_col.rstrip(',').strip()

                # Parse the column
                field = self._parse_column_definition(col_def)
                if field:
                    fields.append(field)

                current_col = ""

        return fields

    def _parse_column_definition(self, col_def: str) -> Dict[str, Any]:
        """Parse a single column definition.

        Args:
            col_def: Column definition string

        Returns:
            BigQuery field definition dict
        """
        # Skip CONSTRAINT clauses
        if col_def.upper().startswith('CONSTRAINT'):
            return None

        # Parse: column_name type [NOT NULL] [CONSTRAINT ...]
        # Example: id character varying NOT NULL CONSTRAINT wos_summary_pk PRIMARY KEY
        parts = col_def.split()
        if len(parts) < 2:
            return None

        col_name = parts[0]

        # Find the type (may be multiple words like "character varying")
        pg_type = None
        for type_name in sorted(PG_TO_BQ_TYPE_MAP.keys(), key=len, reverse=True):
            if type_name in col_def.lower():
                pg_type = type_name
                break

        if not pg_type:
            # Default to STRING for unknown types
            logger.warning(f"Unknown type for column {col_name}, defaulting to STRING")
            pg_type = 'character varying'

        bq_type = PG_TO_BQ_TYPE_MAP[pg_type]

        # Check for NOT NULL constraint
        mode = 'REQUIRED' if 'NOT NULL' in col_def.upper() else 'NULLABLE'

        return {
            'name': col_name,
            'type': bq_type,
            'mode': mode,
            'description': ''  # Can be populated from COMMENT statements
        }

    def write_json_schemas(self, output_dir: str) -> None:
        """Write BigQuery schemas as JSON files.

        Args:
            output_dir: Directory to write schema JSON files
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        for table_name, fields in self.schemas.items():
            schema_file = output_path / f"{table_name}_schema.json"

            with open(schema_file, 'w') as f:
                json.dump(fields, f, indent=2)

            logger.info(f"Wrote schema: {schema_file}")

    def write_terraform_schemas(self, output_file: str) -> None:
        """Write schemas as Terraform-compatible JSON.

        Creates a single JSON file with all table schemas for use in Terraform.

        Args:
            output_file: Path to output JSON file
        """
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(self.schemas, f, indent=2)

        logger.info(f"Wrote Terraform schemas: {output_file}")

    def get_table_names(self) -> List[str]:
        """Get list of all table names.

        Returns:
            Sorted list of table names
        """
        return sorted(self.schemas.keys())


def main():
    """Main function for standalone schema generation."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python schema_generator.py <path_to_sql_file> [output_dir]")
        sys.exit(1)

    sql_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else './config/schemas'

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Generate schemas
    generator = SchemaGenerator(sql_file)
    schemas = generator.parse_sql()

    # Write output files
    generator.write_json_schemas(output_dir)
    generator.write_terraform_schemas(f"{output_dir}/all_schemas.json")

    print(f"\nGenerated schemas for {len(schemas)} tables:")
    for table_name in generator.get_table_names():
        print(f"  - {table_name}")


if __name__ == "__main__":
    main()
