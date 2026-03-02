"""TableList model for managing collections of tables.

The TableList is the memory structure that stores data as it's read from XML.
This is the interface for handling Tables during the main parsing process.
Adapted from generic_parser.py for Beam compatibility.
"""

from typing import List, Dict, Any, Optional, Tuple
from .table import Table


class TableList:
    """Manages a collection of tables during XML parsing.

    Ensures only one instance of a table with the same name exists at a time.
    Provides methods to add tables, add columns to tables, and close tables
    (generating output rows).

    Usage:
        - AddTable: When a tag that needs a table opens
        - AddIdentifier: Only needed for the master table
        - AddCol: For each value detected in the XML
        - CloseTable: When a tag that created a table closes
    """

    def __init__(self):
        """Initialize an empty table list."""
        self.tlist: List[Table] = []

    def add_table(
        self,
        table_name: str,
        parent_name: Optional[str],
        table_path: str,
        ctr_dict: Dict[str, str]
    ) -> None:
        """Add a new table to the list.

        Args:
            table_name: Name of the table to create
            parent_name: Name of the parent table (None for root)
            table_path: Full XML path to this table
            ctr_dict: Counter dictionary from configuration
        """
        t = Table(table_name, parent_name, self, table_path, ctr_dict)
        self.tlist.append(t)

    def add_col(self, table_name: str, col_name: str, col_value: Any) -> None:
        """Add a column to a specific table.

        Args:
            table_name: Name of the table to add column to
            col_name: Column name
            col_value: Column value
        """
        for t in self.tlist:
            if t.name == table_name:
                t.add_col(col_name, col_value)
                return

    def add_identifier(self, table_name: str, col_name: str, col_value: Any) -> None:
        """Add an identifier (primary/foreign key) to a specific table.

        Args:
            table_name: Name of the table to add identifier to
            col_name: Identifier column name
            col_value: Identifier value
        """
        for t in self.tlist:
            if t.name == table_name:
                t.add_identifier(col_name, col_value)
                return

    def close_table(self, table_name: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """Close a table and generate its output row.

        This removes the table from the active list and returns the table name
        and row data as a tuple for BigQuery insertion.

        Args:
            table_name: Name of the table to close

        Returns:
            Tuple of (table_name, row_dict) or None if table not found
        """
        for t in self.tlist:
            if t.name == table_name:
                # Generate the dict row (replaces SQL INSERT)
                row = t.to_dict()
                table_name_out = t.name

                # Remove from active list
                self.tlist.remove(t)

                return (table_name_out, row)

        return None

    def get_table(self, table_name: str) -> Optional[Table]:
        """Get a table by name.

        Args:
            table_name: Name of the table to retrieve

        Returns:
            Table object or None if not found
        """
        for t in self.tlist:
            if t.name == table_name:
                return t
        return None

    def __len__(self):
        """Return number of active tables."""
        return len(self.tlist)

    def __repr__(self):
        return f"TableList(tables={len(self.tlist)})"
