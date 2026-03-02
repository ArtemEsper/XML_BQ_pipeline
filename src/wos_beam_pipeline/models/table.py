"""Table model for storing parsed XML data.

Represents a database table with columns, identifiers, and counters.
Adapted from generic_parser.py for Beam compatibility - outputs dict rows instead of SQL.
"""

from typing import List, Dict, Any, Optional
from .column import Column


class Table:
    """Represents a database table with hierarchical foreign key management.

    The Table structure simulates a DB Table with a name, parent, columns and values.
    Identifiers are specialized columns that include the id and automated counters.
    The table maintains a list of counters for its children, allowing children to request
    the next number in that counter.
    """

    def __init__(
        self,
        name: str,
        parent_name: Optional[str],
        table_list: 'TableList',
        table_path: str,
        ctr_dict: Dict[str, str]
    ):
        """Initialize a table.

        Args:
            name: Table name
            parent_name: Parent table name (None for root table)
            table_list: Reference to the TableList managing this table
            table_path: Full XML path to this table
            ctr_dict: Counter dictionary from configuration
        """
        self.name = name
        self.columns: List[Column] = []
        self.identifiers: List[Column] = []
        self.counters: List[Column] = []
        self.parent_name = parent_name
        self.ctr_dict = ctr_dict

        # If there is a parent, inherit its identifiers and get a counter
        if parent_name is not None:
            parent = table_list.get_table(parent_name)
            if parent:
                # Inherit parent's identifiers (foreign keys)
                for identifier in parent.get_identifiers():
                    self.add_identifier(identifier.name, identifier.value)

                # Get next counter value from parent
                new_id = parent.get_counter(table_path)
                self.add_identifier(new_id.name, new_id.value)

    def add_col(self, col_name: str, col_value: Any) -> None:
        """Add a column name/value pair to the table.

        Args:
            col_name: Column name
            col_value: Column value
        """
        new_col = Column(col_name, col_value)
        self.columns.append(new_col)

    def add_identifier(self, col_name: str, col_value: Any) -> None:
        """Add a column/value to the identifier list (primary/foreign keys).

        Args:
            col_name: Identifier column name
            col_value: Identifier value
        """
        new_col = Column(col_name, col_value)
        self.identifiers.append(new_col)

    def get_counter(self, name: str) -> Column:
        """Get next value for a counter.

        Invoked by child tables. Looks for the counter name in the list,
        increments if found, or creates new counter starting at 1.

        Args:
            name: Counter name (table path)

        Returns:
            Column with counter name and value
        """
        ctr_path = name + "/ctr_id"
        if ctr_path not in self.ctr_dict:
            raise KeyError(f"Counter not found in config: {ctr_path}")

        ctr_id = self.ctr_dict[ctr_path].split(":", 1)[1]

        # Look for existing counter
        for counter in self.counters:
            if counter.name == ctr_id:
                counter.value = counter.value + 1
                return counter

        # Create new counter
        new_counter = Column(ctr_id, 1)
        self.counters.append(new_counter)
        return new_counter

    def get_identifiers(self) -> List[Column]:
        """Return the set of identifiers for the table.

        Used when a child table is created to copy down the identifiers
        for the foreign key relationship.

        Returns:
            List of identifier columns
        """
        return self.identifiers

    def to_dict(self) -> Dict[str, Any]:
        """Convert table to BigQuery-compatible dict row.

        Combines identifiers and columns into a single dict.
        This replaces the SQL INSERT generation from the original code.

        Returns:
            Dict mapping column names to values
        """
        row = {}

        # Add identifiers (primary/foreign keys)
        for col in self.identifiers:
            row[col.name] = col.value

        # Add regular columns
        for col in self.columns:
            row[col.name] = self._sanitize_value(col.value)

        return row

    @staticmethod
    def _sanitize_value(value: Any) -> Any:
        """Sanitize value for BigQuery insertion.

        Handles None values and ensures proper string formatting.

        Args:
            value: Raw value

        Returns:
            Sanitized value suitable for BigQuery
        """
        if value is None:
            return None

        # Convert to string if not already
        if not isinstance(value, (int, float, bool)):
            return str(value)

        return value

    def __repr__(self):
        return f"Table(name='{self.name}', parent='{self.parent_name}', cols={len(self.columns)}, ids={len(self.identifiers)})"
