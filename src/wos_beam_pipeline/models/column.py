"""Column model for table data storage.

A simple storage for a column in a table entry (name and value).
Adapted from generic_parser.py for Beam compatibility.
"""


class Column:
    """Represents a single column with name and value."""

    def __init__(self, name, value):
        """Initialize a column.

        Args:
            name: Column name
            value: Column value (can be any type)
        """
        self.name = name
        self.value = value

    def to_dict(self):
        """Convert column to dictionary representation.

        Returns:
            Dict with name and value keys
        """
        return {'name': self.name, 'value': self.value}

    def __repr__(self):
        return f"Column(name='{self.name}', value='{self.value}')"
