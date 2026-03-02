"""Unit tests for TableList and Table classes."""

import pytest
from wos_beam_pipeline.models import TableList, Table, Column


class TestColumn:
    """Test Column class."""

    def test_column_initialization(self):
        """Test Column can be initialized with name and value."""
        col = Column("test_col", "test_value")
        assert col.name == "test_col"
        assert col.value == "test_value"

    def test_column_to_dict(self):
        """Test Column converts to dict."""
        col = Column("id", 123)
        assert col.to_dict() == {'name': 'id', 'value': 123}


class TestTable:
    """Test Table class."""

    def test_table_initialization(self):
        """Test Table can be initialized."""
        table_list = TableList()
        ctr_dict = {}
        table = Table("test_table", None, table_list, "/test/path", ctr_dict)

        assert table.name == "test_table"
        assert table.parent_name is None
        assert len(table.columns) == 0
        assert len(table.identifiers) == 0

    def test_add_col(self):
        """Test adding columns to table."""
        table_list = TableList()
        ctr_dict = {}
        table = Table("test_table", None, table_list, "/test/path", ctr_dict)

        table.add_col("col1", "value1")
        table.add_col("col2", "value2")

        assert len(table.columns) == 2
        assert table.columns[0].name == "col1"
        assert table.columns[0].value == "value1"

    def test_add_identifier(self):
        """Test adding identifiers to table."""
        table_list = TableList()
        ctr_dict = {}
        table = Table("test_table", None, table_list, "/test/path", ctr_dict)

        table.add_identifier("id", "123")

        assert len(table.identifiers) == 1
        assert table.identifiers[0].name == "id"
        assert table.identifiers[0].value == "123"

    def test_to_dict(self):
        """Test converting table to dict row."""
        table_list = TableList()
        ctr_dict = {}
        table = Table("test_table", None, table_list, "/test/path", ctr_dict)

        table.add_identifier("id", "123")
        table.add_col("name", "test")
        table.add_col("value", 456)

        row = table.to_dict()

        assert row["id"] == "123"
        assert row["name"] == "test"
        assert row["value"] == 456


class TestTableList:
    """Test TableList class."""

    def test_tablelist_initialization(self):
        """Test TableList can be initialized."""
        table_list = TableList()
        assert len(table_list) == 0

    def test_add_table(self):
        """Test adding tables to list."""
        table_list = TableList()
        ctr_dict = {}

        table_list.add_table("table1", None, "/path1", ctr_dict)
        table_list.add_table("table2", None, "/path2", ctr_dict)

        assert len(table_list) == 2

    def test_add_col_to_table(self):
        """Test adding columns to specific table."""
        table_list = TableList()
        ctr_dict = {}

        table_list.add_table("table1", None, "/path1", ctr_dict)
        table_list.add_col("table1", "col1", "value1")

        table = table_list.get_table("table1")
        assert len(table.columns) == 1
        assert table.columns[0].name == "col1"

    def test_close_table(self):
        """Test closing table returns row and removes from list."""
        table_list = TableList()
        ctr_dict = {}

        table_list.add_table("table1", None, "/path1", ctr_dict)
        table_list.add_identifier("table1", "id", "123")
        table_list.add_col("table1", "name", "test")

        assert len(table_list) == 1

        result = table_list.close_table("table1")

        assert result is not None
        table_name, row = result
        assert table_name == "table1"
        assert row["id"] == "123"
        assert row["name"] == "test"
        assert len(table_list) == 0
