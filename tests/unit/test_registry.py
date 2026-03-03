"""Unit tests for registry utility functions (mocked BQ client)."""

import unittest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

from wos_beam_pipeline.utils.registry import (
    load_record_registry,
    check_file_registry,
    register_file,
    cleanup_changed_records,
    _parse_dataset_id,
)


_PROJECT = 'test-project'
_DATASET = 'test-project:test_dataset'
_DATASET_ID = 'test_dataset'


class TestParseDatasetId(unittest.TestCase):
    def test_project_colon_dataset(self):
        self.assertEqual(_parse_dataset_id('proj:ds'), 'ds')

    def test_bare_dataset(self):
        self.assertEqual(_parse_dataset_id('my_dataset'), 'my_dataset')


class TestLoadRecordRegistry(unittest.TestCase):

    @patch('wos_beam_pipeline.utils.registry.bigquery.Client')
    def test_returns_empty_dict_when_table_not_found(self, mock_client_cls):
        from google.cloud.exceptions import NotFound
        mock_client = mock_client_cls.return_value
        mock_client.get_table.side_effect = NotFound('table not found')

        result = load_record_registry(_DATASET, _PROJECT)

        self.assertEqual(result, {})

    @patch('wos_beam_pipeline.utils.registry.bigquery.Client')
    def test_returns_uid_hash_mapping(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.get_table.return_value = MagicMock()  # table exists

        # Simulate query result rows
        row1 = MagicMock()
        row1.uid = 'UID-001'
        row1.record_hash = 'hash_aaa'
        row2 = MagicMock()
        row2.uid = 'UID-002'
        row2.record_hash = 'hash_bbb'

        mock_query_job = MagicMock()
        mock_query_job.result.return_value = iter([row1, row2])
        mock_client.query.return_value = mock_query_job

        result = load_record_registry(_DATASET, _PROJECT)

        self.assertEqual(result, {'UID-001': 'hash_aaa', 'UID-002': 'hash_bbb'})


class TestCheckFileRegistry(unittest.TestCase):

    @patch('wos_beam_pipeline.utils.registry.bigquery.Client')
    def test_returns_false_when_table_not_found(self, mock_client_cls):
        from google.cloud.exceptions import NotFound
        mock_client = mock_client_cls.return_value
        mock_client.get_table.side_effect = NotFound('not found')

        result = check_file_registry(
            'gs://bucket/file.xml', 'md5abc', _DATASET, _PROJECT
        )
        self.assertFalse(result)

    @patch('wos_beam_pipeline.utils.registry.bigquery.Client')
    def test_returns_true_when_file_already_processed(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.get_table.return_value = MagicMock()

        count_row = MagicMock()
        count_row.cnt = 1
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = iter([count_row])
        mock_client.query.return_value = mock_query_job

        result = check_file_registry(
            'gs://bucket/file.xml', 'md5abc', _DATASET, _PROJECT
        )
        self.assertTrue(result)

    @patch('wos_beam_pipeline.utils.registry.bigquery.Client')
    def test_returns_false_when_file_not_found_in_registry(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.get_table.return_value = MagicMock()

        count_row = MagicMock()
        count_row.cnt = 0
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = iter([count_row])
        mock_client.query.return_value = mock_query_job

        result = check_file_registry(
            'gs://bucket/new_file.xml', 'md5xyz', _DATASET, _PROJECT
        )
        self.assertFalse(result)


class TestCleanupChangedRecords(unittest.TestCase):

    @patch('wos_beam_pipeline.utils.registry.bigquery.Client')
    def test_no_op_when_no_uids(self, mock_client_cls):
        mock_client = mock_client_cls.return_value

        cleanup_changed_records(
            [], _DATASET, ['wos_summary', 'wos_page'], _PROJECT,
            '2026-03-03T00:00:00+00:00'
        )

        mock_client.query.assert_not_called()

    @patch('wos_beam_pipeline.utils.registry.bigquery.Client')
    def test_issues_delete_per_table(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = MagicMock()
        mock_client.query.return_value = mock_query_job

        table_names = ['wos_summary', 'wos_page', 'wos_author']
        cleanup_changed_records(
            ['UID-001', 'UID-002'],
            _DATASET,
            table_names,
            _PROJECT,
            '2026-03-03T00:00:00+00:00'
        )

        # One DELETE query per table
        self.assertEqual(mock_client.query.call_count, len(table_names))

        # Verify DELETE keyword appears in each query
        for call_args in mock_client.query.call_args_list:
            sql = call_args[0][0]
            self.assertIn('DELETE FROM', sql)
            self.assertIn('UNNEST(@changed_uids)', sql)
            self.assertIn('ingestion_ts <', sql)

    @patch('wos_beam_pipeline.utils.registry.bigquery.Client')
    def test_continues_after_table_error(self, mock_client_cls):
        """Cleanup logs errors but processes remaining tables."""
        mock_client = mock_client_cls.return_value

        def query_side_effect(sql, job_config=None):
            if 'wos_page' in sql:
                raise Exception("BQ error")
            mock_job = MagicMock()
            mock_job.result.return_value = MagicMock()
            return mock_job

        mock_client.query.side_effect = query_side_effect

        # Should not raise even though wos_page fails
        cleanup_changed_records(
            ['UID-X'],
            _DATASET,
            ['wos_summary', 'wos_page', 'wos_author'],
            _PROJECT,
            '2026-03-03T00:00:00+00:00'
        )

        self.assertEqual(mock_client.query.call_count, 3)


class TestRegisterFile(unittest.TestCase):

    @patch('wos_beam_pipeline.utils.registry.bigquery.Client')
    def test_executes_merge_query(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = MagicMock()
        mock_client.query.return_value = mock_query_job

        register_file(
            'gs://bucket/file.xml', 'md5abc', 1234,
            _DATASET, _PROJECT, '2026-03-03T00:00:00+00:00'
        )

        self.assertEqual(mock_client.query.call_count, 1)
        sql = mock_client.query.call_args[0][0]
        self.assertIn('MERGE', sql)
        self.assertIn('wos_file_registry', sql)


if __name__ == '__main__':
    unittest.main()
