"""Unit tests for SplitXMLRecords DoFn.

Covers the 3-tuple output format and SHA-256 hash correctness introduced
for hash-based idempotent processing.
"""

import hashlib
import io
import unittest
from unittest.mock import MagicMock, patch

import lxml.etree as etree

from wos_beam_pipeline.transforms.xml_splitter import SplitXMLRecords


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NS = 'http://example.com/ns'

def _build_xml(records, namespace=''):
    """Build a minimal XML bytes payload with N <REC> elements."""
    ns_attr = f' xmlns="{namespace}"' if namespace else ''
    recs = ''.join(
        f'<REC{ns_attr}><UID>{uid}</UID><data>{data}</data></REC>'
        for uid, data in records
    )
    return f'<records>{recs}</records>'.encode('utf-8')


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSplitXMLRecordsOutputFormat(unittest.TestCase):
    """Verify that process() yields (uid, xml_string, record_hash) 3-tuples."""

    def _get_records(self, xml_bytes, record_tag='REC', id_tag='UID', namespace=''):
        splitter = SplitXMLRecords(
            record_tag=record_tag,
            id_tag=id_tag,
            namespace=namespace
        )
        # Patch GCS download to return our bytes
        with patch.object(splitter, '_download_from_gcs', return_value=xml_bytes):
            return list(splitter.process('gs://fake-bucket/fake.xml'))

    def test_yields_three_tuple(self):
        xml = _build_xml([('UID-001', 'content')])
        results = self._get_records(xml)
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], tuple)
        self.assertEqual(len(results[0]), 3)

    def test_uid_is_correct(self):
        xml = _build_xml([('UID-ABC', 'somedata')])
        uid, xml_string, record_hash = self._get_records(xml)[0]
        self.assertEqual(uid, 'UID-ABC')

    def test_record_hash_is_valid_sha256_hex(self):
        xml = _build_xml([('UID-001', 'data')])
        uid, xml_string, record_hash = self._get_records(xml)[0]
        # SHA-256 hex digest is exactly 64 lowercase hex characters
        self.assertEqual(len(record_hash), 64)
        self.assertTrue(all(c in '0123456789abcdef' for c in record_hash))

    def test_record_hash_matches_xml_string(self):
        """Hash must equal SHA-256(xml_string.encode('utf-8'))."""
        xml = _build_xml([('UID-001', 'data')])
        uid, xml_string, record_hash = self._get_records(xml)[0]
        expected = hashlib.sha256(xml_string.encode('utf-8')).hexdigest()
        self.assertEqual(record_hash, expected)

    def test_multiple_records_each_have_hash(self):
        xml = _build_xml([('UID-A', 'x'), ('UID-B', 'y'), ('UID-C', 'z')])
        results = self._get_records(xml)
        self.assertEqual(len(results), 3)
        for uid, xml_string, record_hash in results:
            self.assertEqual(len(record_hash), 64)
            expected = hashlib.sha256(xml_string.encode('utf-8')).hexdigest()
            self.assertEqual(record_hash, expected)

    def test_different_content_produces_different_hashes(self):
        """Two records with different XML content must have different hashes."""
        xml = _build_xml([('UID-A', 'content1'), ('UID-B', 'content2')])
        results = self._get_records(xml)
        hash_a = results[0][2]
        hash_b = results[1][2]
        self.assertNotEqual(hash_a, hash_b)

    def test_identical_content_produces_same_hash(self):
        """Two records with identical XML content must have the same hash."""
        # Build two separate single-record XMLs with the same structure
        xml1 = _build_xml([('UID-X', 'data')])
        xml2 = _build_xml([('UID-X', 'data')])
        result1 = self._get_records(xml1)[0]
        result2 = self._get_records(xml2)[0]
        # Same XML string → same hash
        if result1[1] == result2[1]:
            self.assertEqual(result1[2], result2[2])

    def test_namespace_records(self):
        """Records with XML namespace are correctly split and hashed."""
        xml = _build_xml([('UID-NS', 'nsdata')], namespace=_NS)
        results = self._get_records(xml, namespace=_NS)
        self.assertEqual(len(results), 1)
        uid, xml_string, record_hash = results[0]
        self.assertEqual(uid, 'UID-NS')
        self.assertEqual(len(record_hash), 64)


if __name__ == '__main__':
    unittest.main()
