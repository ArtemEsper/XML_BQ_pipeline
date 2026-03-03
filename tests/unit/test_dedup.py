"""Unit tests for FilterUnchangedRecords DoFn.

Tests call process() directly on the DoFn rather than constructing a full
Beam pipeline, which avoids serialization and side-effect pitfalls in unit tests.
"""

import unittest
from apache_beam import pvalue

from wos_beam_pipeline.transforms.dedup import FilterUnchangedRecords


def _run_process(element, registry):
    """Invoke FilterUnchangedRecords.process() and return (new, changed, unchanged) lists."""
    doFn = FilterUnchangedRecords()
    outputs = list(doFn.process(element, registry=registry))
    new_out = [o.value for o in outputs if o.tag == FilterUnchangedRecords.OUTPUT_TAG_NEW]
    changed_out = [o.value for o in outputs if o.tag == FilterUnchangedRecords.OUTPUT_TAG_CHANGED]
    unchanged_out = [o.value for o in outputs if o.tag == FilterUnchangedRecords.OUTPUT_TAG_UNCHANGED]
    return new_out, changed_out, unchanged_out


class TestFilterUnchangedRecordsOutputTags(unittest.TestCase):
    """Verify the class-level output tag constants."""

    def test_output_tag_values(self):
        self.assertEqual(FilterUnchangedRecords.OUTPUT_TAG_NEW, 'new')
        self.assertEqual(FilterUnchangedRecords.OUTPUT_TAG_CHANGED, 'changed')
        self.assertEqual(FilterUnchangedRecords.OUTPUT_TAG_UNCHANGED, 'unchanged')


class TestFilterUnchangedRecords(unittest.TestCase):
    """Tests for the FilterUnchangedRecords DoFn routing logic."""

    def test_new_record_not_in_registry(self):
        """UID not present in registry → routed to NEW."""
        element = ('UID-001', '<REC/>', 'hash_aaa')
        new_out, changed_out, unchanged_out = _run_process(element, registry={})

        self.assertEqual(len(new_out), 1)
        self.assertEqual(len(changed_out), 0)
        self.assertEqual(len(unchanged_out), 0)
        self.assertEqual(new_out[0], element)

    def test_unchanged_record_same_hash(self):
        """UID in registry with matching hash → routed to UNCHANGED."""
        element = ('UID-001', '<REC>content</REC>', 'hash_aaa')
        registry = {'UID-001': 'hash_aaa'}
        new_out, changed_out, unchanged_out = _run_process(element, registry=registry)

        self.assertEqual(len(new_out), 0)
        self.assertEqual(len(changed_out), 0)
        self.assertEqual(len(unchanged_out), 1)
        self.assertEqual(unchanged_out[0], element)

    def test_changed_record_different_hash(self):
        """UID in registry with differing hash → routed to CHANGED."""
        element = ('UID-001', '<REC>updated</REC>', 'hash_new')
        registry = {'UID-001': 'hash_old'}
        new_out, changed_out, unchanged_out = _run_process(element, registry=registry)

        self.assertEqual(len(new_out), 0)
        self.assertEqual(len(changed_out), 1)
        self.assertEqual(len(unchanged_out), 0)
        self.assertEqual(changed_out[0], element)
        # Verify the new hash is preserved in the output tuple
        self.assertEqual(changed_out[0][2], 'hash_new')

    def test_output_is_tagged_output(self):
        """process() must yield pvalue.TaggedOutput objects."""
        element = ('UID-X', '<REC/>', 'h' * 64)
        doFn = FilterUnchangedRecords()
        outputs = list(doFn.process(element, registry={}))
        self.assertEqual(len(outputs), 1)
        self.assertIsInstance(outputs[0], pvalue.TaggedOutput)

    def test_output_preserves_full_3_tuple(self):
        """Routed element must be the original (uid, xml_string, record_hash) 3-tuple."""
        xml = '<REC><UID>Z</UID></REC>'
        h = 'a' * 64
        element = ('UID-Z', xml, h)
        new_out, _, _ = _run_process(element, registry={})

        self.assertEqual(len(new_out), 1)
        uid, xml_out, hash_out = new_out[0]
        self.assertEqual(uid, 'UID-Z')
        self.assertEqual(xml_out, xml)
        self.assertEqual(hash_out, h)

    def test_exactly_one_output_per_element(self):
        """Each call to process() must yield exactly one TaggedOutput."""
        doFn = FilterUnchangedRecords()
        cases = [
            (('UID-A', '<R/>', 'h1'), {}),                      # new
            (('UID-B', '<R/>', 'h2'), {'UID-B': 'h2'}),         # unchanged
            (('UID-C', '<R/>', 'h3_new'), {'UID-C': 'h3_old'}), # changed
        ]
        for element, registry in cases:
            with self.subTest(element=element):
                outputs = list(doFn.process(element, registry=registry))
                self.assertEqual(len(outputs), 1)

    def test_multiple_elements_route_independently(self):
        """Processing multiple elements one-by-one gives correct routing for each."""
        registry = {
            'UID-CHG': 'old_hash',
            'UID-SAME': 'same_hash',
        }
        cases = [
            ('UID-NEW',  '<R/>', 'new_hash',  'new'),
            ('UID-CHG',  '<R/>', 'new_hash',  'changed'),
            ('UID-SAME', '<R/>', 'same_hash', 'unchanged'),
        ]
        for uid, xml, h, expected_tag in cases:
            with self.subTest(uid=uid):
                element = (uid, xml, h)
                doFn = FilterUnchangedRecords()
                outputs = list(doFn.process(element, registry=registry))
                self.assertEqual(len(outputs), 1)
                self.assertEqual(outputs[0].tag, expected_tag)


if __name__ == '__main__':
    unittest.main()
