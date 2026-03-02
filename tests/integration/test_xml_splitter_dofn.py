"""Integration tests for XML Splitter DoFn using Beam TestPipeline."""

import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from wos_beam_pipeline.transforms import SplitXMLRecords


class TestXMLSplitterDoFn:
    """Integration tests for SplitXMLRecords DoFn."""

    @pytest.mark.skip(reason="Requires GCS setup")
    def test_split_xml_records(self):
        """Test that XML file is correctly split into records."""
        # This would require actual GCS file or mocking
        # Example structure:
        with TestPipeline() as p:
            # Mock input
            gcs_path = "gs://bucket/test.xml"

            # Process
            records = (
                p
                | beam.Create([gcs_path])
                | beam.ParDo(SplitXMLRecords(
                    record_tag='REC',
                    id_tag='UID',
                    namespace=''
                ))
            )

            # Verify - would check that we get expected records
            # assert_that(records, ...)
