"""Deduplication DoFn for hash-based idempotent record processing.

Filters records against a registry of previously-seen record hashes,
routing new and changed records to downstream parsing and discarding
unchanged records to avoid redundant writes.
"""

import apache_beam as beam
from apache_beam import pvalue
from typing import Dict, Iterator, Tuple
import logging


logger = logging.getLogger(__name__)


class FilterUnchangedRecords(beam.DoFn):
    """DoFn to filter records based on content hash comparison.

    Compares each record's SHA-256 hash against a registry side input.
    Routes records to three output tags:
      - 'new'       — UID not present in registry
      - 'changed'   — UID present but hash differs (content updated)
      - 'unchanged' — UID present and hash matches (skip entirely)

    Emits Beam metrics for observability.

    Args (side input):
        registry: Dict mapping uid -> record_hash from wos_record_registry
    """

    OUTPUT_TAG_NEW = 'new'
    OUTPUT_TAG_CHANGED = 'changed'
    OUTPUT_TAG_UNCHANGED = 'unchanged'

    def __init__(self):
        self._new_counter = beam.metrics.Metrics.counter(
            'dedup', 'new_record_count'
        )
        self._changed_counter = beam.metrics.Metrics.counter(
            'dedup', 'changed_record_count'
        )
        self._unchanged_counter = beam.metrics.Metrics.counter(
            'dedup', 'unchanged_record_count'
        )

    def process(
        self,
        element: Tuple[str, str, str],
        registry: Dict[str, str]
    ) -> Iterator[pvalue.TaggedOutput]:
        """Classify a record as new, changed, or unchanged.

        Args:
            element: 3-tuple of (uid, xml_string, record_hash)
            registry: Side input dict mapping uid -> previously-stored hash

        Yields:
            TaggedOutput with tag NEW, CHANGED, or UNCHANGED
        """
        uid, xml_string, record_hash = element

        if uid not in registry:
            self._new_counter.inc()
            logger.debug(f"New record: {uid}")
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_NEW, element)

        elif registry[uid] != record_hash:
            self._changed_counter.inc()
            logger.debug(f"Changed record: {uid}")
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_CHANGED, element)

        else:
            self._unchanged_counter.inc()
            logger.debug(f"Unchanged record (skipped): {uid}")
            yield pvalue.TaggedOutput(self.OUTPUT_TAG_UNCHANGED, element)
