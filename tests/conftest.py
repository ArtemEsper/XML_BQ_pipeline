"""Pytest configuration and fixtures for WoS Beam Pipeline tests."""

import pytest
import os
from pathlib import Path


@pytest.fixture
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def sample_xml():
    """Return sample WoS XML content."""
    return """<?xml version="1.0"?>
    <REC>
        <UID>WOS:000123456700001</UID>
        <static>
            <summary>
                <pub_info>
                    <pubyear>2024</pubyear>
                    <vol>123</vol>
                    <issue>4</issue>
                </pub_info>
            </summary>
        </static>
    </REC>
    """


@pytest.fixture
def sample_config():
    """Return sample WoS config."""
    return {
        'table_dict': {
            'static/summary/table': 'wos_summary:wos_summary',
        },
        'value_dict': {
            'static/summary/pub_info/pubyear/': 'wos_summary:pubyear',
            'static/summary/pub_info/vol/': 'wos_summary:vol',
        },
        'ctr_dict': {},
        'attrib_dict': {},
        'attrib_defaults': {}
    }


@pytest.fixture
def sample_schema():
    """Return sample BigQuery schema."""
    return [
        {
            'name': 'id',
            'type': 'STRING',
            'mode': 'REQUIRED',
            'description': 'WoS ID'
        },
        {
            'name': 'pubyear',
            'type': 'STRING',
            'mode': 'NULLABLE',
            'description': 'Publication year'
        },
        {
            'name': 'vol',
            'type': 'STRING',
            'mode': 'NULLABLE',
            'description': 'Volume'
        }
    ]
