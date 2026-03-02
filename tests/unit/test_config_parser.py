"""Unit tests for config parser."""

import pytest
from wos_beam_pipeline.utils import WosConfig, parse_config_xml
import io


class TestConfigParser:
    """Test config parser functions."""

    def test_wos_config_initialization(self):
        """Test WosConfig can be initialized."""
        config = WosConfig(
            table_dict={"path1": "table1"},
            value_dict={"path2": "value2"},
            ctr_dict={"path3": "ctr3"},
            attrib_dict={"path4": "attrib4"},
            attrib_defaults={}
        )

        assert len(config.table_dict) == 1
        assert config.table_dict["path1"] == "table1"

    def test_parse_simple_config(self):
        """Test parsing a simple config XML."""
        xml_content = b"""<?xml version="1.0"?>
        <config>
            <summary table="wos_summary:wos_summary">
                <pubyear>wos_summary:pubyear</pubyear>
            </summary>
        </config>
        """

        config = parse_config_xml(io.BytesIO(xml_content), namespace='')

        assert "config/summary/table" in config.table_dict
        assert config.table_dict["config/summary/table"] == "wos_summary:wos_summary"
        assert "config/summary/pubyear/" in config.value_dict
        assert config.value_dict["config/summary/pubyear/"] == "wos_summary:pubyear"
