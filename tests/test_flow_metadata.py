"""Tests for flow_metadata module."""

from unittest.mock import patch
from xml.etree.ElementTree import Element, SubElement

import pandas as pd
import pytest

from sdmxabs.flow_metadata import (
    FlowMetaDict,
    build_key,
    code_list_for_dim,
    code_lists,
    data_dimensions,
    data_flows,
    frame,
)


class TestDataFlows:
    """Test data_flows function."""

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_data_flows_all(self, mock_acquire_xml):
        """Test retrieving all data flows."""
        # Create mock XML structure
        root = Element("message:Structure")
        root.set("xmlns:message", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
        root.set("xmlns:str", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")
        root.set("xmlns:com", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")

        structures = SubElement(root, "message:Structures")
        dataflows = SubElement(structures, "str:DataFlows")

        # Add test dataflows
        df1 = SubElement(dataflows, "str:DataFlow")
        df1.set("id", "CPI")
        name1 = SubElement(df1, "com:Name")
        name1.text = "Consumer Price Index"

        df2 = SubElement(dataflows, "str:DataFlow")
        df2.set("id", "WPI")
        name2 = SubElement(df2, "com:Name")
        name2.text = "Wage Price Index"

        mock_acquire_xml.return_value = root

        result = data_flows()

        assert isinstance(result, dict)
        assert "CPI" in result
        assert "WPI" in result
        assert result["CPI"]["name"] == "Consumer Price Index"
        assert result["WPI"]["name"] == "Wage Price Index"

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_data_flows_specific(self, mock_acquire_xml):
        """Test retrieving specific data flow."""
        root = Element("message:Structure")
        root.set("xmlns:message", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
        root.set("xmlns:str", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")
        root.set("xmlns:com", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")

        structures = SubElement(root, "message:Structures")
        dataflows = SubElement(structures, "str:DataFlows")

        df1 = SubElement(dataflows, "str:DataFlow")
        df1.set("id", "CPI")
        name1 = SubElement(df1, "com:Name")
        name1.text = "Consumer Price Index"

        mock_acquire_xml.return_value = root

        result = data_flows(flow_id="CPI")

        assert "CPI" in result
        assert result["CPI"]["name"] == "Consumer Price Index"

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_data_flows_empty_response(self, mock_acquire_xml):
        """Test handling of empty dataflows response."""
        root = Element("message:Structure")
        mock_acquire_xml.return_value = root

        result = data_flows()

        assert isinstance(result, dict)
        assert len(result) == 0


class TestDataDimensions:
    """Test data_dimensions function."""

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_data_dimensions_success(self, mock_acquire_xml):
        """Test successful data dimensions retrieval."""
        root = Element("message:Structure")
        root.set("xmlns:message", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
        root.set("xmlns:str", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")
        root.set("xmlns:com", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")

        structures = SubElement(root, "message:Structures")
        dsds = SubElement(structures, "str:DataStructures")
        dsd = SubElement(dsds, "str:DataStructure")
        dsd.set("id", "CPI_DSD")

        dsd_components = SubElement(dsd, "str:DataStructureComponents")
        dim_list = SubElement(dsd_components, "str:DimensionList")

        # Add dimensions
        dim1 = SubElement(dim_list, "str:Dimension")
        dim1.set("id", "FREQ")
        dim1.set("position", "1")
        local_rep1 = SubElement(dim1, "str:LocalRepresentation")
        enum1 = SubElement(local_rep1, "str:Enumeration")
        ref1 = SubElement(enum1, "Ref")
        ref1.set("id", "CL_FREQ")
        ref1.set("package", "codelist")

        mock_acquire_xml.return_value = root

        result = data_dimensions("CPI")

        assert isinstance(result, dict)
        assert "FREQ" in result
        assert result["FREQ"]["id"] == "CL_FREQ"
        assert result["FREQ"]["package"] == "codelist"
        assert result["FREQ"]["position"] == "1"


class TestCodeLists:
    """Test code_lists function."""

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_code_lists_success(self, mock_acquire_xml):
        """Test successful code lists retrieval."""
        root = Element("message:Structure")
        root.set("xmlns:message", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
        root.set("xmlns:str", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")
        root.set("xmlns:com", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")

        structures = SubElement(root, "message:Structures")
        codelists = SubElement(structures, "str:Codelists")
        codelist = SubElement(codelists, "str:Codelist")
        codelist.set("id", "CL_FREQ")

        # Add codes
        code1 = SubElement(codelist, "str:Code")
        code1.set("id", "Q")
        name1 = SubElement(code1, "com:Name")
        name1.text = "Quarterly"

        code2 = SubElement(codelist, "str:Code")
        code2.set("id", "M")
        name2 = SubElement(code2, "com:Name")
        name2.text = "Monthly"

        mock_acquire_xml.return_value = root

        result = code_lists("CL_FREQ")

        assert isinstance(result, dict)
        assert "Q" in result
        assert "M" in result
        assert result["Q"]["name"] == "Quarterly"
        assert result["M"]["name"] == "Monthly"

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_code_lists_with_parent(self, mock_acquire_xml):
        """Test code lists with parent relationships."""
        root = Element("message:Structure")
        root.set("xmlns:message", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
        root.set("xmlns:str", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")
        root.set("xmlns:com", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")

        structures = SubElement(root, "message:Structures")
        codelists = SubElement(structures, "str:Codelists")
        codelist = SubElement(codelists, "str:Codelist")
        codelist.set("id", "CL_REGION")

        code1 = SubElement(codelist, "str:Code")
        code1.set("id", "AUS")
        name1 = SubElement(code1, "com:Name")
        name1.text = "Australia"

        code2 = SubElement(codelist, "str:Code")
        code2.set("id", "NSW")
        code2.set("parent", "AUS")
        name2 = SubElement(code2, "com:Name")
        name2.text = "New South Wales"

        mock_acquire_xml.return_value = root

        result = code_lists("CL_REGION")

        assert "NSW" in result
        assert result["NSW"]["parent"] == "AUS"
        assert "parent" not in result["AUS"]


class TestCodeListForDim:
    """Test code_list_for_dim function."""

    @patch("sdmxabs.flow_metadata.data_dimensions")
    @patch("sdmxabs.flow_metadata.code_lists")
    def test_code_list_for_dim_success(self, mock_code_lists, mock_data_dimensions):
        """Test successful code list retrieval for dimension."""
        mock_data_dimensions.return_value = {"FREQ": {"id": "CL_FREQ", "package": "codelist"}}
        mock_code_lists.return_value = {"Q": {"name": "Quarterly"}, "M": {"name": "Monthly"}}

        result = code_list_for_dim("CPI", "FREQ")

        assert result == {"Q": {"name": "Quarterly"}, "M": {"name": "Monthly"}}
        mock_data_dimensions.assert_called_once_with("CPI")
        mock_code_lists.assert_called_once_with("CL_FREQ")

    @patch("sdmxabs.flow_metadata.data_dimensions")
    def test_code_list_for_dim_missing_dimension(self, mock_data_dimensions):
        """Test handling of missing dimension."""
        mock_data_dimensions.return_value = {}

        with pytest.raises(KeyError):
            code_list_for_dim("CPI", "NONEXISTENT")


class TestBuildKey:
    """Test build_key function."""

    def test_build_key_no_dimensions(self):
        """Test build_key with no dimensions."""
        result = build_key("CPI", None)
        assert result == "all"

    def test_build_key_empty_dimensions(self):
        """Test build_key with empty dimensions dict."""
        result = build_key("CPI", {})
        assert result == "all"

    @patch("sdmxabs.flow_metadata.data_dimensions")
    def test_build_key_with_dimensions(self, mock_data_dimensions):
        """Test build_key with valid dimensions."""
        mock_data_dimensions.return_value = {
            "FREQ": {"position": "1"},
            "REGION": {"position": "2"},
            "MEASURE": {"position": "3"},
        }

        dimensions = {"FREQ": "Q", "REGION": "AUS", "MEASURE": "1"}
        result = build_key("CPI", dimensions)

        assert result == "Q.AUS.1"

    @patch("sdmxabs.flow_metadata.data_dimensions")
    def test_build_key_partial_dimensions(self, mock_data_dimensions):
        """Test build_key with partial dimensions."""
        mock_data_dimensions.return_value = {
            "FREQ": {"position": "1"},
            "REGION": {"position": "2"},
            "MEASURE": {"position": "3"},
        }

        dimensions = {"FREQ": "Q", "MEASURE": "1"}  # Missing REGION
        result = build_key("CPI", dimensions)

        assert result == "Q..1"  # Empty string for missing dimension

    @patch("sdmxabs.flow_metadata.data_dimensions")
    def test_build_key_multiple_values(self, mock_data_dimensions):
        """Test build_key with multiple values for dimension."""
        mock_data_dimensions.return_value = {"FREQ": {"position": "1"}, "REGION": {"position": "2"}}

        dimensions = {"FREQ": "Q+M", "REGION": "AUS"}
        result = build_key("CPI", dimensions)

        assert result == "Q+M.AUS"

    @patch("sdmxabs.flow_metadata.data_dimensions")
    def test_build_key_with_validation(self, mock_data_dimensions):
        """Test build_key with validation enabled."""
        mock_data_dimensions.return_value = {"FREQ": {"position": "1"}, "REGION": {"position": "2"}}

        dimensions = {"FREQ": "Q", "INVALID_DIM": "value"}

        with pytest.warns(UserWarning, match="not found in flow dimensions"):
            result = build_key("CPI", dimensions, validate=True)

        assert result == "Q."


class TestFrame:
    """Test frame function."""

    def test_frame_conversion(self):
        """Test conversion of FlowMetaDict to DataFrame."""
        flow_meta: FlowMetaDict = {
            "CPI": {"name": "Consumer Price Index", "agency": "ABS"},
            "WPI": {"name": "Wage Price Index", "agency": "ABS"},
        }

        result = frame(flow_meta)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert "CPI" in result.index
        assert "WPI" in result.index
        assert result.loc["CPI", "name"] == "Consumer Price Index"
        assert result.loc["WPI", "name"] == "Wage Price Index"

    def test_frame_empty_dict(self):
        """Test frame with empty dictionary."""
        result = frame({})

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_frame_single_item(self):
        """Test frame with single item."""
        flow_meta: FlowMetaDict = {"CPI": {"name": "Consumer Price Index"}}

        result = frame(flow_meta)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.loc["CPI", "name"] == "Consumer Price Index"


class TestIntegration:
    """Integration tests for flow_metadata module."""

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_full_workflow(self, mock_acquire_xml):
        """Test a full workflow from data_flows to code_lists."""

        # Mock the XML responses for different calls
        def side_effect(url, **kwargs):
            if "dataflow" in url:
                # Return dataflows structure
                root = Element("message:Structure")
                structures = SubElement(root, "message:Structures")
                dataflows = SubElement(structures, "str:DataFlows")
                df = SubElement(dataflows, "str:DataFlow")
                df.set("id", "CPI")
                name = SubElement(df, "com:Name")
                name.text = "Consumer Price Index"
                return root
            if "datastructure" in url:
                # Return data structure
                root = Element("message:Structure")
                structures = SubElement(root, "message:Structures")
                dsds = SubElement(structures, "str:DataStructures")
                dsd = SubElement(dsds, "str:DataStructure")
                dsd_components = SubElement(dsd, "str:DataStructureComponents")
                dim_list = SubElement(dsd_components, "str:DimensionList")
                dim = SubElement(dim_list, "str:Dimension")
                dim.set("id", "FREQ")
                return root
            # Return codelist
            root = Element("message:Structure")
            structures = SubElement(root, "message:Structures")
            codelists = SubElement(structures, "str:Codelists")
            codelist = SubElement(codelists, "str:Codelist")
            code = SubElement(codelist, "str:Code")
            code.set("id", "Q")
            name = SubElement(code, "com:Name")
            name.text = "Quarterly"
            return root

        mock_acquire_xml.side_effect = side_effect

        # Test the workflow
        flows = data_flows()
        assert "CPI" in flows

        dims = data_dimensions("CPI")
        assert isinstance(dims, dict)

        codes = code_lists("CL_FREQ")
        assert isinstance(codes, dict)
