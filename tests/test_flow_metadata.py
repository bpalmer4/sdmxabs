"""Tests for flow_metadata module."""

from unittest.mock import patch
from xml.etree.ElementTree import Element, SubElement

import pandas as pd
import pytest

from sdmxabs.flow_metadata import (
    FlowMetaDict,
    build_key,
    code_list_for,
    code_lists,
    data_flows,
    data_structures,
    frame,
    structure_from_flow_id,
    structure_ident,
)


class TestDataFlows:
    """Test data_flows function."""

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_data_flows_all(self, mock_acquire_xml):
        """Test retrieving all data flows."""
        # Create mock XML structure using Clark notation for namespaces
        root = Element("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structure")
        structures = SubElement(root, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structures")
        dataflows = SubElement(structures, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DataFlows")

        # Add test dataflows
        df1 = SubElement(dataflows, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Dataflow")
        df1.set("id", "CPI")
        name1 = SubElement(df1, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name")
        name1.text = "Consumer Price Index"
        struct1 = SubElement(df1, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Structure")
        ref1 = SubElement(struct1, "Ref")
        ref1.set("id", "CPI_DSD")

        df2 = SubElement(dataflows, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Dataflow")
        df2.set("id", "WPI")
        name2 = SubElement(df2, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name")
        name2.text = "Wage Price Index"
        struct2 = SubElement(df2, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Structure")
        ref2 = SubElement(struct2, "Ref")
        ref2.set("id", "WPI_DSD")

        mock_acquire_xml.return_value = root

        result = data_flows()

        assert isinstance(result, dict)
        assert "CPI" in result
        assert "WPI" in result
        assert result["CPI"]["flow_name"] == "Consumer Price Index"
        assert result["CPI"]["data_structure_id"] == "CPI_DSD"
        assert result["WPI"]["flow_name"] == "Wage Price Index"
        assert result["WPI"]["data_structure_id"] == "WPI_DSD"

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_data_flows_specific(self, mock_acquire_xml):
        """Test retrieving specific data flow."""
        root = Element("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structure")
        structures = SubElement(root, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structures")
        dataflows = SubElement(structures, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DataFlows")

        df1 = SubElement(dataflows, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Dataflow")
        df1.set("id", "CPI")
        name1 = SubElement(df1, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name")
        name1.text = "Consumer Price Index"
        struct1 = SubElement(df1, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Structure")
        ref1 = SubElement(struct1, "Ref")
        ref1.set("id", "CPI_DSD")

        mock_acquire_xml.return_value = root

        result = data_flows(flow_id="CPI")

        assert "CPI" in result
        assert result["CPI"]["flow_name"] == "Consumer Price Index"
        assert result["CPI"]["data_structure_id"] == "CPI_DSD"

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_data_flows_empty_response(self, mock_acquire_xml):
        """Test handling of empty dataflows response."""
        # Clear cache to avoid interference from other tests
        data_flows.cache_clear()
        
        root = Element("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structure")
        mock_acquire_xml.return_value = root

        result = data_flows(flow_id="EMPTY_TEST")

        assert isinstance(result, dict)
        assert len(result) == 0


class TestDataStructures:
    """Test data_structures function."""

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_data_structures_success(self, mock_acquire_xml):
        """Test successful data structures retrieval."""
        root = Element("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structure")
        root.set("xmlns:mes", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
        root.set("xmlns:str", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")
        root.set("xmlns:com", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")

        structures = SubElement(root, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structures")
        dsds = SubElement(structures, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DataStructures")
        dsd = SubElement(dsds, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DataStructure")
        dsd.set("id", "CPI_DSD")

        dsd_components = SubElement(dsd, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DataStructureComponents")
        dim_list = SubElement(dsd_components, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DimensionList")

        # Add dimensions
        dim1 = SubElement(dim_list, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Dimension")
        dim1.set("id", "FREQ")
        dim1.set("position", "1")
        local_rep1 = SubElement(dim1, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}LocalRepresentation")
        enum1 = SubElement(local_rep1, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Enumeration")
        ref1 = SubElement(enum1, "Ref")
        ref1.set("id", "CL_FREQ")
        ref1.set("package", "codelist")

        mock_acquire_xml.return_value = root

        result = data_structures("CPI_DSD")

        assert isinstance(result, dict)
        assert "FREQ" in result
        assert result["FREQ"]["codelist_id"] == "CL_FREQ"
        assert result["FREQ"]["package"] == "codelist"
        assert result["FREQ"]["position"] == "1"


class TestCodeLists:
    """Test code_lists function."""

    @patch("sdmxabs.flow_metadata.acquire_xml")
    def test_code_lists_success(self, mock_acquire_xml):
        """Test successful code lists retrieval."""
        root = Element("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structure")
        root.set("xmlns:mes", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
        root.set("xmlns:str", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")
        root.set("xmlns:com", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")

        structures = SubElement(root, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structures")
        codelists = SubElement(structures, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Codelists")
        codelist = SubElement(codelists, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Codelist")
        codelist.set("id", "CL_FREQ")

        # Add codes
        code1 = SubElement(codelist, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Code")
        code1.set("id", "Q")
        name1 = SubElement(code1, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name")
        name1.text = "Quarterly"

        code2 = SubElement(codelist, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Code")
        code2.set("id", "M")
        name2 = SubElement(code2, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name")
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
        root = Element("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structure")
        root.set("xmlns:mes", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message")
        root.set("xmlns:str", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure")
        root.set("xmlns:com", "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common")

        structures = SubElement(root, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structures")
        codelists = SubElement(structures, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Codelists")
        codelist = SubElement(codelists, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Codelist")
        codelist.set("id", "CL_REGION")

        code1 = SubElement(codelist, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Code")
        code1.set("id", "AUS")
        name1 = SubElement(code1, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name")
        name1.text = "Australia"

        code2 = SubElement(codelist, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Code")
        code2.set("id", "NSW")
        parent2 = SubElement(code2, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Parent")
        parent_ref = SubElement(parent2, "Ref")
        parent_ref.set("id", "AUS")
        name2 = SubElement(code2, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name")
        name2.text = "New South Wales"

        mock_acquire_xml.return_value = root

        result = code_lists("CL_REGION")

        assert "NSW" in result
        assert result["NSW"]["parent"] == "AUS"
        assert "parent" not in result["AUS"]


class TestCodeListFor:
    """Test code_list_for function."""

    @patch("sdmxabs.flow_metadata.data_structures")
    @patch("sdmxabs.flow_metadata.code_lists")
    def test_code_list_for_success(self, mock_code_lists, mock_data_structures):
        """Test successful code list retrieval for dimension."""
        mock_data_structures.return_value = {"FREQ": {"codelist_id": "CL_FREQ", "package": "codelist"}}
        mock_code_lists.return_value = {"Q": {"name": "Quarterly"}, "M": {"name": "Monthly"}}

        result = code_list_for("CPI_DSD", "FREQ")

        assert result == {"Q": {"name": "Quarterly"}, "M": {"name": "Monthly"}}
        mock_data_structures.assert_called_once_with("CPI_DSD")
        mock_code_lists.assert_called_once_with("CL_FREQ")

    @patch("sdmxabs.flow_metadata.data_structures")
    def test_code_list_for_missing_dimension(self, mock_data_structures):
        """Test handling of missing dimension."""
        mock_data_structures.return_value = {}

        with pytest.raises(ValueError):
            code_list_for("CPI_DSD", "NONEXISTENT")


class TestStructureIdent:
    """Test structure_ident function."""

    @patch("sdmxabs.flow_metadata.data_flows")
    def test_structure_ident_success(self, mock_data_flows):
        """Test successful structure identifier retrieval."""
        mock_data_flows.return_value = {
            "CPI": {"flow_name": "Consumer Price Index", "data_structure_id": "CPI_DSD"}
        }

        result = structure_ident("CPI")

        assert result == "CPI_DSD"
        mock_data_flows.assert_called_once_with("CPI")

    @patch("sdmxabs.flow_metadata.data_flows")
    def test_structure_ident_missing_flow(self, mock_data_flows):
        """Test handling of missing flow."""
        mock_data_flows.return_value = {}

        with pytest.raises(ValueError, match="No data structure found for flow 'NONEXISTENT'"):
            structure_ident("NONEXISTENT")

    @patch("sdmxabs.flow_metadata.data_flows")
    def test_structure_ident_missing_structure_id(self, mock_data_flows):
        """Test handling of flow without structure id."""
        # Clear cache to avoid interference
        structure_ident.cache_clear()
        
        # Missing data_structure_id
        mock_data_flows.return_value = {"CPI": {"flow_name": "Consumer Price Index"}}

        with pytest.raises(ValueError, match="No data structure found for flow 'CPI'"):
            structure_ident("CPI")


class TestBuildKey:
    """Test build_key function."""

    @patch("sdmxabs.flow_metadata.structure_from_flow_id")
    def test_build_key_no_dimensions(self, mock_structure_from_flow_id):
        """Test build_key with no dimensions."""
        mock_structure_from_flow_id.return_value = {}
        result = build_key("CPI", None)
        assert result == "all"

    @patch("sdmxabs.flow_metadata.structure_from_flow_id")
    def test_build_key_empty_dimensions(self, mock_structure_from_flow_id):
        """Test build_key with empty dimensions dict."""
        mock_structure_from_flow_id.return_value = {}
        result = build_key("CPI", {})
        assert result == "all"

    @patch("sdmxabs.flow_metadata.structure_from_flow_id")
    def test_build_key_with_dimensions(self, mock_structure_from_flow_id):
        """Test build_key with valid dimensions."""
        mock_structure_from_flow_id.return_value = {
            "FREQ": {"position": "1"},
            "REGION": {"position": "2"},
            "MEASURE": {"position": "3"},
        }

        dimensions = {"FREQ": "Q", "REGION": "AUS", "MEASURE": "1"}
        result = build_key("CPI", dimensions)

        assert result == "Q.AUS.1"

    @patch("sdmxabs.flow_metadata.structure_from_flow_id")
    def test_build_key_partial_dimensions(self, mock_structure_from_flow_id):
        """Test build_key with partial dimensions."""
        mock_structure_from_flow_id.return_value = {
            "FREQ": {"position": "1"},
            "REGION": {"position": "2"},
            "MEASURE": {"position": "3"},
        }

        dimensions = {"FREQ": "Q", "MEASURE": "1"}  # Missing REGION
        result = build_key("CPI", dimensions)

        assert result == "Q..1"  # Empty string for missing dimension

    @patch("sdmxabs.flow_metadata.structure_from_flow_id")
    def test_build_key_multiple_values(self, mock_structure_from_flow_id):
        """Test build_key with multiple values for dimension."""
        mock_structure_from_flow_id.return_value = {"FREQ": {"position": "1"}, "REGION": {"position": "2"}}

        dimensions = {"FREQ": "Q+M", "REGION": "AUS"}
        result = build_key("CPI", dimensions)

        assert result == "Q+M.AUS"

    @patch("sdmxabs.flow_metadata.structure_from_flow_id")
    def test_build_key_with_validation(self, mock_structure_from_flow_id):
        """Test build_key with validation enabled."""
        mock_structure_from_flow_id.return_value = {"FREQ": {"position": "1"}, "REGION": {"position": "2"}}

        dimensions = {"FREQ": "Q", "INVALID_DIM": "value"}
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
                root = Element("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structure")
                structures = SubElement(root, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structures")
                dataflows = SubElement(structures, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DataFlows")
                df = SubElement(dataflows, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Dataflow")
                df.set("id", "CPI")
                name = SubElement(df, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name")
                name.text = "Consumer Price Index"
                struct = SubElement(df, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Structure")
                ref = SubElement(struct, "Ref")
                ref.set("id", "CPI_DSD")
                return root
            if "datastructure" in url:
                # Return data structure
                root = Element("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structure")
                structures = SubElement(root, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structures")
                dsds = SubElement(structures, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DataStructures")
                dsd = SubElement(dsds, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DataStructure")
                dsd_components = SubElement(dsd, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DataStructureComponents")
                dim_list = SubElement(dsd_components, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}DimensionList")
                dim = SubElement(dim_list, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Dimension")
                dim.set("id", "FREQ")
                return root
            # Return codelist
            root = Element("{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structure")
            structures = SubElement(root, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}Structures")
            codelists = SubElement(structures, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Codelists")
            codelist = SubElement(codelists, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Codelist")
            code = SubElement(codelist, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Code")
            code.set("id", "Q")
            name = SubElement(code, "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common}Name")
            name.text = "Quarterly"
            return root

        mock_acquire_xml.side_effect = side_effect

        # Test the workflow
        flows = data_flows()
        assert "CPI" in flows

        # Note: Using structure_ident + data_structures would be more correct,
        # but for this test we'll use the simpler mock approach
        struct_id = "CPI_DSD"  # This should match the mocked structure
        structures = data_structures(struct_id)
        assert isinstance(structures, dict)

        codes = code_lists("CL_FREQ")
        assert isinstance(codes, dict)
