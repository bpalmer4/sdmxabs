"""Tests for fetch module."""

from unittest.mock import patch
from xml.etree.ElementTree import Element, SubElement

import pandas as pd
import pytest

from sdmxabs.download_cache import CacheError, HttpError
from sdmxabs.fetch import (
    FREQUENCY_MAPPING,
    MetadataContext,
    _convert_to_period_index,
    _decode_meta_value,
    _extract,
    _extract_observation_data,
    _get_series_data,
    fetch,
)


class TestFrequencyMapping:
    """Test frequency mapping constants."""

    def test_frequency_mapping_completeness(self):
        """Test that all expected frequencies are mapped."""
        expected_frequencies = ["Annual", "Quarterly", "Monthly", "Daily"]

        for freq in expected_frequencies:
            assert freq in FREQUENCY_MAPPING

        assert FREQUENCY_MAPPING["Annual"] == "Y"
        assert FREQUENCY_MAPPING["Quarterly"] == "Q"
        assert FREQUENCY_MAPPING["Monthly"] == "M"
        assert FREQUENCY_MAPPING["Daily"] == "D"


class TestMetadataContext:
    """Test MetadataContext dataclass."""

    def test_metadata_context_creation(self):
        """Test MetadataContext creation."""
        context = MetadataContext(
            series_count=1,
            label_elements=["CPI"],
            meta_items={"FREQ": "Q"},
            structure={},
            item_count=0,
        )

        assert context.series_count == 1
        assert context.label_elements == ["CPI"]
        assert context.meta_items == {"FREQ": "Q"}
        assert context.structure == {}
        assert context.item_count == 0


class TestConvertToPeriodIndex:
    """Test _convert_to_period_index function."""

    def test_convert_quarterly_frequency(self):
        """Test conversion with quarterly frequency."""
        series = pd.Series([100, 101, 102], index=["2023-Q1", "2023-Q2", "2023-Q3"])
        result = _convert_to_period_index(series, "Quarterly")

        assert isinstance(result.index, pd.PeriodIndex)
        # Handle pandas version differences - frequency can be represented differently
        freq_str = str(result.index.freq)
        assert any(x in freq_str for x in ["Q", "QE", "QuarterEnd"])

    def test_convert_monthly_frequency(self):
        """Test conversion with monthly frequency."""
        series = pd.Series([100, 101], index=["2023-01", "2023-02"])
        result = _convert_to_period_index(series, "Monthly")

        assert isinstance(result.index, pd.PeriodIndex)
        # Handle pandas version differences - frequency can be represented differently
        freq_str = str(result.index.freq)  
        assert any(x in freq_str for x in ["M", "ME", "MonthEnd"])

    def test_convert_unknown_frequency(self):
        """Test conversion with unknown frequency."""
        series = pd.Series([100, 101], index=["2023-01", "2023-02"])
        result = _convert_to_period_index(series, "Unknown")

        # Should return original series unchanged
        assert not isinstance(result.index, pd.PeriodIndex)
        assert result.equals(series)


class TestExtractObservationData:
    """Test _extract_observation_data function."""

    def test_extract_observation_data_success(self):
        """Test successful observation data extraction."""
        from sdmxabs.xml_base import NAME_SPACES
        
        # Create mock XML series element with proper namespace
        series = Element("{%s}Series" % NAME_SPACES["gen"])

        obs1 = SubElement(series, "{%s}Obs" % NAME_SPACES["gen"])
        dim1 = SubElement(obs1, "{%s}ObsDimension" % NAME_SPACES["gen"])
        dim1.set("value", "2023-Q1")
        val1 = SubElement(obs1, "{%s}ObsValue" % NAME_SPACES["gen"])
        val1.set("value", "100.5")

        obs2 = SubElement(series, "{%s}Obs" % NAME_SPACES["gen"])
        dim2 = SubElement(obs2, "{%s}ObsDimension" % NAME_SPACES["gen"])
        dim2.set("value", "2023-Q2")
        val2 = SubElement(obs2, "{%s}ObsValue" % NAME_SPACES["gen"])
        val2.set("value", "101.2")

        result = _extract_observation_data(series)

        assert result == {"2023-Q1": "100.5", "2023-Q2": "101.2"}

    def test_extract_observation_data_missing_dimension(self):
        """Test extraction with missing observation dimension."""
        series = Element("gen:Series")
        obs = SubElement(series, "gen:Obs")
        val = SubElement(obs, "gen:ObsValue")
        val.set("value", "100.5")
        # No ObsDimension element

        result = _extract_observation_data(series)

        assert result == {}

    def test_extract_observation_data_missing_value(self):
        """Test extraction with missing observation value."""
        series = Element("gen:Series")
        obs = SubElement(series, "gen:Obs")
        dim = SubElement(obs, "gen:ObsDimension")
        dim.set("value", "2023-Q1")
        # No ObsValue element

        result = _extract_observation_data(series)

        assert result == {}


class TestGetSeriesData:
    """Test _get_series_data function."""

    def test_get_series_data_numeric(self):
        """Test series data extraction with numeric values."""
        # Use proper namespace URI for gen
        gen_ns = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
        series = Element(f"{{{gen_ns}}}Series")

        obs1 = SubElement(series, f"{{{gen_ns}}}Obs")
        dim1 = SubElement(obs1, f"{{{gen_ns}}}ObsDimension")
        dim1.set("value", "2023-Q1")
        val1 = SubElement(obs1, f"{{{gen_ns}}}ObsValue")
        val1.set("value", "100.5")

        obs2 = SubElement(series, f"{{{gen_ns}}}Obs")
        dim2 = SubElement(obs2, f"{{{gen_ns}}}ObsDimension")
        dim2.set("value", "2023-Q2")
        val2 = SubElement(obs2, f"{{{gen_ns}}}ObsValue")
        val2.set("value", "101.2")

        meta = pd.Series({"FREQ": "Quarterly", "name": "test_series"})
        meta.name = "test_series"

        result = _get_series_data(series, meta)

        assert isinstance(result, pd.Series)
        assert len(result) == 2
        assert result.iloc[0] == 100.5
        assert result.iloc[1] == 101.2
        assert isinstance(result.index, pd.PeriodIndex)

    def test_get_series_data_non_numeric(self):
        """Test series data extraction with non-numeric values."""
        # Use proper namespace URI for gen
        gen_ns = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
        series = Element(f"{{{gen_ns}}}Series")

        obs = SubElement(series, f"{{{gen_ns}}}Obs")
        dim = SubElement(obs, f"{{{gen_ns}}}ObsDimension")
        dim.set("value", "2023-Q1")
        val = SubElement(obs, f"{{{gen_ns}}}ObsValue")
        val.set("value", "N/A")

        meta = pd.Series({"FREQ": "Quarterly", "name": "test_series"})
        meta.name = "test_series"

        with patch("builtins.print") as mock_print:
            result = _get_series_data(series, meta)

        assert isinstance(result, pd.Series)
        assert len(result) == 1  # Make sure we have data before accessing it
        assert result.iloc[0] == "N/A"
        mock_print.assert_called_once()

    def test_get_series_data_empty_values(self):
        """Test series data extraction with empty values."""
        # Use proper namespace URI for gen
        gen_ns = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
        series = Element(f"{{{gen_ns}}}Series")

        obs = SubElement(series, f"{{{gen_ns}}}Obs")
        dim = SubElement(obs, f"{{{gen_ns}}}ObsDimension")
        dim.set("value", "2023-Q1")
        val = SubElement(obs, f"{{{gen_ns}}}ObsValue")
        val.set("value", "")

        meta = pd.Series({"FREQ": "Quarterly", "name": "test_series"})
        meta.name = "test_series"

        result = _get_series_data(series, meta)

        assert isinstance(result, pd.Series)
        assert len(result) == 1  # Make sure we have data before accessing it
        assert pd.isna(result.iloc[0])


class TestDecodeMetaValue:
    """Test _decode_meta_value function."""

    @patch("sdmxabs.fetch.code_lists")
    def test_decode_meta_value_success(self, mock_code_lists):
        """Test successful metadata value decoding."""
        mock_code_lists.return_value = {"Q": {"name": "Quarterly"}, "M": {"name": "Monthly"}}

        structure = {"FREQ": {"codelist_id": "CL_FREQ", "package": "codelist"}}

        result = _decode_meta_value("Q", "FREQ", structure)

        assert result == "Quarterly"
        mock_code_lists.assert_called_once_with("CL_FREQ")

    def test_decode_meta_value_no_dimension(self):
        """Test decoding when dimension not found."""
        dims = {}

        result = _decode_meta_value("Q", "FREQ", dims)

        assert result == "Q"  # Should return original value

    def test_decode_meta_value_no_codelist(self):
        """Test decoding when not a codelist."""
        dims = {"FREQ": {"id": "CL_FREQ", "package": "other"}}

        result = _decode_meta_value("Q", "FREQ", dims)

        assert result == "Q"  # Should return original value

    @patch("sdmxabs.fetch.code_lists")
    def test_decode_meta_value_code_not_found(self, mock_code_lists):
        """Test decoding when code not found in codelist."""
        mock_code_lists.return_value = {"M": {"name": "Monthly"}}

        dims = {"FREQ": {"id": "CL_FREQ", "package": "codelist"}}

        result = _decode_meta_value("Q", "FREQ", dims)

        assert result == "Q"  # Should return original value


class TestFetch:
    """Test fetch function."""

    @patch("sdmxabs.fetch._extract")
    @patch("sdmxabs.fetch.acquire_xml")
    @patch("sdmxabs.fetch.build_key")
    def test_fetch_success(self, mock_build_key, mock_acquire_xml, mock_extract):
        """Test successful fetch operation."""
        mock_build_key.return_value = "Q.AUS"
        mock_acquire_xml.return_value = Element("root")

        # Create mock DataFrames
        data_df = pd.DataFrame({"series1": [100, 101, 102]})
        meta_df = pd.DataFrame({"series1": {"FREQ": "Q", "REGION": "AUS"}}).T
        mock_extract.return_value = (data_df, meta_df)

        result_data, result_meta = fetch("CPI", {"FREQ": "Q", "REGION": "AUS"})

        assert isinstance(result_data, pd.DataFrame)
        assert isinstance(result_meta, pd.DataFrame)
        assert len(result_data) == 3
        mock_build_key.assert_called_once_with("CPI", {"FREQ": "Q", "REGION": "AUS"}, validate=False)

    @patch("sdmxabs.fetch.acquire_xml")
    @patch("sdmxabs.fetch.build_key")
    def test_fetch_no_dimensions(self, mock_build_key, mock_acquire_xml):
        """Test fetch with no dimensions."""
        mock_build_key.return_value = "all"
        mock_acquire_xml.return_value = Element("root")

        with patch("sdmxabs.fetch._extract") as mock_extract:
            mock_extract.return_value = (pd.DataFrame(), pd.DataFrame())
            fetch("CPI", None)

        mock_build_key.assert_called_once_with("CPI", None, validate=False)

    def test_fetch_invalid_parameters(self):
        """Test fetch with invalid parameters."""
        with pytest.raises(ValueError, match="Invalid detail value"):
            fetch("CPI", None, parameters={"detail": "invalid"})

    @patch("sdmxabs.fetch.build_key")
    def test_fetch_with_parameters(self, mock_build_key):
        """Test fetch with URL parameters."""
        mock_build_key.return_value = "Q.AUS"

        with patch("sdmxabs.fetch.acquire_xml") as mock_acquire_xml:
            mock_acquire_xml.return_value = Element("root")
            with patch("sdmxabs.fetch._extract") as mock_extract:
                mock_extract.return_value = (pd.DataFrame(), pd.DataFrame())

                parameters = {"startPeriod": "2020-Q1", "endPeriod": "2023-Q4", "detail": "full"}

                fetch("CPI", {"FREQ": "Q"}, parameters=parameters)

        # Check that the URL was built with parameters
        call_args = mock_acquire_xml.call_args[0]
        url = call_args[0]
        assert "startPeriod=2020-Q1" in url
        assert "endPeriod=2023-Q4" in url
        assert "detail=full" in url

    @patch("sdmxabs.fetch.acquire_xml")
    def test_fetch_http_error(self, mock_acquire_xml):
        """Test fetch handling HTTP errors."""
        mock_acquire_xml.side_effect = HttpError("HTTP error")

        with pytest.raises(HttpError):
            fetch("CPI", {"FREQ": "Q"})

    @patch("sdmxabs.fetch.acquire_xml")
    def test_fetch_cache_error(self, mock_acquire_xml):
        """Test fetch handling cache errors."""
        mock_acquire_xml.side_effect = CacheError("Cache error")

        with pytest.raises(CacheError):
            fetch("CPI", {"FREQ": "Q"})


class TestExtract:
    """Test _extract function."""

    @patch("sdmxabs.fetch.structure_from_flow_id")
    def test_extract_success(self, mock_structure_from_flow_id):
        """Test successful data extraction from XML."""
        mock_structure_from_flow_id.return_value = {
            "FREQ": {"codelist_id": "CL_FREQ", "package": "codelist"},
            "REGION": {"codelist_id": "CL_REGION", "package": "codelist"},
        }

        # Create mock XML tree with proper namespaces
        gen_ns = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
        root = Element("root")
        series = SubElement(root, f"{{{gen_ns}}}Series")

        # Add series key
        series_key = SubElement(series, f"{{{gen_ns}}}SeriesKey")
        value1 = SubElement(series_key, f"{{{gen_ns}}}Value")
        value1.set("id", "FREQ")
        value1.set("value", "Q")

        # Add attributes
        attributes = SubElement(series, f"{{{gen_ns}}}Attributes")
        value2 = SubElement(attributes, f"{{{gen_ns}}}Value")
        value2.set("id", "UNIT")
        value2.set("value", "INDEX")

        # Add observations
        obs = SubElement(series, f"{{{gen_ns}}}Obs")
        dim = SubElement(obs, f"{{{gen_ns}}}ObsDimension")
        dim.set("value", "2023-Q1")
        val = SubElement(obs, f"{{{gen_ns}}}ObsValue")
        val.set("value", "100.5")

        with patch("sdmxabs.fetch.data_flows") as mock_data_flows:
            mock_data_flows.return_value = {"CPI": {"flow_name": "Consumer Price Index"}}

            data_df, meta_df = _extract("CPI", root)

        assert isinstance(data_df, pd.DataFrame)
        assert isinstance(meta_df, pd.DataFrame)
        assert len(data_df.columns) == 1
        assert len(meta_df) == 1

    @patch("sdmxabs.fetch.structure_from_flow_id")
    def test_extract_no_series(self, mock_structure_from_flow_id):
        """Test extraction with no series in XML."""
        mock_structure_from_flow_id.return_value = {}

        root = Element("root")
        # No series elements

        data_df, meta_df = _extract("CPI", root)

        assert isinstance(data_df, pd.DataFrame)
        assert isinstance(meta_df, pd.DataFrame)
        assert len(data_df.columns) == 0
        assert len(meta_df) == 0

    @patch("sdmxabs.fetch.structure_from_flow_id")
    def test_extract_duplicate_series(self, mock_structure_from_flow_id):
        """Test extraction with duplicate series (same metadata)."""
        mock_structure_from_flow_id.return_value = {"FREQ": {"codelist_id": "CL_FREQ", "package": "codelist"}}

        gen_ns = "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic"
        root = Element("root")

        # Create two series with same metadata
        for i in range(2):
            series = SubElement(root, f"{{{gen_ns}}}Series")

            series_key = SubElement(series, f"{{{gen_ns}}}SeriesKey")
            value = SubElement(series_key, f"{{{gen_ns}}}Value")
            value.set("id", "FREQ")
            value.set("value", "Q")

            obs = SubElement(series, f"{{{gen_ns}}}Obs")
            dim = SubElement(obs, f"{{{gen_ns}}}ObsDimension")
            dim.set("value", f"2023-Q{i + 1}")
            val = SubElement(obs, f"{{{gen_ns}}}ObsValue")
            val.set("value", str(100 + i))

        with patch("sdmxabs.fetch.data_flows") as mock_data_flows:
            mock_data_flows.return_value = {"CPI": {"flow_name": "Consumer Price Index"}}

            data_df, meta_df = _extract("CPI", root)

        # Should combine the series data
        assert len(data_df.columns) == 1
        series_data = data_df.iloc[:, 0]
        assert len(series_data.dropna()) == 2  # Should have both observations
