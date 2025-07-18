"""Integration tests for sdmxabs package."""

from unittest.mock import patch
from xml.etree.ElementTree import Element, SubElement

import pandas as pd
import pytest

import sdmxabs as sa
from sdmxabs.download_cache import CacheError, HttpError


class TestIntegrationBasicWorkflow:
    """Test basic integration workflows."""

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_data_flows_to_fetch_workflow(self, mock_acquire_xml):
        """Test complete workflow from data_flows to fetch."""

        def xml_side_effect(url, **kwargs):
            if "dataflow" in url:
                # Mock dataflows response
                root = Element("message:Structure")
                structures = SubElement(root, "message:Structures")
                dataflows = SubElement(structures, "str:DataFlows")

                df = SubElement(dataflows, "str:DataFlow")
                df.set("id", "CPI")
                name = SubElement(df, "com:Name")
                name.text = "Consumer Price Index"

                return root

            if "datastructure" in url:
                # Mock data structure response
                root = Element("message:Structure")
                structures = SubElement(root, "message:Structures")
                dsds = SubElement(structures, "str:DataStructures")
                dsd = SubElement(dsds, "str:DataStructure")

                components = SubElement(dsd, "str:DataStructureComponents")
                dim_list = SubElement(components, "str:DimensionList")

                # Add FREQ dimension
                freq_dim = SubElement(dim_list, "str:Dimension")
                freq_dim.set("id", "FREQ")
                freq_dim.set("position", "1")
                freq_rep = SubElement(freq_dim, "str:LocalRepresentation")
                freq_enum = SubElement(freq_rep, "str:Enumeration")
                freq_ref = SubElement(freq_enum, "Ref")
                freq_ref.set("id", "CL_FREQ")
                freq_ref.set("package", "codelist")

                return root

            if "codelist" in url:
                # Mock codelist response
                root = Element("message:Structure")
                structures = SubElement(root, "message:Structures")
                codelists = SubElement(structures, "str:Codelists")
                codelist = SubElement(codelists, "str:Codelist")

                code = SubElement(codelist, "str:Code")
                code.set("id", "Q")
                name = SubElement(code, "com:Name")
                name.text = "Quarterly"

                return root

            # Mock data response
            root = Element("message:StructureSpecificData")
            dataset = SubElement(root, "message:DataSet")
            series = SubElement(dataset, "gen:Series")

            # Add series key
            series_key = SubElement(series, "gen:SeriesKey")
            value = SubElement(series_key, "gen:Value")
            value.set("id", "FREQ")
            value.set("value", "Q")

            # Add observations
            obs = SubElement(series, "gen:Obs")
            dim = SubElement(obs, "gen:ObsDimension")
            dim.set("value", "2023-Q1")
            val = SubElement(obs, "gen:ObsValue")
            val.set("value", "100.0")

            return root

        mock_acquire_xml.side_effect = xml_side_effect

        # Test the workflow
        flows = sa.data_flows()
        assert "CPI" in flows
        assert flows["CPI"]["name"] == "Consumer Price Index"

        dims = sa.data_dimensions("CPI")
        assert "FREQ" in dims

        codes = sa.code_lists("CL_FREQ")
        assert "Q" in codes

        data, meta = sa.fetch("CPI", {"FREQ": "Q"})
        assert isinstance(data, pd.DataFrame)
        assert isinstance(meta, pd.DataFrame)

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_fetch_selection_workflow(self, mock_acquire_xml):
        """Test fetch_selection workflow."""

        def xml_side_effect(url, **kwargs):
            if "datastructure" in url:
                # Mock data structure
                root = Element("message:Structure")
                structures = SubElement(root, "message:Structures")
                dsds = SubElement(structures, "str:DataStructures")
                dsd = SubElement(dsds, "str:DataStructure")

                components = SubElement(dsd, "str:DataStructureComponents")
                dim_list = SubElement(components, "str:DimensionList")

                # Add dimensions
                freq_dim = SubElement(dim_list, "str:Dimension")
                freq_dim.set("id", "FREQ")
                freq_dim.set("position", "1")

                region_dim = SubElement(dim_list, "str:Dimension")
                region_dim.set("id", "REGION")
                region_dim.set("position", "2")

                return root

            if "codelist" in url and "FREQ" in url:
                # Mock frequency codelist
                root = Element("message:Structure")
                structures = SubElement(root, "message:Structures")
                codelists = SubElement(structures, "str:Codelists")
                codelist = SubElement(codelists, "str:Codelist")

                code = SubElement(codelist, "str:Code")
                code.set("id", "Q")
                name = SubElement(code, "com:Name")
                name.text = "Quarterly"

                return root

            if "codelist" in url and "REGION" in url:
                # Mock region codelist
                root = Element("message:Structure")
                structures = SubElement(root, "message:Structures")
                codelists = SubElement(structures, "str:Codelists")
                codelist = SubElement(codelists, "str:Codelist")

                code = SubElement(codelist, "str:Code")
                code.set("id", "AUS")
                name = SubElement(code, "com:Name")
                name.text = "Australia"

                return root

            # Mock data response
            root = Element("message:StructureSpecificData")
            dataset = SubElement(root, "message:DataSet")
            series = SubElement(dataset, "gen:Series")

            # Add observations
            obs = SubElement(series, "gen:Obs")
            dim = SubElement(obs, "gen:ObsDimension")
            dim.set("value", "2023-Q1")
            val = SubElement(obs, "gen:ObsValue")
            val.set("value", "100.0")

            return root

        mock_acquire_xml.side_effect = xml_side_effect

        # Test fetch_selection
        criteria = [
            sa.match_item("Quarterly", "FREQ", sa.MatchType.PARTIAL),
            sa.match_item("Australia", "REGION", sa.MatchType.PARTIAL),
        ]

        data, meta = sa.fetch_selection("CPI", criteria)
        assert isinstance(data, pd.DataFrame)
        assert isinstance(meta, pd.DataFrame)

    def test_measures_integration(self, sample_dataframe_data):
        """Test measures module integration."""
        data, meta = sample_dataframe_data

        # Test measure names
        names = sa.measure_names(meta)
        assert isinstance(names, pd.Series)
        assert len(names) == 2

        # Test recalibration
        new_data, new_units = sa.recalibrate(data, names)
        assert isinstance(new_data, pd.DataFrame)
        assert isinstance(new_units, pd.Series)
        assert new_data.shape == data.shape

        # Test single series recalibration
        series = data.iloc[:, 0]
        label = names.iloc[0]
        new_series, new_label = sa.recalibrate_series(series, label)
        assert isinstance(new_series, pd.Series)
        assert isinstance(new_label, str)


class TestIntegrationErrorHandling:
    """Test error handling in integration scenarios."""

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_http_error_propagation(self, mock_acquire_xml):
        """Test that HTTP errors are properly propagated."""
        mock_acquire_xml.side_effect = HttpError("Network error")

        with pytest.raises(HttpError):
            sa.data_flows()

        with pytest.raises(HttpError):
            sa.fetch("CPI", {"FREQ": "Q"})

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_cache_error_propagation(self, mock_acquire_xml):
        """Test that cache errors are properly propagated."""
        mock_acquire_xml.side_effect = CacheError("Cache error")

        with pytest.raises(CacheError):
            sa.data_flows()

        with pytest.raises(CacheError):
            sa.fetch("CPI", {"FREQ": "Q"})

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_invalid_xml_handling(self, mock_acquire_xml):
        """Test handling of invalid XML responses."""
        mock_acquire_xml.side_effect = ValueError("Invalid XML")

        with pytest.raises(ValueError):
            sa.data_flows()


class TestIntegrationRealWorldScenarios:
    """Test realistic usage scenarios."""

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_gdp_fetch_scenario(self, mock_acquire_xml):
        """Test GDP-specific fetch scenario."""
        # Mock XML response for GDP data
        root = Element("message:StructureSpecificData")
        dataset = SubElement(root, "message:DataSet")
        series = SubElement(dataset, "gen:Series")

        # Add series metadata
        series_key = SubElement(series, "gen:SeriesKey")

        freq_val = SubElement(series_key, "gen:Value")
        freq_val.set("id", "FREQ")
        freq_val.set("value", "Q")

        measure_val = SubElement(series_key, "gen:Value")
        measure_val.set("id", "MEASURE")
        measure_val.set("value", "3")

        # Add observations
        periods = ["2023-Q1", "2023-Q2", "2023-Q3", "2023-Q4"]
        values = ["100.0", "101.5", "102.1", "103.0"]

        for period, value in zip(periods, values, strict=False):
            obs = SubElement(series, "gen:Obs")
            dim = SubElement(obs, "gen:ObsDimension")
            dim.set("value", period)
            val = SubElement(obs, "gen:ObsValue")
            val.set("value", value)

        mock_acquire_xml.return_value = root

        # Test GDP fetch
        data, meta = sa.fetch_gdp(seasonality="seasonally_adjusted", price_measure="chain_volume")

        assert isinstance(data, pd.DataFrame)
        assert isinstance(meta, pd.DataFrame)
        assert len(data) == 4  # Should have 4 quarters of data

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_population_fetch_scenario(self, mock_acquire_xml):
        """Test population-specific fetch scenario."""
        # Mock XML response for population data
        root = Element("message:StructureSpecificData")
        dataset = SubElement(root, "message:DataSet")
        series = SubElement(dataset, "gen:Series")

        # Add observations
        periods = ["2023-Q1", "2023-Q2", "2023-Q3"]
        values = ["25000000", "25100000", "25200000"]

        for period, value in zip(periods, values, strict=False):
            obs = SubElement(series, "gen:Obs")
            dim = SubElement(obs, "gen:ObsDimension")
            dim.set("value", period)
            val = SubElement(obs, "gen:ObsValue")
            val.set("value", value)

        mock_acquire_xml.return_value = root

        # Test population fetch
        data, meta = sa.fetch_pop()

        assert isinstance(data, pd.DataFrame)
        assert isinstance(meta, pd.DataFrame)
        assert len(data) == 3

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_multi_fetch_scenario(self, mock_acquire_xml):
        """Test multi-series fetch scenario."""
        # Create a wanted DataFrame
        wanted = pd.DataFrame({"flow_id": ["CPI", "WPI"], "FREQ": ["Q", "Q"], "REGION": ["AUS", "AUS"]})

        # Mock XML response
        root = Element("message:StructureSpecificData")
        dataset = SubElement(root, "message:DataSet")

        # Create multiple series
        for i in range(2):
            series = SubElement(dataset, "gen:Series")

            # Add series key
            series_key = SubElement(series, "gen:SeriesKey")
            freq_val = SubElement(series_key, "gen:Value")
            freq_val.set("id", "FREQ")
            freq_val.set("value", "Q")

            # Add observation
            obs = SubElement(series, "gen:Obs")
            dim = SubElement(obs, "gen:ObsDimension")
            dim.set("value", "2023-Q1")
            val = SubElement(obs, "gen:ObsValue")
            val.set("value", str(100 + i))

        mock_acquire_xml.return_value = root

        # Test multi-fetch
        data, meta = sa.fetch_multi(wanted)

        assert isinstance(data, pd.DataFrame)
        assert isinstance(meta, pd.DataFrame)


class TestIntegrationDataTypes:
    """Test data type handling in integration scenarios."""

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_numeric_data_handling(self, mock_acquire_xml):
        """Test handling of various numeric data types."""
        root = Element("message:StructureSpecificData")
        dataset = SubElement(root, "message:DataSet")
        series = SubElement(dataset, "gen:Series")

        # Add observations with different numeric formats
        test_values = ["100.0", "101.5", "102", "-1.5", "0"]

        for i, value in enumerate(test_values):
            obs = SubElement(series, "gen:Obs")
            dim = SubElement(obs, "gen:ObsDimension")
            dim.set("value", f"2023-0{i + 1}")
            val = SubElement(obs, "gen:ObsValue")
            val.set("value", value)

        mock_acquire_xml.return_value = root

        data, meta = sa.fetch("CPI", {"FREQ": "M"})

        # Check that data is properly converted to numeric
        assert isinstance(data, pd.DataFrame)
        if len(data.columns) > 0:
            series_data = data.iloc[:, 0]
            assert pd.api.types.is_numeric_dtype(series_data)
            assert not series_data.empty

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_missing_data_handling(self, mock_acquire_xml):
        """Test handling of missing and empty data."""
        root = Element("message:StructureSpecificData")
        dataset = SubElement(root, "message:DataSet")
        series = SubElement(dataset, "gen:Series")

        # Add observations with missing values
        test_values = ["100.0", "", "102.0", None]

        for i, value in enumerate(test_values):
            obs = SubElement(series, "gen:Obs")
            dim = SubElement(obs, "gen:ObsDimension")
            dim.set("value", f"2023-0{i + 1}")
            val = SubElement(obs, "gen:ObsValue")
            if value is not None:
                val.set("value", value)

        mock_acquire_xml.return_value = root

        data, meta = sa.fetch("CPI", {"FREQ": "M"})

        # Check that missing values are handled properly
        assert isinstance(data, pd.DataFrame)
        if len(data.columns) > 0:
            series_data = data.iloc[:, 0]
            # Should have some NaN values where data was missing
            assert series_data.isna().any()


class TestIntegrationPerformance:
    """Test performance-related integration scenarios."""

    @patch("sdmxabs.xml_base.acquire_xml")
    def test_large_dataset_handling(self, mock_acquire_xml):
        """Test handling of large datasets."""
        root = Element("message:StructureSpecificData")
        dataset = SubElement(root, "message:DataSet")

        # Create multiple series with many observations
        for series_idx in range(5):
            series = SubElement(dataset, "gen:Series")

            # Add series key
            series_key = SubElement(series, "gen:SeriesKey")
            freq_val = SubElement(series_key, "gen:Value")
            freq_val.set("id", "SERIES")
            freq_val.set("value", str(series_idx))

            # Add many observations
            for obs_idx in range(100):
                obs = SubElement(series, "gen:Obs")
                dim = SubElement(obs, "gen:ObsDimension")
                dim.set("value", f"2023-{obs_idx:03d}")
                val = SubElement(obs, "gen:ObsValue")
                val.set("value", str(100 + obs_idx))

        mock_acquire_xml.return_value = root

        data, meta = sa.fetch("TEST", {"FREQ": "D"})

        # Verify large dataset is handled correctly
        assert isinstance(data, pd.DataFrame)
        assert isinstance(meta, pd.DataFrame)
        assert data.shape[1] == 5  # 5 series
        assert data.shape[0] == 100  # 100 observations per series
