"""Basic tests to verify the testing framework works."""

import numpy as np
import pandas as pd
import pytest

# Import the main package
import sdmxabs as sa


class TestBasicImports:
    """Test that basic imports work."""

    def test_package_imports(self):
        """Test that the package imports successfully."""
        assert hasattr(sa, "__version__")
        assert hasattr(sa, "fetch")
        assert hasattr(sa, "data_flows")

    def test_exception_classes(self):
        """Test that exception classes are available."""
        from sdmxabs.download_cache import CacheError, HttpError

        # Test that we can create instances
        http_err = HttpError("test")
        cache_err = CacheError("test")

        assert str(http_err) == "test"
        assert str(cache_err) == "test"


class TestBasicFunctionality:
    """Test basic functionality without external dependencies."""

    def test_frequency_mapping(self):
        """Test frequency mapping constants."""
        from sdmxabs.fetch import FREQUENCY_MAPPING

        assert "Quarterly" in FREQUENCY_MAPPING
        assert FREQUENCY_MAPPING["Quarterly"] in ["Q", "QE"]

    def test_measures_constants(self):
        """Test measures module constants."""
        from sdmxabs.measures import INDICIES, MAX_FACTOR

        assert isinstance(INDICIES, dict)
        assert len(INDICIES) > 0
        assert isinstance(MAX_FACTOR, int)

    def test_measure_names_basic(self):
        """Test measure_names with simple data."""
        meta = pd.DataFrame({
            "series1": {"name": "Test Series 1"},
            "series2": {"name": "Test Series 2"}
        }).T

        result = sa.measure_names(meta)

        assert isinstance(result, pd.Series)
        assert len(result) == 2

    def test_recalibrate_series_basic(self):
        """Test basic series recalibration."""
        series = pd.Series([100, 200, 300], name="test")
        label = "Test Label"

        new_series, new_label = sa.recalibrate_series(series, label)

        assert isinstance(new_series, pd.Series)
        assert isinstance(new_label, str)
        # Series in good range should be unchanged
        assert new_series.equals(series)
        assert new_label == label


class TestDataStructures:
    """Test data structure handling."""

    def test_dataframe_creation(self):
        """Test basic DataFrame operations."""
        data = pd.DataFrame({
            "A": [1, 2, 3],
            "B": [4, 5, 6]
        })

        assert data.shape == (3, 2)
        assert list(data.columns) == ["A", "B"]

    def test_series_with_nan(self):
        """Test handling of NaN values."""
        series = pd.Series([1.0, np.nan, 3.0])

        assert len(series) == 3
        assert series.isna().sum() == 1
        assert not series.dropna().empty


class TestMatchTypes:
    """Test match type functionality."""

    def test_match_type_enum(self):
        """Test MatchType enum values."""
        assert hasattr(sa, "MatchType")

        # Test enum values exist
        assert hasattr(sa.MatchType, "EXACT")
        assert hasattr(sa.MatchType, "PARTIAL") 
        assert hasattr(sa.MatchType, "REGEX")

    def test_match_item_creation(self):
        """Test creating match items."""
        item = sa.match_item("test", "FREQ", sa.MatchType.PARTIAL)

        assert isinstance(item, tuple)
        assert len(item) == 3
        assert item[0] == "test"
        assert item[1] == "FREQ"
        assert item[2] == sa.MatchType.PARTIAL
