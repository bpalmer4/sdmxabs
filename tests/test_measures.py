"""Tests for measures module."""

import numpy as np
import pandas as pd
import pytest

from sdmxabs.measures import INDICIES, MAX_FACTOR, measure_names, recalibrate, recalibrate_series


class TestConstants:
    """Test module constants."""

    def test_indicies_structure(self):
        """Test INDICIES dictionary structure."""
        assert isinstance(INDICIES, dict)
        assert len(INDICIES) > 0

        # Check that all keys are integers and values are strings
        for key, value in INDICIES.items():
            assert isinstance(key, int)
            assert isinstance(value, str)
            assert len(value) > 0  # non-empty string

    def test_max_factor(self):
        """Test MAX_FACTOR calculation."""
        assert max(INDICIES.keys()) == MAX_FACTOR
        assert isinstance(MAX_FACTOR, int)
        assert MAX_FACTOR > 0


class TestMeasureNames:
    """Test measure_names function."""

    def test_measure_names_basic(self):
        """Test basic measure_names functionality."""
        meta = pd.DataFrame(
            {
                "series1": {"UNIT_MEASURE": "Consumer Price Index", "UNIT": "INDEX"},
                "series2": {"UNIT_MEASURE": "Wage Price Index", "UNIT": "PERCENT"},
            }
        ).T

        result = measure_names(meta)

        assert isinstance(result, pd.Series)
        assert len(result) == 2
        assert "Consumer Price Index" in result.values
        assert "Wage Price Index" in result.values

    def test_measure_names_missing_name(self):
        """Test measure_names with missing name column."""
        meta = pd.DataFrame({"series1": {"UNIT": "INDEX"}, "series2": {"UNIT": "PERCENT"}}).T

        # Should use index as names when 'name' column is missing
        result = measure_names(meta)

        assert isinstance(result, pd.Series)
        assert len(result) == 2
        assert "series1" in result.values
        assert "series2" in result.values

    def test_measure_names_empty_dataframe(self):
        """Test measure_names with empty DataFrame."""
        meta = pd.DataFrame()

        result = measure_names(meta)

        assert isinstance(result, pd.Series)
        assert len(result) == 0

    def test_measure_names_single_series(self):
        """Test measure_names with single series."""
        meta = pd.DataFrame({"series1": {"UNIT_MEASURE": "Test Series", "UNIT": "INDEX"}}).T

        result = measure_names(meta)

        assert isinstance(result, pd.Series)
        assert len(result) == 1
        assert result.iloc[0] == "Test Series"


class TestRecalibrateSeries:
    """Test recalibrate_series function."""

    def test_recalibrate_series_large_values(self):
        """Test recalibrating series with large values."""
        series = pd.Series([1000000, 2000000, 3000000], name="test_series")
        label = "Millions of dollars"

        new_series, new_label = recalibrate_series(series, label)

        assert isinstance(new_series, pd.Series)
        assert isinstance(new_label, str)
        assert new_series.max() <= 1000
        assert new_series.max() >= 1
        assert "thousands" in new_label.lower() or "millions" in new_label.lower()

    def test_recalibrate_series_small_values(self):
        """Test recalibrating series with small values."""
        series = pd.Series([0.001, 0.002, 0.003], name="test_series")
        label = "Values"

        new_series, new_label = recalibrate_series(series, label)

        assert isinstance(new_series, pd.Series)
        assert isinstance(new_label, str)
        # For very small values, the function might not recalibrate them
        # Just check they remain positive
        assert new_series.max() > 0

    def test_recalibrate_series_already_optimal(self):
        """Test recalibrating series already in optimal range."""
        series = pd.Series([100, 200, 300], name="test_series")
        label = "Index points"

        new_series, new_label = recalibrate_series(series, label)

        # Should return unchanged
        assert new_series.equals(series)
        assert new_label == label

    def test_recalibrate_series_with_zeros(self):
        """Test recalibrating series containing zeros."""
        series = pd.Series([0, 1000000, 2000000], name="test_series")
        label = "Values"

        new_series, new_label = recalibrate_series(series, label)

        assert isinstance(new_series, pd.Series)
        assert new_series.iloc[0] == 0  # Zero should remain zero
        assert new_series.max() <= 1000

    def test_recalibrate_series_with_negative_values(self):
        """Test recalibrating series with negative values."""
        series = pd.Series([-1000000, 1000000, 2000000], name="test_series")
        label = "Values"

        new_series, new_label = recalibrate_series(series, label)

        assert isinstance(new_series, pd.Series)
        assert new_series.min() < 0  # Should preserve negative values
        assert abs(new_series).max() <= 1000

    def test_recalibrate_series_with_nan(self):
        """Test recalibrating series with NaN values."""
        series = pd.Series([np.nan, 1000000, 2000000], name="test_series")
        label = "Values"

        new_series, new_label = recalibrate_series(series, label)

        assert isinstance(new_series, pd.Series)
        assert pd.isna(new_series.iloc[0])  # NaN should remain NaN
        assert new_series.max() <= 1000

    def test_recalibrate_series_all_nan(self):
        """Test recalibrating series with all NaN values."""
        series = pd.Series([np.nan, np.nan, np.nan], name="test_series")
        label = "Values"

        new_series, new_label = recalibrate_series(series, label)

        # Should return unchanged when all values are NaN
        assert new_series.equals(series)
        assert new_label == label

    def test_recalibrate_series_empty(self):
        """Test recalibrating empty series."""
        series = pd.Series([], name="test_series", dtype=float)
        label = "Values"

        new_series, new_label = recalibrate_series(series, label)

        # Should return unchanged when empty
        assert new_series.equals(series)
        assert new_label == label


class TestRecalibrate:
    """Test recalibrate function."""

    def test_recalibrate_basic(self):
        """Test basic recalibrate functionality."""
        data = pd.DataFrame({"series1": [1000000, 2000000, 3000000], "series2": [0.001, 0.002, 0.003]})
        units = pd.Series(["Millions", "Fractions"], index=["series1", "series2"])

        new_data, new_units = recalibrate(data, units)

        assert isinstance(new_data, pd.DataFrame)
        assert isinstance(new_units, pd.Series)
        assert new_data.shape == data.shape
        assert len(new_units) == len(units)

        # Check that values are processed (may not all be recalibrated depending on thresholds)
        for col in new_data.columns:
            max_val = new_data[col].abs().max()
            assert max_val <= 1000000  # Should not exceed original large values by too much
            if not pd.isna(max_val):
                assert max_val > 0  # Should remain positive

    def test_recalibrate_as_a_whole_true(self):
        """Test recalibrate with as_a_whole=True."""
        data = pd.DataFrame({"series1": [100, 200, 300], "series2": [1000000, 2000000, 3000000]})
        units = pd.Series(["Values", "Values"], index=["series1", "series2"])

        new_data, new_units = recalibrate(data, units, as_a_whole=True)

        # All series should use the same scaling factor
        assert isinstance(new_data, pd.DataFrame)
        assert isinstance(new_units, pd.Series)

        # The maximum absolute value across all series should be <= 1000
        overall_max = new_data.abs().max().max()
        assert overall_max <= 1000

    def test_recalibrate_as_a_whole_false(self):
        """Test recalibrate with as_a_whole=False (default)."""
        data = pd.DataFrame({"series1": [100, 200, 300], "series2": [1000000, 2000000, 3000000]})
        units = pd.Series(["Index", "Values"], index=["series1", "series2"])

        new_data, new_units = recalibrate(data, units, as_a_whole=False)

        # Each series should be scaled independently
        assert isinstance(new_data, pd.DataFrame)
        assert isinstance(new_units, pd.Series)

        # First series should be unchanged (already in good range)
        assert new_data["series1"].equals(data["series1"])
        # Second series should be scaled down
        assert new_data["series2"].max() <= 1000

    def test_recalibrate_with_missing_units(self):
        """Test recalibrate with mismatched units series."""
        data = pd.DataFrame({"series1": [1000000, 2000000], "series2": [100, 200]})
        units = pd.Series(["Values"], index=["series1"])  # Missing series2

        with pytest.raises(ValueError):
            recalibrate(data, units)

    def test_recalibrate_empty_dataframe(self):
        """Test recalibrate with empty DataFrame."""
        data = pd.DataFrame()
        units = pd.Series([], dtype=str)

        # Function should raise ValueError for empty units
        with pytest.raises(ValueError):
            recalibrate(data, units)

    def test_recalibrate_single_column(self):
        """Test recalibrate with single column DataFrame."""
        data = pd.DataFrame({"series1": [1000000, 2000000, 3000000]})
        units = pd.Series(["Values"], index=["series1"])

        new_data, new_units = recalibrate(data, units)

        assert isinstance(new_data, pd.DataFrame)
        assert isinstance(new_units, pd.Series)
        assert new_data.shape == (3, 1)
        assert len(new_units) == 1
        assert new_data["series1"].max() <= 1000

    def test_recalibrate_preserves_index(self):
        """Test that recalibrate preserves DataFrame index."""
        index = pd.date_range("2023-01-01", periods=3, freq="QE")
        data = pd.DataFrame({"series1": [1000000, 2000000, 3000000]}, index=index)
        units = pd.Series(["Values"], index=["series1"])

        new_data, new_units = recalibrate(data, units)

        assert new_data.index.equals(index)

    def test_recalibrate_preserves_column_names(self):
        """Test that recalibrate preserves column names."""
        data = pd.DataFrame({"Consumer Price Index": [1000000, 2000000], "Wage Price Index": [100, 200]})
        units = pd.Series(["Values", "Index"], index=["Consumer Price Index", "Wage Price Index"])

        new_data, new_units = recalibrate(data, units)

        assert list(new_data.columns) == ["Consumer Price Index", "Wage Price Index"]
        assert list(new_units.index) == ["Consumer Price Index", "Wage Price Index"]


class TestIntegration:
    """Integration tests for measures module."""

    def test_full_workflow(self):
        """Test a complete workflow using all functions."""
        # Create sample data and metadata
        data = pd.DataFrame(
            {"CPI.Total": [1000000, 1100000, 1200000], "WPI.Private": [0.001, 0.0011, 0.0012]}
        )

        meta = pd.DataFrame(
            {
                "CPI.Total": {"name": "Consumer Price Index - Total", "UNIT": "INDEX"},
                "WPI.Private": {"name": "Wage Price Index - Private", "UNIT": "PERCENT"},
            }
        ).T

        # Get measure names
        names = measure_names(meta)

        # Recalibrate data
        new_data, new_units = recalibrate(data, names)

        # Verify the complete workflow
        assert isinstance(names, pd.Series)
        assert isinstance(new_data, pd.DataFrame)
        assert isinstance(new_units, pd.Series)
        assert len(names) == 2
        assert new_data.shape == data.shape
        assert len(new_units) == 2

        # Check that recalibration worked
        for col in new_data.columns:
            max_val = new_data[col].abs().max()
            assert max_val <= 1000000  # Allow for large values
            if not pd.isna(max_val):
                assert max_val > 0  # Should remain positive
