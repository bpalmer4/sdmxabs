"""Manage the measure names and units of measurement for ABS data (for naming the y-axis)."""

import math
from typing import cast

import numpy as np
import pandas as pd

# --- constants
INDICIES = {
    0: "Number",
    3: "Thousand",
    6: "Million",
    9: "Billion",
    12: "Trillion",
    # --- included for completeness, but not used in practice
    15: "Quadrillion",
    18: "Quintillion",
    21: "Sextillion",
    24: "Septillion",
    27: "Octillion",
    30: "Nonillion",
    33: "Decillion",
}
FACTORS = {v: k for k, v in INDICIES.items()}
DONT_RECALIBRATE = ("Percent", "Index Numbers", "Proportion")
MAX_FACTOR = max(INDICIES.keys())

RECALIBRATION_THRESHOLD = 1_000
FACTOR_INCREMENT = int(round(math.log10(RECALIBRATION_THRESHOLD), 0))
MIN_SAFE_FACTOR = FACTOR_INCREMENT


# --- private functions
def _are_all_columns_numeric(df: pd.DataFrame) -> bool:
    """Check if all columns in the DataFrame are numeric."""
    return df.shape[1] == df.select_dtypes(include=np.number).shape[1]


def _is_all_the_same(series: pd.Series) -> bool:
    """Check if all values in the Series are the same."""
    array = series.to_numpy()
    return array.shape[0] == 0 or (array[0] == array).all()


def _should_skip_recalibration(data: pd.DataFrame, label: str) -> bool:
    """Check if the data and label might be problematic in recalibration."""
    if any(x in label for x in DONT_RECALIBRATE):
        return True
    if data.empty:
        return True
    if not _are_all_columns_numeric(data):
        return True
    return bool(data.fillna(0).eq(0).all().all())  # all values are zero or NaN


def _refactor(data: pd.DataFrame | pd.Series, label: str) -> tuple[pd.DataFrame | pd.Series, str]:
    """Refactor the data and label so that the maximum absolute value is between 1 and 1000."""
    # --- make everything a DataFrame, copy as to not affect the original data
    revert_to_series = isinstance(data, pd.Series)
    d: pd.DataFrame = pd.DataFrame(data.copy())

    # --- dont bother cases
    if _should_skip_recalibration(d, label):
        return data, label

    # --- factorise the label
    ofactor = factor = next((FACTORS[x] for x in label.split(" ") if x in FACTORS), 0)
    if factor not in INDICIES:
        # If the factor is not in the indices, we assume it's a custom factor
        # and we will not change the label.
        return data, label

    # --- recalibrate value down (pushes factor up)
    while d.abs().to_numpy().max() > RECALIBRATION_THRESHOLD and factor < MAX_FACTOR:
        d = d / RECALIBRATION_THRESHOLD
        factor += FACTOR_INCREMENT

    # --- recalibrate value up (pulls factor down)
    while d.abs().to_numpy().max() <= 1 and factor >= MIN_SAFE_FACTOR:
        d = d * RECALIBRATION_THRESHOLD
        factor -= FACTOR_INCREMENT

    # --- check if the factor has changed
    if ofactor == factor:
        # No recalibration was necessary
        return data, label

    # --- update the label
    old_label = INDICIES.get(ofactor, "")
    new_label = INDICIES.get(factor, "")

    if old_label and new_label:
        label = label.replace(old_label, new_label)
    elif old_label:
        label = label.replace(old_label, "").strip()
        if not label:
            label = new_label
    elif new_label:
        label = f"{new_label} {label}" if label else new_label

    # --- revert to Series if necessary
    return d if not revert_to_series else d[d.columns[0]], label


# --- public functions
def measure_names(meta: pd.DataFrame) -> pd.Series:
    """Get the measure names for each row in the metadata DataFrame - (for y-axis labels).

    Args:
        meta (pd.DataFrame): The metadata DataFrame.

    Returns:
        pd.Series: A Series containing the measure names, indexed by the row labels.

    """
    series = pd.Series(dtype=str)
    duplicate_number: str = " Number"
    for label, row in meta.iterrows():
        name: str = str(label)  # worst case scenario
        if "UNIT_MEASURE" in row:
            name = str(row["UNIT_MEASURE"])  # a better base case
        if row.get("UNIT_MULT"):
            try:
                index = int(row["UNIT_MULT"])
                if index in INDICIES and index > 0:
                    name = f"{INDICIES[index]} {name}"  # best case
            except ValueError:
                pass
        name = name.removesuffix(duplicate_number)
        series[label] = name
    return series


def recalibrate(
    data: pd.DataFrame, units: pd.Series, *, as_a_whole: bool = False
) -> tuple[pd.DataFrame, pd.Series]:
    """Recalibrate the data so that its maximum value is between 1 and 1000.

    Args:
        units (pd.Series): The units of measure (as returned by measure_names()).
        data (pd.DataFrame): The data to recalibrate.
        as_a_whole (bool): If True, recalibrate the data as a whole, otherwise
            recalibrate each column separately.

    Returns:
        tuple[pd.Series, pd.DataFrame]: The recalibrated units and recalibrated data.

    Why recalibrate?
        So that the chart is easier to read and interpret, in units that are more familiar.

    """
    # --- data/argument validation
    if units.empty:
        raise ValueError("The units Series is empty.")
    if len(units) != len(data.columns):
        raise ValueError("The units Series must have the same length as the data DataFrame's columns.")
    if as_a_whole and not _is_all_the_same(units):
        raise ValueError("Cannot recalibrate as a whole when there are multiple units of measure.")
    if not all(x in data.columns for x in units.index):
        raise ValueError("The units Series must all be indexed by the data DataFrame's columns.")

    if as_a_whole:
        label = units.iloc[0]
        datax, label = _refactor(data, label)
        units.index = pd.Index([label] * len(units))
        return pd.DataFrame(datax), units

    for column in data.columns:
        label = units[column]
        series = data[column]
        seriesx, label = _refactor(series, label)
        data[column] = cast("pd.Series", seriesx)
        units[column] = label

    return data, units


def recalibrate_series(series: pd.Series, label: str) -> tuple[pd.Series, str]:
    """Recalibrate a Series with a label.

    Args:
        series (pd.Series): The Series to recalibrate.
        label (str): The label for the Series.

    Returns:
        tuple[pd.Series, str]: The recalibrated Series and label.

    """
    seriesx, label = _refactor(series, label)
    return cast("pd.Series", seriesx), label


if __name__ == "__main__":
    from sdmxabs.fetch import fetch

    def test_module() -> None:
        """Run the module tests."""
        for flow_id in ["LF", "ANA_AGG"]:  # complex metadata
            # - get some complicated metadata
            one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
            prev_period = one_year_ago.to_period("M") if flow_id == "LF" else one_year_ago.to_period("Q")
            prev = str(prev_period)
            prev = prev if flow_id == "LF" else prev[:-2] + "-" + prev[-2:]
            parameters = {"startPeriod": prev}
            data, meta = fetch(flow_id, parameters=parameters, modality="prefer-cache")

            # test the measure names
            units = measure_names(meta)
            print("Raw units before recalibration:")
            print(units.head(15))

            # test the recalibration
            data, units = recalibrate(data, units, as_a_whole=False)
            print("Recalibrated units:")
            print(units.head(15))

    test_module()
