"""Select one or more data series from the ABS Catalogue based on search criteria."""

import re
from collections.abc import Sequence
from enum import Enum
from typing import Unpack

import pandas as pd

from sdmxabs.download_cache import GetFileKwargs
from sdmxabs.fetch_multi import fetch_multi
from sdmxabs.flow_metadata import FlowMetaDict, code_lists, data_dimensions, data_flows


# --- some types specific to this module
class MatchType(Enum):
    """Enumeration for match types."""

    EXACT = 1
    PARTIAL = 2
    REGEX = 3


MatchItem = tuple[str, str, MatchType]  # pattern, dimension, MatchType
MatchCriteria = Sequence[MatchItem]  # Sequence of tuples containing (pattern, dimension, MatchType)


# --- private functions
def _package_codes(codes: list[str], dimension: str, return_dict: dict[str, str]) -> None:
    """Package the codes into the return dictionary for a given dimension.

    If the dimension already exists in the return_dict, we will intersect the newly
    identified  codes with the existing codes. If the intersection is a null set, the
    dimension will be removed from the return_dict (ie. the global match).

    Note: multiple matched codes are separated by a '+' sign in the return_dict.

    """
    if dimension in return_dict:
        previous = set(return_dict[dimension].split("+"))
        codes = list(previous.intersection(set(codes)))
        if not codes:
            del return_dict[dimension]  # no intersecting matches, remove dimension
    if codes:
        return_dict[dimension] = "+".join(sorted(set(codes)))


def _get_codes(
    code_list_dict: FlowMetaDict,
    pattern: str,
    match_type: MatchType = MatchType.PARTIAL,
) -> list[str]:
    """Obtain all codes matching the pattern."""
    codes = []
    for code, code_list in code_list_dict.items():
        name = code_list.get("name", "")
        if not name:
            # should not happen, but if it does, raise an error
            raise ValueError(f"Code '{code}' has no name in codelist")
        match match_type:
            case MatchType.EXACT:
                if name == pattern:
                    codes.append(code)
            case MatchType.PARTIAL:
                # Case-insensitive partial match
                if pattern.lower() in name.lower():
                    codes.append(code)
            case MatchType.REGEX:
                try:
                    if re.search(pattern, name):
                        codes.append(code)
                except re.error as e:
                    raise ValueError(f"Invalid regex pattern '{pattern}': {e}") from e
    return codes


def _validate_flow_and_dimensions(flow_id: str) -> FlowMetaDict:
    """Validate flow_id and return dimensions.

    Args:
        flow_id (str): The ID of the data flow to validate.

    Returns:
        FlowMetaDict: Dictionary containing the flow's dimensions.

    Raises:
        ValueError: If the flow_id is not valid or has no dimensions.

    """
    if flow_id not in data_flows():
        raise ValueError(f"Invalid flow_id: {flow_id}.")
    dimensions = data_dimensions(flow_id)
    if not dimensions:
        raise ValueError(f"No dimensions found for flow_id: {flow_id}.")
    return dimensions


def _validate_dimension(dimension: str, flow_id: str, dimensions: FlowMetaDict) -> str:
    """Validate dimension and return codelist name if valid.

    Args:
        dimension (str): The dimension name to validate.
        flow_id (str): The flow ID for error messages.
        dimensions (FlowMetaDict): Dictionary containing the flow's dimensions.

    Returns:
        str: The codelist name if valid.

    Raises:
        ValueError: If dimension is not found or doesn't have a codelist.

    """
    if dimension not in dimensions:
        raise ValueError(f"Dimension '{dimension}' not found for flow '{flow_id}'")

    dim_dict = dimensions[dimension]
    if "package" not in dim_dict or dim_dict["package"] != "codelist" or "id" not in dim_dict:
        raise ValueError(f"Dimension '{dimension}' does not have a codelist for flow '{flow_id}'")

    return dim_dict.get("id", "")


def _process_match_criteria(
    criteria: MatchCriteria, flow_id: str, dimensions: FlowMetaDict
) -> dict[str, str]:
    """Process match criteria and build the result dictionary.

    Args:
        criteria (MatchCriteria): The match criteria to process.
        flow_id (str): The flow ID for error messages.
        dimensions (FlowMetaDict): Dictionary containing the flow's dimensions.

    Returns:
        dict[str, str]: Dictionary of dimension codes.

    """
    result_dict: dict[str, str] = {}

    for pattern, dimension, match_type in criteria:
        code_list_name = _validate_dimension(dimension, flow_id, dimensions)
        codes = _get_codes(code_lists(code_list_name), pattern, match_type)
        if codes:
            _package_codes(codes, dimension, result_dict)

    return result_dict


# --- public function
def match_item(
    pattern: str,
    dimension: str,
    match_type: MatchType = MatchType.PARTIAL,
) -> MatchItem:
    """Create a new MatchItem for use in select_items() and fetch_selection().

    Args:
        pattern (str): The pattern to match.
        dimension (str): The dimension to match against.
        match_type (MatchType, optional): The type of match to perform. Defaults to MatchType.EXACT.

    Returns:
        MatchElement: A tuple representing the match element.

    """
    return (pattern, dimension, match_type)


def make_wanted(
    flow_id: str,
    criteria: MatchCriteria,
) -> pd.DataFrame:
    """Build a `wanted` Dataframe for use by fetch_multi() by matching flow metadata.

    Args:
        flow_id (str): The ID of the data flow to select items from.
        criteria (MatchCriteria): A sequence of tuples containing the pattern,
            dimension name, and match-type (exact, partial, or regex).

    Returns:
        pd.DataFrame: A DataFrame containing the selected items, which can be dropped
            into the call of the function fetch_multi().

    Raises:
        ValueError: If the flow_id is not valid or if no items match the criteria.

    Notes:
    -   Should build a one line DataFrame. This Frame may select multiple data series,
        when passed to fetch_multi. It also can be concatenated with other DataFrames
        to build a larger selection.
    -   If two match elements refer to the same dimension, only the `intersection` of the
        matches will be returned.

    """
    dimensions = _validate_flow_and_dimensions(flow_id)
    result_dict = _process_match_criteria(criteria, flow_id, dimensions)

    # Add flow_id and return as DataFrame
    result_dict["flow_id"] = flow_id
    return pd.DataFrame([result_dict]).astype(str)


def fetch_selection(
    flow_id: str,
    criteria: MatchCriteria,
    parameters: dict[str, str] | None = None,
    *,
    validate: bool = False,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch data based on a selection criteria for items.

    Args:
        flow_id (str): The ID of the data flow to fetch.
        criteria (MatchCriteria): A sequence of match criteria to filter the data.
        parameters (dict[str, str] | None, optional): Additional parameters for the fetch.
        validate (bool, optional): If True, validate the selection against the flow's
            required dimensions when generating the URL key. Defaults to False.
        **kwargs: Additional keyword arguments for the fetch_multi function.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the fetched data and metadata.

    """
    verbose = kwargs.get("verbose", False)
    if verbose:
        print(f"fetch_selection(): {flow_id=} {criteria=} {parameters=} {validate=} {kwargs=}")

    selection = make_wanted(flow_id, criteria)
    return fetch_multi(selection, parameters, validate=validate, **kwargs)


if __name__ == "__main__":

    def test_module() -> None:
        """Test the match_item function."""
        # --- test match_item()
        item = match_item("Australia", "REGION", MatchType.EXACT)
        if item != ("Australia", "REGION", MatchType.EXACT):
            print(f"Test failed: {item}")
        else:
            print("Test passed, match_item() works as expected.")

        # --- specify a selection from the Wage Price Index (WPI) data flow
        mat_criteria = []
        mat_criteria.append(match_item("Australia", "REGION", MatchType.EXACT))
        mat_criteria.append(
            match_item(
                "Percentage change from corresponding quarter of previous year", "MEASURE", MatchType.EXACT
            )
        )
        mat_criteria.append(
            match_item("Total hourly rates of pay excluding bonuses", "INDEX", MatchType.PARTIAL)
        )
        mat_criteria.append(match_item("Seas|Trend", "TSEST", MatchType.REGEX))
        mat_criteria.append(match_item("13-Industry aggregate", "INDUSTRY", MatchType.EXACT))
        mat_criteria.append(match_item("Private and Public", "SECTOR", MatchType.EXACT))

        # --- test the selection
        expected_count = 2  # expecting two data series
        parameters = {"startPeriod": "2020-Q1", "endPeriod": "2020-Q4", "detail": "full"}
        data, meta = fetch_selection("WPI", mat_criteria, parameters=parameters, verbose=False)
        if len(data.columns) == expected_count and meta.shape[0] == expected_count:
            print("Test passed: Data and metadata have expected dimensions.")
        else:
            print(f"Test FAILED: Data columns {len(data.columns)}, Metadata rows {meta.shape[0]}")
        expected_seasonal = {"Trend", "Seasonally Adjusted"}
        if set(meta.TSEST.to_list()) == expected_seasonal:
            print("Test passed: TSEST has expected values.")
        else:
            print(f"Test FAILED: TSEST values {meta.TSEST.to_list()}")
        expected_shape = (4, 2)  # 4 quarters of data, over two series
        if data.shape == expected_shape:
            print("Test passed: Fetched data has expected shape.")
        else:
            print(f"Test FAILED: Fetched data shape {data.shape=} is unexpected {expected_shape=}.")

    test_module()
