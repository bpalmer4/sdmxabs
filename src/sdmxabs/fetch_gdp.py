"""Fetch GDP data from the ABS SDMX API with specific seasonality and price measure options."""

from typing import Literal, Unpack

import pandas as pd

from sdmxabs.download_cache import GetFileKwargs
from sdmxabs.fetch_selection import MatchType as Mt
from sdmxabs.fetch_selection import fetch_selection, match_item

# --- constants
PRICE_MAP = {"cvm": "Chain volume measures", "cp": "Current prices"}  # MEASURE
SEAS_MAP = {"o": "Original", "s": "Seasonally Adjusted", "t": "Trend"}  # TSEST


# --- public functions
def fetch_gdp(
    seasonality: Literal["o", "s", "t"] = "o",
    price_measure: Literal["cp", "cvm"] = "cp",
    parameters: dict[str, str] | None = None,
    *,
    validate: bool = False,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch quarterly GDP data in $ from the ABS SDMX API.

    Args:
        seasonality (str): Type of seasonal adjustment to apply:
            - "o": Original data without seasonal adjustment (default)
            - "s": Seasonally adjusted data
            - "t": Trend data
        price_measure (str): Price measure type:
            - "cp": Current prices (default)
            - "cvm": Chain volume measures
        parameters (dict[str, str] | None): Additional parameters for the API request,
            such as 'startPeriod'.
        validate (bool, optional): If True, validate the selection against the flow's
            required dimensions when generating the URL key. Defaults to False.
        **kwargs: Additional arguments passed to the fetch_selection() function

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the GDP data and metadata

    Raises:
        ValueError: If invalid seasonality or price_measure values are provided

    """
    # report the parameters used if requested
    verbose = kwargs.get("verbose", False)
    if verbose:
        print(f"fetch_gdp(): {seasonality=}, {price_measure=} {validate=} {kwargs=}")

    # Validate inputs
    if seasonality not in SEAS_MAP:
        error = f"Invalid '{seasonality=}'. Must be one of: {list(SEAS_MAP.keys())}"
        raise ValueError(error)
    if price_measure not in PRICE_MAP:
        error = f"Invalid '{price_measure=}'. Must be one of: {list(PRICE_MAP.keys())}"
        raise ValueError(error)

    # build a selection criteria
    selection_criteria = []
    selection_criteria.append(match_item(SEAS_MAP[seasonality], "TSEST", Mt.EXACT))
    selection_criteria.append(match_item(PRICE_MAP[price_measure], "MEASURE", Mt.EXACT))
    selection_criteria.append(match_item("Gross domestic product", "DATA_ITEM", Mt.EXACT))

    # return the data
    flow_id = "ANA_AGG"
    return fetch_selection(flow_id, selection_criteria, parameters, validate=validate, **kwargs)


if __name__ == "__main__":

    def test_fetch_gdp() -> None:
        """Test function to fetch GDP data.

        NOTE: The trend data is not available after 2019-Q1???
              Not sure why? [Report: as at 12 July 2025]

        """
        failed = False
        parameters = {"startPeriod": "2019-Q1"}
        print(f"Testing with {parameters=}")
        for seasonality in ("o", "s", "t"):
            for price_measure in ("cp", "cvm"):
                gdp_data, metadata = fetch_gdp(
                    seasonality=seasonality,
                    price_measure=price_measure,
                    parameters=parameters,
                    verbose=False,
                )
                if gdp_data.empty or metadata.empty:
                    print(f"Test FAILED for {seasonality=} {price_measure=}")
                    failed = True
                    continue
                if len(metadata) != 1:
                    print(f"Test FAILED for {seasonality=} {price_measure=}")
                    failed = True
                if metadata.iloc[0]["TSEST"].lower()[0] != seasonality:
                    print(f"Test FAILED for {seasonality=}")
                    failed = True
                if metadata.iloc[0]["MEASURE"] != PRICE_MAP[price_measure]:
                    print(f"Test FAILED for {price_measure=}")
                    failed = True
                print(f"Test passed for {seasonality=} {price_measure=} ==> {len(gdp_data)} rows")
        if not failed:
            print("Test passed for all combinations of seasonality and price_measure")

    test_fetch_gdp()
