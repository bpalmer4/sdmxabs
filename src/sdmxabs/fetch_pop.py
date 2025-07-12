"""Fetch Australian population data from the ABS SDMX API, either ERP or implied from National Accounts.

The module also allows for a naive projection of the population data forward to the current quarter,
based on the annual growth over the latest 12 months of data.
"""

from typing import Literal, Unpack

import numpy as np
import pandas as pd

from sdmxabs.download_cache import GetFileKwargs
from sdmxabs.fetch_gdp import fetch_gdp
from sdmxabs.fetch_selection import MatchType as Mt
from sdmxabs.fetch_selection import fetch_selection, match_item

# --- constants
QUARTERS_IN_YEAR = 4
LAST_QUARTER_TOO_OLD_FOR_PROJECTION = 4


# --- private functions
def _erp_population(
    parameters: dict[str, str] | None,
    *,
    validate: bool,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch Estimated Resident Population (ERP) data from the ABS SDMX API."""
    flow_id = "ERP_COMP_Q"
    selection_criteria = []
    selection_criteria.append(match_item("Estimated Resident Population", "MEASURE", Mt.EXACT))
    selection_criteria.append(match_item("Australia", "REGION", Mt.EXACT))
    selection_criteria.append(match_item("Q", "FREQ", Mt.EXACT))
    d, m = fetch_selection(flow_id, selection_criteria, parameters, validate=validate, **kwargs)
    d.columns = m.index = pd.Index(["Estimated Resident Population"])
    return d, m


def _na_population(
    parameters: dict[str, str] | None,
    *,
    validate: bool,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extrapolate population from the National Accounts data from the ABS SDMX API."""
    # --- Fetch GDP data
    gdp, _ = fetch_gdp(
        seasonality="o",
        price_measure="cp",
        parameters=parameters,
        validate=validate,
        **kwargs,
    )

    # --- Fetch GDP per capita data
    selection_criteria = []
    selection_criteria.append(match_item("Original", "TSEST", Mt.EXACT))
    selection_criteria.append(match_item("Current prices", "MEASURE", Mt.EXACT))
    selection_criteria.append(match_item("GDP per capita", "DATA_ITEM", Mt.EXACT))
    flow_id = "ANA_AGG"
    d, m = fetch_selection(flow_id, selection_criteria, parameters, validate=validate, **kwargs)

    # --- Extrapolate population from the above two series, Fudge meta-data
    name = "Implicit Population from GDP"
    gdp_s = gdp[gdp.columns[0]].astype(float)
    gdppc_s = d[d.columns[0]].astype(float)
    pop_s = gdp_s.div(gdppc_s) * 1_000
    d = pd.DataFrame(pop_s)
    d.columns = m.index = pd.Index([name])
    for k, v in {"UNIT_MEASURE": "NUM", "UNIT_MULT": "3", "DATA_ITEM": name}.items():
        m.loc[name, k] = v
    return d, m


def _make_projection(data: pd.DataFrame) -> pd.DataFrame:
    """Make a projection of the population data forward to the current quarter."""
    # --- validations
    if data.empty:
        raise ValueError("No data available to make a projection.")

    current_quarter = pd.Timestamp.now().to_period("Q")
    last_period = data.index[-1]
    if last_period >= current_quarter or last_period < current_quarter - LAST_QUARTER_TOO_OLD_FOR_PROJECTION:
        raise ValueError(
            f"Data is not recent enough for projection. "
            f"Latest data is {last_period}, current quarter is {current_quarter}."
        )

    annual_growth: float = data[data.columns[0]].astype(float).pct_change(QUARTERS_IN_YEAR).iloc[-1]
    if np.isnan(annual_growth):
        raise ValueError("Insufficient data to calculate annual growth for projection.")

    # --- Make the projection
    compound_growth_factor = (1 + annual_growth) ** (1 / QUARTERS_IN_YEAR)
    new_periods = pd.period_range(start=last_period + 1, end=current_quarter, freq="Q")
    if new_periods.empty:
        return data  # No new periods to project
    new_data = pd.Series(
        data.iloc[-1, 0] * (compound_growth_factor ** np.arange(1, len(new_periods) + 1)), index=new_periods
    )
    return pd.DataFrame(data[data.columns[0]].combine_first(new_data))


# --- public functions
def fetch_pop(
    source: Literal["erp", "na"] = "erp",
    parameters: dict[str, str] | None = None,
    *,
    projection: bool = False,
    validate: bool = False,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch Estimated Resident Population (ERP) data from the ABS SDMX API.

    Args:
        source (str): Source of the population data:
            - "erp": ABS published Estimated Resident Population (default)
            - "na": Implied population from the ABS National Accounts
        parameters (dict[str, str] | None): Additional parameters for the API request,
            such as 'startPeriod'.
        projection (bool, optional): If True, and data is available for the most recent year,
            make a projection forward to the current quarter, based on the latest growth
            over 4 quarters.
        validate (bool, optional): If True, validate the selection against the flow's
            required dimensions when generating the URL key. Defaults to False.
        **kwargs: Additional arguments passed to the fetch_selection() function

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the population data and metadata

    """
    # report the parameters used if requested
    verbose = kwargs.get("verbose", False)
    if verbose:
        print(f"fetch_pop(): {source=} {validate=} {kwargs=}")

    # build a selection criteria and fetch the relevant data
    match source:
        case "erp":
            data, meta = _erp_population(parameters, validate=validate, **kwargs)
        case "na":
            data, meta = _na_population(parameters, validate=validate, **kwargs)
        case _:
            raise ValueError(f"Invalid source '{source}'. Must be one of: ['erp', 'na']")

    # if requested, make a projection of the data
    if projection:
        data = _make_projection(data)

    return data, meta


if __name__ == "__main__":

    def test_fetch_pop() -> None:
        """Test function to fetch population data."""
        parameters = {"startPeriod": "2020-Q1"}
        pop_data, pop_meta = fetch_pop(source="na", parameters=parameters, projection=True)
        print(pop_data, "\n", pop_meta.T)

    test_fetch_pop()
