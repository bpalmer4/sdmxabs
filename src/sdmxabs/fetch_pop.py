"""Fetch Australian population data from the ABS SDMX API, either ERP or implied from National Accounts."""

from typing import Literal, Unpack

import numpy as np
import pandas as pd

from sdmxabs.download_cache import GetFileKwargs
from sdmxabs.fetch_gdp import fetch_gdp
from sdmxabs.fetch_selection import MatchType as Mt
from sdmxabs.fetch_selection import fetch_selection
from sdmxabs.flow_metadata import code_list_for_dim

# --- constants
FLOW_ID = "ERP_COMP_Q"
QUARTERS_IN_YEAR = 4
LAST_QUARTER_TOO_OLD_FOR_PROJECTION = 4


# --- private functions
def _erp_population(
    state: str,
    parameters: dict[str, str] | None,
    *,
    validate: bool,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch Estimated Resident Population (ERP) data from the ABS SDMX API."""
    selection_criteria = [
        ("Estimated Resident Population", "MEASURE", Mt.EXACT),
        (state, "REGION", Mt.EXACT),
        ("Q", "FREQ", Mt.EXACT),
    ]
    d, m = fetch_selection(FLOW_ID, selection_criteria, parameters, validate=validate, **kwargs)
    if len(m) != 1:
        raise ValueError(f"Expected 1 match for {state}, found {len(m)}")
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
    selection_criteria = [
        ("Original", "TSEST", Mt.EXACT),
        ("Current prices", "MEASURE", Mt.EXACT),
        ("GDP per capita", "DATA_ITEM", Mt.EXACT),
    ]
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
        if k not in m.columns:
            continue
        m.loc[name, k] = v
    return d, m


def _make_projection(data: pd.DataFrame) -> pd.DataFrame:
    """Make a naive projection of the population data forward to the current quarter.

    Return original data if (for example) the data is empty or too old for a reasonable
    projection. The projection is based on the annual growth over the latest quarters.

    """
    # --- validation/preparation
    if data.empty:
        return data  # No data to project
    current_quarter = pd.Timestamp.now().to_period("Q")
    last_period = data.index[-1]
    if last_period >= current_quarter:
        return data  # No projection needed
    if last_period < current_quarter - LAST_QUARTER_TOO_OLD_FOR_PROJECTION:
        return data  # Too old for projection
    annual_growth: float = data[data.columns[0]].astype(float).pct_change(QUARTERS_IN_YEAR).iloc[-1]
    if np.isnan(annual_growth):
        return data  # No valid growth rate
    new_periods = pd.period_range(start=last_period + 1, end=current_quarter, freq="Q")
    if new_periods.empty:
        return data  # No new periods to project

    # --- Make the projection
    compound_q_growth_factor = (1 + annual_growth) ** (1 / QUARTERS_IN_YEAR)
    new_data = pd.Series(
        data.iloc[-1, 0] * (compound_q_growth_factor ** np.arange(1, len(new_periods) + 1)), index=new_periods
    )
    return pd.DataFrame(data[data.columns[0]].combine_first(new_data))


def _state_name_from_abbrev(state: str) -> str:
    """Convert a state abbreviation to its full name."""
    # Abbreviation to full name mapping
    abbrev_to_name = {
        "nsw": "New South Wales",
        "vic": "Victoria",
        "qld": "Queensland",
        "sa": "South Australia",
        "wa": "Western Australia",
        "tas": "Tasmania",
        "nt": "Northern Territory",
        "act": "Australian Capital Territory",
    }
    for abbrev in ("aust", "aus", "au"):
        abbrev_to_name[abbrev] = "Australia"

    lower_case_abbrev = state.lower().strip()
    state_name = abbrev_to_name.get(lower_case_abbrev, state.strip())
    state_names = pd.DataFrame(code_list_for_dim(FLOW_ID, "REGION")).T
    if state_name not in state_names["name"].to_numpy():
        raise ValueError(f"Invalid state '{state_name}'. Available: {list(state_names['name'].unique())}")
    return state_name


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
            make a projection forward to the current quarter, based on growth over the last 4 quarters.
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
            data, meta = _erp_population("Australia", parameters, validate=validate, **kwargs)
        case "na":
            data, meta = _na_population(parameters, validate=validate, **kwargs)
        case _:
            raise ValueError(f"Invalid source '{source}'. Must be one of: ['erp', 'na']")

    # if requested, make a projection of the data
    if projection:
        data = _make_projection(data)

    return data, meta


def fetch_state_pop(
    state: str,
    parameters: dict[str, str] | None = None,
    *,
    projection: bool = False,
    validate: bool = False,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch state-level ERP population data from the ABS SDMX API.

    Args:
        state (str): State/territory name or case-insensitive abbreviation (e.g., "NSW", "Vic", "qld", etc.).
        parameters (dict[str, str] | None): Additional parameters for the API request,
            such as 'startPeriod'.
        projection (bool, optional): If True, make a projection forward to the current quarter
            based on growth over the last 4 quarters.
        validate (bool, optional): If True, validate the selection against the flow's
            required dimensions when generating the URL key. Defaults to False.
        **kwargs: Additional arguments passed to the fetch_selection() function

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the population data and metadata

    """
    # report the parameters used if requested
    verbose = kwargs.get("verbose", False)
    if verbose:
        print(f"fetch_state_pop(): {state=} {validate=} {kwargs=}")

    full_state_name = _state_name_from_abbrev(state)

    data, meta = _erp_population(full_state_name, parameters, validate=validate, **kwargs)

    if projection:
        data = _make_projection(data)

    return data, meta


if __name__ == "__main__":

    def test_fetch_pop() -> None:
        """Test function to fetch population data."""
        parameters = {"startPeriod": "2023-Q4"}
        sources: list[Literal["erp", "na"]] = ["erp", "na"]
        for source in sources:
            for proj in [False, True]:
                pop_data, _pop_meta = fetch_pop(source, parameters=parameters, projection=proj, verbose=False)
                print(f"{source} --> fetch_pop(): {pop_data.index[-1]} = {pop_data.tail(1).iloc[0, 0]:,.0f}")

    def test_fetch_state_pop() -> None:
        """Test function to fetch state population data."""
        # Test abbreviations
        for state in ["AUS", "VIC", "QLD"]:
            print(f"{state} --> {_state_name_from_abbrev(state)}")

        # Test fetch_state_pop
        data, _meta = fetch_state_pop("SA", projection=False, validate=False)
        print(f"SA: {data.index[-1]} = {data.tail(1).iloc[0, 0]:,.0f}")

        # Test projection
        data, _meta = fetch_state_pop("SA", projection=True)
        print(f"SA with projection: {data.index[-1]} = {data.tail(1).iloc[0, 0]:,.0f}")

    print("\n" + "=" * 50)
    test_fetch_pop()
    print("\n" + "=" * 50)
    test_fetch_state_pop()
    print("\n" + "=" * 50)
