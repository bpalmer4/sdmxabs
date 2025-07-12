"""Fetch multiple datasets from the SDMX API."""

from io import StringIO
from typing import Unpack

import pandas as pd

from sdmxabs.download_cache import CacheError, GetFileKwargs, HttpError
from sdmxabs.fetch import fetch

# --- private function
IndexInformation = tuple[type, str | None]  # (Index type, frequency if PeriodIndex)


def _validate_index_compatibility(
    data: pd.DataFrame, reference_index_info: IndexInformation | None
) -> IndexInformation:
    """Validate that the index of the current DataFrame is compatible with the reference index."""
    # establish the index information for the current DataFrame
    if isinstance(data.index, pd.PeriodIndex):
        current_index_info: IndexInformation = (type(data.index), data.index.freqstr)
    else:
        current_index_info = (type(data.index), None)

    # if this is the first DataFrame, set the reference index info
    if reference_index_info is None:
        reference_index_info = current_index_info

    # if this is not the first DataFrame, check for index compatibility
    elif current_index_info != reference_index_info:
        raise ValueError(
            f"Index mismatch: cannot mix {reference_index_info} "
            f"with {current_index_info}. "
            f"All datasets must have the same index type (e.g., all quarterly or all monthly data)."
        )

    return reference_index_info


def _extract(
    wanted: pd.DataFrame,
    parameters: dict[str, str] | None,
    *,
    validate: bool = False,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:  # data / metadata
    """Extract the data and metadata for each row in the dimensions DataFrame.

    Args:
        wanted (pd.DataFrame): DataFrame containing the dimensions to fetch.
                               DataFrame cells with NAN values will be ignored.
                               The DataFrame must have a populated 'flow_id' column.
        parameters (dict[str, str] | None): Additional parameters to pass to the fetch function.
                                           If None, no additional parameters are used.
        validate (bool, optional): If True, validate `wanted` against the flow's
            required dimensions when generating the URL key. Defaults to False.
        **kwargs: Additional keyword arguments passed to the underlying data fetching function.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A DataFrame with the fetched data and
                                        a DataFrame with the metadata.

    Raises:
        ValueError: if any input data is not as expected, or if incompatible
                   index types are detected (e.g., mixing quarterly and monthly data).

    Note: CacheError and HttpError are raised by the fetch function.
          These will be caught and reported to standard output.

    """
    # --- initial setup - empty return results
    return_meta = {}
    return_data = {}
    counter = 0
    reference_index_info: IndexInformation | None = None

    # --- loop over the rows of the wanted DataFrame
    for _index, row in wanted.iterrows():
        # --- get the arguments for the fetch (ignoring NaN values)
        row_dict: dict[str, str] = row.dropna().to_dict()
        flow_id = row_dict.pop("flow_id", "")
        if not flow_id:
            # --- if there is no flow_id, we will skip this row
            print(f"Skipping row with no flow_id: {row_dict}")
            continue

        # --- fetch the data and meta data for each row of the selection table
        try:
            data, meta = fetch(flow_id, dims=row_dict, parameters=parameters, validate=validate, **kwargs)
        except (CacheError, HttpError, ValueError) as e:
            # --- if there is an error, we will skip this row
            print(f"Error fetching {flow_id} with dimensions {row_dict}: {e}")
            continue
        if data.empty or meta.empty:
            # --- this should not happen, but if it does, we will skip this row
            print(f"No data for {flow_id} with dimensions {row_dict}")
            continue

        # --- validate index compatibility - including frequency compatibility for PeriodIndex
        reference_index_info = _validate_index_compatibility(data, reference_index_info)

        # --- manage duplicates
        for col in data.columns:
            counter += 1
            save_name = col
            if save_name in return_data:
                save_name += f"_{counter:03d}"
            return_data[save_name] = data[col]
            return_meta[save_name] = meta.loc[col]

    return pd.DataFrame(return_data), pd.DataFrame(return_meta).T


# --- public function
def fetch_multi(
    wanted: pd.DataFrame,
    parameters: dict[str, str] | None = None,
    *,
    validate: bool = False,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch multiple SDMX datasets based on a DataFrame of desired datasets.

    Args:
        wanted: A DataFrame with rows for each desired data set (of one or more series).
                Each row should contain the necessary identifiers to fetch the dataset.
                The columns will be 'flow_id', plus the ABS dimensions relevant to the flow.
                The 'flow_id' column is mandatory, and the rest are optional.
                Note: the DataFrame index is not used in the fetching process.
        parameters: A dictionary of additional parameters to pass to the fetch function.
        validate: If True, the function will validate dimensions and values against
                  the ABS SDMX API codelists. Defaults to False.
        **kwargs: Additional keyword arguments passed to the underlying data fetching function.

    Returns:
        A tuple containing two DataFrames:
        - The first DataFrame contains the fetched data.
        - The second DataFrame contains metadata about the fetched datasets.

    Raises:
        ValueError: If the 'flow_id' column is missing from the `wanted` DataFrame.

    Note:
        CacheError and HttpError are raised by the fetch function.
        These will be caught and reported to standard output.

    Note:
        The function validates that all datasets have compatible index types.
        A ValueError will be raised if incompatible index types are detected
        (e.g., mixing quarterly and monthly data).

    """
    # --- report the parameters used if requested
    verbose = kwargs.get("verbose", False)
    if verbose:
        print(f"fetch_multi(): {wanted=}, {parameters=}, {validate=}, {kwargs=}")

    # --- quick sanity checks
    if wanted.empty:
        print("wanted DataFrame is empty, returning empty DataFrames.")
        return pd.DataFrame(), pd.DataFrame()
    if "flow_id" not in wanted.columns:
        raise ValueError("The 'flow_id' column is required in the 'wanted' DataFrame.")

    # --- do the work
    return _extract(wanted, parameters, validate=validate, **kwargs)


if __name__ == "__main__":

    def module_test() -> None:
        """Run a simple test of the module."""
        wanted_text = """
        flow_id, MEASURE, INDEX, TSEST, REGION, DATA_ITEM, SECTOR, FREQ
        CPI,           3, 10001,    10,     50,         -,      -,    Q
        CPI,           3, 999902,   20,     50,         -,      -,    Q
        CPI,           3, 999903,   20,     50,         -,      -,    Q
        ANA_EXP,     DCH,      -,   20,    AUS,       FCE,    PHS,    Q
        ANA_EXP, PCT_DCH,      -,   20,    AUS,       FCE,    PHS,    Q
        """
        wanted = pd.read_csv(StringIO(wanted_text), dtype=str, skipinitialspace=True)
        parameters = {"startPeriod": "2020-Q1", "endPeriod": "2020-Q4", "detail": "full"}
        fetched_data, _fetched_meta = fetch_multi(
            wanted,
            parameters=parameters,
            validate=False,
            modality="prefer-url",
        )
        expected = (4, 5)
        if fetched_data.shape == expected:
            print(f"Test passed: {fetched_data.shape=}.")
        else:
            print(f"Test FAILED: data shape {fetched_data.shape} is unexpected {expected=}.")

    module_test()
