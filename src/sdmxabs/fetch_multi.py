"""Fetch multiple datasets from the SDMX API."""

from typing import Unpack

import pandas as pd

from sdmxabs.download_cache import CacheError, GetFileKwargs, HttpError
from sdmxabs.fetch import fetch


# --- private function
def extract(
    wanted: pd.DataFrame, *, validate: bool = False, **kwargs: Unpack[GetFileKwargs]
) -> tuple[pd.DataFrame, pd.DataFrame]:  # data / metadata
    """Extract the data and metadata for each row in the dimensions DataFrame.

    Args:
        wanted (pd.DataFrame): DataFrame containing the dimensions to fetch.
        validate (bool): If True, the function will validate the dimensions and values
                         against the ABS SDMX API codelists. Defaults to False.
        **kwargs: Additional keyword arguments passed to the underlying data fetching function.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A DataFrame with the fetched data and
                                        a DataFrame with the metadata.

    Raises:
        ValueError: if any input data is not as expected.

    Note: CacheError and HttpError are raised by the fetch function.
          These will be caught and reported to standard output.

    """
    # --- initial setup - empty return results
    return_meta = {}
    return_data = {}
    counter = 0

    # --- loop over the rows of the wanted DataFrame
    for _index, row in wanted.iterrows():
        # --- get the arguments for the fetch
        row_dict: dict[str, str] = row.to_dict()
        flow_id = row_dict.pop("flow_id", "")
        if not flow_id:
            # --- if there is no flow_id, we will skip this row
            print(f"Skipping row with no flow_id: {row_dict}")
            continue

        # --- fetch the data and meta data for each row of the selection table
        try:
            data, meta = fetch(flow_id, dims=row_dict, validate=validate, **kwargs)
        except (CacheError, HttpError, ValueError) as e:
            # --- if there is an error, we will skip this row
            print(f"Error fetching {flow_id} with dimensions {row_dict}: {e}")
            continue
        if data.empty or meta.empty:
            # --- this should not happen, but if it does, we will skip this row
            print(f"No data for {flow_id} with dimensions {row_dict}")
            continue

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

    Caution:
        The selected data should all have the same index. You cannot mix (for example)
        Quarterly and Monthly data in the same DataFrame.

    """
    # --- quick sanity checks
    if wanted.empty:
        print("wanted DataFrame is empty, returning empty DataFrames.")
        return pd.DataFrame(), pd.DataFrame()
    if "flow_id" not in wanted.columns:
        raise ValueError("The 'flow_id' column is required in the 'wanted' DataFrame.")

    # --- do the work
    return extract(wanted, validate=validate, **kwargs)
