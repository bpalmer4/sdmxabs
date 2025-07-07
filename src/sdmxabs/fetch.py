"""Obtain data from the ABS SDMX API."""

from typing import Unpack
from xml.etree.ElementTree import Element

import numpy as np
import pandas as pd

from sdmxabs.download_cache import GetFileKwargs
from sdmxabs.flow_metadata import FlowMetaDict, build_key, code_lists, data_dimensions
from sdmxabs.xml_base import NAME_SPACES, URL_STEM, acquire_xml


def get_series_data(xml_series: Element, meta: pd.Series) -> pd.Series:
    """Extract observed data from the XML tree for a given single series."""
    series_elements = {}
    for item in xml_series.findall("gen:Obs", NAME_SPACES):
        # --- get the index and value from the XML item, or nan if not found
        index_container = item.find("gen:ObsDimension", NAME_SPACES)
        index_obs = index_container.attrib.get("value", None) if index_container is not None else None
        value_container = item.find("gen:ObsValue", NAME_SPACES)
        value_obs = value_container.attrib.get("value", None) if value_container is not None else None
        if index_obs is None or value_obs is None:
            continue
        series_elements[index_obs] = value_obs
    series: pd.Series = pd.Series(series_elements).sort_index()

    # --- if we can, make the series values numeric
    series = series.replace("", np.nan)
    try:
        series = pd.to_numeric(series)
    except ValueError:
        # If conversion fails, keep the series as is (it may contain useful non-numeric data)
        print(f"Could not convert series {meta.name} to numeric, keeping as is.")

    # --- if we can, make the index a PeriodIndex based on the frequency
    if "FREQ" in meta.index:
        freq = meta["FREQ"]
        if freq == "Annual":
            series.index = pd.PeriodIndex(series.index, freq="Y")
        elif freq == "Quarterly":
            series.index = pd.PeriodIndex(series.index, freq="Q")
        elif freq == "Monthly":
            series.index = pd.PeriodIndex(series.index, freq="M")
        elif freq in ("Daily", "Daily or businessweek"):
            series.index = pd.PeriodIndex(series.index, freq="D")
        else:
            print(f"Unknown frequency {freq}, leaving index as is.")

    return series


def decode_meta_value(meta_value: str, meta_id: str, dims: FlowMetaDict) -> str:
    """Decode a metadata value based on its ID and the relevant ABS codelist."""
    return_value = meta_value  # default to returning the raw value
    if meta_id in dims and "id" in dims[meta_id] and "package" in dims[meta_id]:
        cl_id = dims[meta_id]["id"]
        cl_package_type = dims[meta_id]["package"]
        if cl_id and cl_package_type == "codelist":
            cl = code_lists(cl_id)
            if meta_value in cl and "name" in cl[meta_value]:
                return_value = cl[meta_value]["name"]
    return return_value


def get_series_meta_data(xml_series: Element, series_count: int, dims: FlowMetaDict) -> tuple[str, pd.Series]:
    """Extract and decode metadata from the XML tree for one given series.

    Args:
        xml_series (Element): The XML element representing the series.
        series_count (int): The index of the series in the XML tree.
        dims (FlowMetaDict): Dictionary containing metadata dimensions and
            their associated codelist names.

    Returns:
        tuple[str, pd.Series]: A tuple containing the series label and a Series
            of metadata items for the series.

    """
    item_count = 0
    keys = []
    meta_items = {}
    key_sets = ("SeriesKey", "Attributes")
    for key_set in key_sets:
        attribs = xml_series.find(f"gen:{key_set}", NAME_SPACES)
        if attribs is None:
            print(f"No {key_set} found in series, skipping.")
            continue
        for item in attribs.findall("gen:Value", NAME_SPACES):
            # --- get the metadata item ID and value, or create a placeholder if missing
            meta_id = item.attrib.get("id", f"missing meta_id {series_count}-{item_count}")
            meta_value = item.attrib.get("value", f"missing meta_value {series_count}-{item_count}")
            keys.append(meta_value)
            decoded_meta_value = decode_meta_value(meta_value, meta_id, dims)
            meta_items[meta_id] = decoded_meta_value
            item_count += 1

    final_key = ".".join(keys)  # create a unique label for the series

    return final_key, pd.Series(meta_items).rename(final_key)


def extract(flow_id: str, tree: Element) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract data from the XML tree."""
    # Get the data dimensions for the flow_id, it provides entree to the metadata
    dims = data_dimensions(flow_id)

    meta = {}
    data = {}
    for series_count, xml_series in enumerate(tree.findall(".//gen:Series", NAME_SPACES)):
        if xml_series is None:
            print("No Series found in XML tree, skipping.")
            continue
        label, dataset = get_series_meta_data(
            # python typing is not smart enough to know that
            # xml_series is an ElementTree
            xml_series,
            series_count,
            dims,
        )
        if label in meta:
            # this shoudl not happen, but if it does, skip the series
            print(f"Duplicate series {label} in {flow_id} found, skipping.")
            continue
        meta[label] = dataset
        series = get_series_data(xml_series, dataset)
        series.name = label
        data[label] = series

    return pd.DataFrame(data), pd.DataFrame(meta).T  # data, meta


# === public functions ===
def fetch(
    flow_id: str,
    dims: dict[str, str] | None = None,
    constraints: dict[str, str] | None = None,  # not implemented yet
    *,
    validate: bool = False,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch data from the ABS SDMX API.

    Args:
        flow_id (str): The ID of the data flow from which to retrieve data items.
        dims (dict[str, str], optional): A dictionary of dimensions to select the
            data items. If None, the ABS fetch request will be for all data items,
            which can be slow.
        constraints (dict[str, str], optional): A dictionary of constraints to apply
            to the data items. If None, no constraints are applied.
        validate (bool): If True, print validation diagnostics for the proposed
            dimensions against the metadata requirements. Defaults to False.
        **kwargs (GetFileKwargs): Additional keyword arguments passed to acquire_xml().

    Returns: a tuple of two DataFrames:
        - The first DataFrame contains the fetched data.
        - The second DataFrame contains the metadata.

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML root is found in the response.

    """
    # --- prepare to get the XML root from the ABS SDMX API
    kwargs["modality"] = kwargs.get("modality", "prefer-cache")
    key = build_key(
        flow_id,
        dims,
        validate=validate,
    )
    _not_implemented = constraints
    url = f"{URL_STEM}/data/{flow_id}/{key}"
    xml_root = acquire_xml(url, **kwargs)
    return extract(flow_id, xml_root)


# --- quick and dirty testing
if __name__ == "__main__":
    # Example usage
    FLOW_ID = "WPI"
    DIMS = {
        "MEASURE": "3",
        "INDEX": "OHRPEB",
        "SECTOR": "7",
        "INDUSTRY": "TOT",
        "TSEST": "10",
        "REGION": "AUS",
        "FREQ": "Q",
    }

    FETCHED_DATA, FETCHED_META = fetch(
        FLOW_ID,
        dims=DIMS,
        validate=True,
        modality="prefer-url",
    )
    # Note: The transpose (.T) is used here to make the output more readable
    print("\nFetched Data:\n", FETCHED_DATA.T, sep="")
    print("\nFetched Metadata:\n", FETCHED_META.T, sep="")
