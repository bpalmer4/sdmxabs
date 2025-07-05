"""Extract data from the ABS SDMX API."""

from typing import Unpack, cast
import xml.etree.ElementTree as ET
import pandas as pd
from numpy import nan

from sdmxabs.metadata import build_key, data_dimensions, code_lists
from sdmxabs.download_cache import GetFileKwargs
from sdmxabs.xml_base import acquire_xml, URL_STEM, NAME_SPACES


def populate_data(xml_series: ET.ElementTree, meta: pd.Series) -> pd.Series:
    """Extract data from the XML tree for a given series."""

    series_elements = {}
    for item in xml_series.findall("gen:Obs", NAME_SPACES):
        index_container = item.find("gen:ObsDimension", NAME_SPACES)
        index = (
            index_container.attrib.get("value", nan)
            if index_container is not None
            else nan
        )
        value_container = item.find("gen:ObsValue", NAME_SPACES)
        value = (
            value_container.attrib.get("value", nan)
            if value_container is not None
            else nan
        )
        if index is nan or value is nan:
            continue
        series_elements[index] = value
    series = pd.Series(series_elements).sort_index()

    # --- to do fix timeseries index
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


def populate_meta(
    xml_series: ET.ElementTree, series_count: int, dims: pd.DataFrame
) -> tuple[str, pd.Series]:
    """Extract metadata from the XML tree for a given series."""

    key_sets = ("SeriesKey", "Attributes")
    meta_items = {}
    item_count = 0
    keys = []
    for key_set in key_sets:
        attribs = xml_series.find(f"gen:{key_set}", NAME_SPACES)
        if attribs is None:
            print(f"No {key_set} found in series, skipping.")
            continue
        for item in attribs.findall("gen:Value", NAME_SPACES):
            # --- get the metadata item ID and value, or create a placeholder if missing
            meta_id = item.attrib.get("id", f"missing {series_count}-{item_count}")
            meta_value = item.attrib.get(
                "value", f"missing {series_count}-{item_count}"
            )
            keys.append(meta_value)

            # --- expand out the meta data to something approaching human readable, if possible
            if (
                meta_id in dims.index
                and "id" in dims.columns
                and "package" in dims.columns
            ):
                cl_id = dims.loc[meta_id, "id"]
                cl_package = dims.loc[meta_id, "package"]
                if cl_id and cl_package == "codelist":
                    cl = code_lists(cl_id)
                    if (
                        meta_value in cl.index
                        and "name" in cl.columns
                        and cl.loc[meta_value, "name"]
                    ):
                        # --- if the value is in the code list, use the name
                        meta_value = cast(str, cl.loc[meta_value, "name"])

            # --- add the metadata item to the dictionary
            meta_items[meta_id] = meta_value
            item_count += 1

    # --- create a unique label for the series based on the keys
    final_key = ".".join(keys)

    # --- and return
    return final_key, pd.Series(meta_items).rename(final_key)


def populate(flow_id: str, tree: ET.ElementTree) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract data from the XML tree."""

    # Get the data dimensions for the flow_id, it provides entree to the metadata
    dims = data_dimensions(flow_id)

    meta = {}
    data = {}
    for series_count, xml_series in enumerate(
        tree.findall(".//gen:Series", NAME_SPACES)
    ):
        if xml_series is None:
            print("No Series found in XML tree, skipping.")
            continue
        label, dataset = populate_meta(
            # python typing is not smart enough to know that xml_series is an ElementTree
            cast(ET.ElementTree, xml_series),
            series_count,
            dims,
        )
        if label in meta:
            # this shoudl not happen, but if it does, skip the series
            print(f"Duplicate series {label} in {flow_id} found, skipping.")
            continue
        meta[label] = dataset
        series = populate_data(cast(ET.ElementTree, xml_series), dataset)
        series.name = label
        data[label] = series

    return pd.DataFrame(data), pd.DataFrame(meta).T  # data, meta


def fetch(
    flow_id: str,
    dims: dict[str, str] | None = None,
    # constraints: dict[str, str] | None = None,  # not implemented yet
    *,
    validate: bool = False,
    **kwargs: Unpack[GetFileKwargs],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch data from the ABS SDMX API for a given flow ID and data dimensions.

    Args:
        flow_id (str): The ID of the data flow from which to retrieve data items.
        dims (dict[str, str], optional): A dictionary of dimensions to select the
            data items. If None, all dimensions are used (but note, this can be very
            slow, especially with large data flows).
        constraints (dict[str, str], optional): A dictionary of constraints to apply
            to the data items. If None, no constraints are applied.
        validate (bool): If True, print validation diagnostics for the proposed dims
            against the metadata requirements. Defaults to False.
        **kwargs (GetFileKwargs): Additional keyword arguments passed to acquire_xml().

    Returns: a tuple of two DataFrames:
        - The first DataFrame contains the fetched data.
        - The second DataFrame contains the metadata.

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML tree is found in the response.

    """
    # --- prepare to get the XML tree from the ABS SDMX API
    kwargs["modality"] = kwargs.get("modality", "prefer_cache")  # default prefer_cache
    key = build_key(
        flow_id,
        dims,
        validate=validate,
    )

    # --- get the XML tree from the ABS SDMX API
    url = f"{URL_STEM}/data/{flow_id}/{key}"
    tree = acquire_xml(url, **kwargs)

    # --- extract and return metadata and data from the XML tree
    return populate(flow_id, tree)


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

    # CONSTRAINTS = {"constraint1": "value1"}
    FETCHED_DATA, FETCHED_META = fetch(
        FLOW_ID,
        dims=DIMS,
        # constraints=CONSTRAINTS,
        validate=True,
        modality="prefer_url",
    )
    print("\nFetched Data:\n", FETCHED_DATA.T, sep="")
    print("\nFetched Metadata:\n", FETCHED_META.T, sep="")
