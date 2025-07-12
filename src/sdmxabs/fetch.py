"""Obtain data from the ABS SDMX API."""

from dataclasses import dataclass
from typing import Unpack
from xml.etree.ElementTree import Element

import numpy as np
import pandas as pd

from sdmxabs.download_cache import GetFileKwargs
from sdmxabs.flow_metadata import (
    FlowMetaDict,
    build_key,
    code_lists,
    data_dimensions,
    data_flows,
)
from sdmxabs.xml_base import NAME_SPACES, URL_STEM, acquire_xml

# --- constants
FREQUENCY_MAPPING = {
    "Annual": "Y",
    "Quarterly": "Q",
    "Monthly": "M",
    "Daily": "D",
}

XML_KEY_SETS = ("SeriesKey", "Attributes")
CODELIST_PACKAGE_TYPE = "codelist"


@dataclass
class MetadataContext:
    """Context object for processing XML metadata."""

    series_count: int
    label_elements: list[str]
    meta_items: dict[str, str]
    dims: FlowMetaDict
    item_count: int


# --- private functions
def _convert_to_period_index(series: pd.Series, frequency: str) -> pd.Series:
    """Convert series index to PeriodIndex if frequency is recognized."""
    if frequency not in FREQUENCY_MAPPING:
        return series
    freq_code = FREQUENCY_MAPPING[frequency]
    series.index = pd.PeriodIndex(series.index, freq=freq_code)
    return series


def _extract_observation_data(xml_series: Element) -> dict[str, str]:
    """Extract observation data from XML series element."""
    series_elements = {}
    for item in xml_series.findall("gen:Obs", NAME_SPACES):
        index_container = item.find("gen:ObsDimension", NAME_SPACES)
        value_container = item.find("gen:ObsValue", NAME_SPACES)

        index_obs = index_container.attrib.get("value") if index_container is not None else None
        value_obs = value_container.attrib.get("value") if value_container is not None else None

        if index_obs is not None and value_obs is not None:
            series_elements[index_obs] = value_obs

    return series_elements


def _get_series_data(xml_series: Element, meta: pd.Series) -> pd.Series:
    """Extract observed data from the XML for a given single series."""
    series_elements = _extract_observation_data(xml_series)
    series: pd.Series = pd.Series(series_elements)

    # --- if we can, make the series values numeric
    series = series.replace("", np.nan)
    try:
        series = pd.to_numeric(series)
    except ValueError:
        # If conversion fails, keep the series as is (it may contain useful non-numeric data)
        print(f"Could not convert series {meta.name} to numeric, keeping as is.")

    # --- convert to PeriodIndex if frequency is available, and sort the index
    frequency = meta.get("FREQ", "")
    return _convert_to_period_index(series, frequency).sort_index()


def _decode_meta_value(meta_value: str, meta_id: str, dims: FlowMetaDict) -> str:
    """Decode a metadata value based on its ID and the relevant ABS codelist."""
    # Early return if basic requirements not met
    if meta_id not in dims:
        return meta_value

    dim_config = dims[meta_id]
    if "id" not in dim_config or "package" not in dim_config:
        return meta_value

    # Early return if not a codelist
    if not dim_config["id"] or dim_config["package"] != CODELIST_PACKAGE_TYPE:
        return meta_value

    # Try to decode using codelist
    cl = code_lists(dim_config["id"])
    if meta_value in cl and "name" in cl[meta_value]:
        return cl[meta_value]["name"]

    return meta_value


def _process_xml_attributes(xml_series: Element, key_set: str, context: MetadataContext) -> None:
    """Process XML attributes for a given key set."""
    attribs = xml_series.find(f"gen:{key_set}", NAME_SPACES)
    if attribs is None:
        print(f"No {key_set} found in series, skipping.")
        return

    for item in attribs.findall("gen:Value", NAME_SPACES):
        # Extract meta_id, meta_value, and decode it - replace with text if missing
        meta_id = item.attrib.get("id", f"missing meta_id {context.series_count}-{context.item_count}")
        meta_value = item.attrib.get(
            "value", f"missing meta_value {context.series_count}-{context.item_count}"
        )
        context.label_elements.append(meta_value)
        context.meta_items[meta_id] = _decode_meta_value(meta_value, meta_id, context.dims)
        context.item_count += 1


def _get_series_meta_data(
    flow_id: str, xml_series: Element, series_count: int, dims: FlowMetaDict
) -> tuple[str, pd.Series]:
    """Extract and decode metadata from the XML tree for one given series.

    Args:
        flow_id (str): The ID of the data flow to which the series belongs.
        xml_series (Element): The XML element representing the series.
        series_count (int): The index of the series in the XML tree.
        dims (FlowMetaDict): Dictionary containing metadata dimensions and
            their associated codelist names.

    Returns:
        tuple[str, pd.Series]: A tuple containing the series label and a Series
            of metadata items for the series.

    """
    label_elements = [flow_id]
    flow_name = data_flows().get(flow_id, {"name": flow_id})["name"]
    meta_items = {"DATAFLOW": flow_name}

    context = MetadataContext(
        series_count=series_count,
        label_elements=label_elements,
        meta_items=meta_items,
        dims=dims,
        item_count=0,
    )

    for key_set in XML_KEY_SETS:
        _process_xml_attributes(xml_series, key_set, context)

    series_label = ".".join(context.label_elements)
    return series_label, pd.Series(context.meta_items).rename(series_label)


def _extract(flow_id: str, tree: Element) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract data from the XML tree."""
    # Get the data dimensions for the flow_id, it provides entree to the metadata
    dims = data_dimensions(flow_id)

    meta = {}
    data: dict[str, pd.Series] = {}
    for series_count, xml_series in enumerate(tree.findall(".//gen:Series", NAME_SPACES)):
        if xml_series is None:
            print("No Series found in XML tree, skipping.")
            continue
        label, meta_series = _get_series_meta_data(
            flow_id,
            # python typing is not smart enough to know that
            # xml_series is an ElementTree
            xml_series,
            series_count,
            dims,
        )
        series = _get_series_data(xml_series, meta_series)
        if label in data:
            # sometimes the SDMX API returns two incomplete series with the same metadata (our label)
            # my guess: the API may be inconsistent sometimes.
            series = series.combine_first(data[label])
        meta[label] = meta_series
        series.name = label
        data[label] = series

    return pd.DataFrame(data), pd.DataFrame(meta).T  # data, meta


# === public functions ===
def fetch(
    flow_id: str,
    dims: dict[str, str] | None = None,
    parameters: dict[str, str] | None = None,
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
        parameters (dict[str, str], optional): A dictionary of SDMX parameters to apply
            to the data request. Supported parameters include:
            - 'startPeriod': Start period for data filtering (e.g., '2020-Q1')
            - 'endPeriod': End period for data filtering (e.g., '2023-Q4')
            - 'detail': Level of detail ('full', 'dataonly', 'serieskeysonly', 'nodata')
            If None, no parameters are applied.
        validate (bool, optional): If True, validate `dims` against the flow's
            required dimensions when generating the URL key. Defaults to False.
        **kwargs (GetFileKwargs): Additional keyword arguments passed to acquire_xml().

    Returns: a tuple of two DataFrames:
        - The first DataFrame contains the fetched data.
        - The second DataFrame contains the metadata.

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML root is found in the response.
        ValueError: If invalid parameter values are provided.

    Notes:
        If the `dims` argument is not valid you should get a CacheError or HttpError.
        If the `flow_id` is not valid, you should get a ValueError.

    """
    # --- report the parameters used if requested
    verbose = kwargs.get("verbose", False)
    if verbose:
        print(f"fetch(): {flow_id=} {dims=} {parameters=} {validate=} {kwargs=}")

    # --- validate parameters
    valid_detail_values = {"full", "dataonly", "serieskeysonly", "nodata"}
    if parameters:
        detail_value = parameters.get("detail")
        if detail_value and detail_value not in valid_detail_values:
            raise ValueError(f"Invalid detail value '{detail_value}'. Must be one of: {valid_detail_values}")

    # --- prepare to get the XML root from the ABS SDMX API
    # prefer fresh data every time
    kwargs["modality"] = kwargs.get("modality", "prefer-url")
    key = build_key(
        flow_id,
        dims,
        validate=validate,
    )

    # --- build URL with optional parameters
    url = f"{URL_STEM}/data/{flow_id}/{key}"
    if parameters:
        url_params = []
        if "startPeriod" in parameters:
            url_params.append(f"startPeriod={parameters['startPeriod']}")
        if "endPeriod" in parameters:
            url_params.append(f"endPeriod={parameters['endPeriod']}")
        if "detail" in parameters:
            url_params.append(f"detail={parameters['detail']}")
        if url_params:
            url += "?" + "&".join(url_params)

    xml_root = acquire_xml(url, **kwargs)
    return _extract(flow_id, xml_root)


if __name__ == "__main__":

    def fetch_test() -> None:
        """Test the fetch() function from the ABS SDMX API."""
        flow_id = "WPI"
        dims = {
            "MEASURE": "3",
            "INDEX": "OHRPEB",
            "SECTOR": "7",
            "INDUSTRY": "TOT",
            "TSEST": "10",
            "REGION": "AUS",
            "FREQ": "Q",
        }

        # Test with parameters
        parameters = {"startPeriod": "2020-Q1", "endPeriod": "2023-Q4", "detail": "full"}

        fetched_data, fetched_meta = fetch(
            flow_id,
            dims=dims,
            parameters=parameters,
            validate=True,
            modality="prefer-url",
        )
        expected = (16, 1)
        if fetched_data.shape != expected:
            print(f"Test FAILED: data shape {fetched_data.shape} is unexpected {expected=}.")
        else:
            print(f"Test passed: {fetched_data.shape=}.")
        expected_tsest = "Original"
        if ("TSEST" in fetched_meta.columns) and fetched_meta["TSEST"].iloc[0] == expected_tsest:
            print("Test passed: TSEST has expected value.")
        else:
            print(
                f"Test FAILED: TSEST value {fetched_meta['TSEST'].iloc[0]} is unexpected {expected_tsest=}."
            )

    fetch_test()
