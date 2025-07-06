"""Extract key metadata from the ABS SDMX API.

Note: the ABS has advised that Metadata is primarily available in XML.
(source: https://www.abs.gov.au/about/data-services/
         application-programming-interfaces-apis/data-api-user-guide)
"""

from functools import cache
from typing import Unpack

import pandas as pd

from sdmxabs.download_cache import GetFileKwargs
from sdmxabs.xml_base import NAME_SPACES, URL_STEM, acquire_xml


# --- functions
@cache
def data_flows(flow_id: str = "all", **kwargs: Unpack[GetFileKwargs]) -> pd.DataFrame:
    """Get the toplevel metadata from the ABS SDMX API.

    Args:
        flow_id (str): The ID of the dataflow to retrieve. Defaults to "all".
        **kwargs: Additional keyword arguments passed to acquire_url().

    Returns:
        pd.Series: A Series containing the dataflow IDs and names.

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML root is found in the response.

    """
    tree = acquire_xml(f"{URL_STEM}/dataflow/ABS/{flow_id}", **kwargs)

    df = {}
    for dataflow in tree.findall(".//str:Dataflow", NAME_SPACES):
        attributes = dataflow.attrib.copy()
        if "id" not in attributes:
            continue
        df_id = attributes.pop("id")
        name_elem = dataflow.find("com:Name", NAME_SPACES)
        df_name = name_elem.text if name_elem is not None else "(no name)"
        attributes["name"] = str(df_name)
        df[df_id] = attributes
    return pd.DataFrame(df).T.sort_index().rename_axis(index="dataflows")
    # Note: The returned DataFrame has the dataflow IDs as the index and
    # the attributes (like name, etc.) as columns.


@cache
def data_dimensions(flow_id: str, **kwargs: Unpack[GetFileKwargs]) -> pd.DataFrame:
    """Get the data dimensions metadata from the ABS SDMX API.

    Args:
        flow_id (str): The ID of the dataflow to retrieve dimensions for.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML root is found in the response.

    """
    tree = acquire_xml(f"{URL_STEM}/datastructure/ABS/{flow_id}", **kwargs)

    dimensions = {}
    for dim in tree.findall(".//str:Dimension", NAME_SPACES):
        dim_id = dim.get("id")
        dim_pos = dim.get("position")
        if dim_id is None or dim_pos is None:
            continue
        contents = {"position": dim_pos}
        if (lr := dim.find("str:LocalRepresentation", NAME_SPACES)) is not None and (
            enumer := lr.find("str:Enumeration/Ref", NAME_SPACES)
        ) is not None:
            contents = contents | enumer.attrib
        dimensions[dim_id] = contents
    return pd.DataFrame(dimensions).T.rename_axis(index="dimensions")


@cache
def code_lists(cl_id: str, **kwargs: Unpack[GetFileKwargs]) -> pd.DataFrame:
    """Get the code list metadata from the ABS SDMX API.

    Args:
        cl_id (str): The ID of the code list to retrieve.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML root is found in the response.

    """
    tree = acquire_xml(f"{URL_STEM}/codelist/ABS/{cl_id}", **kwargs)

    codes = {}
    for code in tree.findall(".//str:Code", NAME_SPACES):
        code_id = code.get("id")
        if code_id is None:
            continue
        elements = {}
        name = code.find("com:Name", NAME_SPACES)
        elements["name"] = name.text if name is not None else None
        parent = code.find("str:Parent", NAME_SPACES)
        parent_id = None
        if parent is not None:
            ref = parent.find("Ref", NAME_SPACES)
            if ref is not None:
                parent_id = ref.get("id")
            elements["parent"] = parent_id
        codes[code_id] = elements

    return pd.DataFrame(codes).T.sort_index().rename_axis(index=cl_id)


def problem_code(dim: str, value: str, required: pd.DataFrame) -> str:
    """Check if a value for a dimension is in the codelist for the flow_id.

    Args:
        dim (str): The dimension to check.
        value (str): The value to check.
        required (pd.DataFrame): The required dimensions for the dataflow.

    Returns:
        str: The name of the codelist if the value is not found, otherwise an empty string.

    """
    package = required.loc[dim, "package"]
    if package and package == "codelist":
        codelist_name = str(required.loc[dim, "id"])
        if codelist_name and value not in code_lists(codelist_name).index:
            return f"Code '{value}' for dimension '{dim}' is not valid for codelist '{codelist_name}'"
    return ""  # empty string if no problem


def publish_alerts(flow_id: str, missing: list[str], extra: list[str], wrong: list[str]) -> None:
    """Publish alerts for missing, extra, or wrongly valued dimensions."""
    if missing:
        print(f"Missing dimensions for {flow_id}: {missing}")
    if extra:
        print(f"Extra dimensions for {flow_id}: {extra}")
    if wrong:
        for w in wrong:
            print(w)


def build_key(flow_id: str, dimensions: dict[str, str] | None, *, validate: bool = False) -> str:
    """Build a key for a dataflow based on its dimensions.

    Args:
        flow_id (str): The identifier for the dataflow.
        dimensions (dict[str, str] | None): A dictionary of dimension IDs and
            their values. If None, the returned key will be "all".
        validate (bool): If True, validate the dimensions against the required
            dimensions for the flow_id.

    Returns:
        str: A string representing the key for the requested data.

    """
    # --- check validity of inputs
    if not flow_id or flow_id not in data_flows().index:
        raise ValueError("A valid flow_id must be specified")

    if dimensions is None:
        return "all"

    position = "position"
    required = data_dimensions(flow_id)
    if required is None or required.empty or position not in required.columns:
        return "all"
    required[position] = required[position].astype(int)  # for the sort

    # --- build key using the required dimensions
    keys = []
    wrong = []
    for dim in required.sort_values(position).index:
        if dim in dimensions:
            value = dimensions[dim]
            issues = [problem_code(dim, v, required) for v in value.split("+")]
            issues = [i for i in issues if i]  # filter out empty strings
            if not issues:
                keys.append(f"{value}")
                continue
            wrong += issues
        keys.append("")

    # --- alert to any dimensional coding issues
    if validate:
        missing = [k for k in required.index if k not in dimensions]
        extra = [k for k in dimensions if k not in required.index]
        publish_alerts(flow_id, missing, extra, wrong)

    # --- if there are no keys, return "all"
    if keys:
        return f"{'.'.join(keys)}"  # the dot separated key
    return "all"


if __name__ == "__main__":
    # --- data_flows -- all dataflows
    FLOWS = data_flows(modality="prefer_cache")
    print("Length:", len(FLOWS))
    print("Columns:", FLOWS.columns)
    print("Example rows:\n", FLOWS[FLOWS.name.str.contains("National Accounts")], sep="")

    # --- data_flows -- specific dataflow
    FLOWS = data_flows(flow_id="WPI", modality="prefer_cache")
    print(len(FLOWS))
    print(FLOWS)

    # --- data_dimensions
    DIMENSIONS = data_dimensions("WPI", modality="prefer_cache")
    print(len(DIMENSIONS))
    print(DIMENSIONS)

    # --- code lists
    CODE_LISTS = code_lists("CL_WPI_MEASURES", modality="prefer_cache")
    print(len(CODE_LISTS))
    print(CODE_LISTS)

    CODE_LISTS = code_lists("CL_WPI_PCI", modality="prefer_cache")
    print(len(CODE_LISTS))
    print(CODE_LISTS)

    CODE_LISTS = code_lists("CL_SECTOR", modality="prefer_cache")
    print(len(CODE_LISTS))
    print(CODE_LISTS)

    CODE_LISTS = code_lists("CL_ANZSIC_2006", modality="prefer_cache")
    print(len(CODE_LISTS))
    print(CODE_LISTS)

    CODE_LISTS = code_lists("CL_TSEST", modality="prefer_cache")
    print(len(CODE_LISTS))
    print(CODE_LISTS)

    CODE_LISTS = code_lists("CL_STATE", modality="prefer_cache")
    print(len(CODE_LISTS))
    print(CODE_LISTS)

    CODE_LISTS = code_lists("CL_FREQ", modality="prefer_cache")
    print(len(CODE_LISTS))
    print(CODE_LISTS)

    # --- build_key
    KEY = build_key("WPI", {"FREQ": "Q", "REGION": "NSW", "MEASURES": "CPI"}, validate=True)
    print("Key:", KEY)

    KEY = build_key("WPI", {"FREQ": "Q", "REGION": "1+2", "MEASURES": "CPI"}, validate=False)
    print("Key:", KEY)
