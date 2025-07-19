"""Extract key metadata from the ABS SDMX API.

Key functions:
- data_flows(): Get the metadata for all dataflows.
- structure_ident(): Get the data structure ID for a specific dataflow.
- data_structures(): Get the data structure (ie. the dimensions and attributes metadata)
    for a data structure identifier.
- structure_from_flow_id(): Get the structure metadata for a specific dataflow.
    Combines the two steps of getting the structure_ident() and then the
    data_structures() metadata.
- code_lists(): Get the code list metadata (code=name pairs) for a specific code list.
- code_list_for(): Get the code list for a specific dimension or attribute in a data
    flow.
- frame(): Convert a FlowMetaDict to a pandas DataFrame for easier viewing.

Note: the ABS has advised that Metadata is primarily available in XML.
(source: https://www.abs.gov.au/about/data-services/
         application-programming-interfaces-apis/data-api-user-guide)
"""

from functools import cache
from typing import Unpack

import pandas as pd

from sdmxabs.download_cache import GetFileKwargs
from sdmxabs.xml_base import NAME_SPACES, URL_STEM, acquire_xml

# --- constants
FlowMetaDict = dict[str, dict[str, str]]  # useful type alias


FLOW_NAME = "flow_name"
DATA_STRUCT_ID = "data_structure_id"
POSITION = "position"
CODE_LIST_ID = "codelist_id"


# --- public functions
@cache
def data_flows(flow_id: str = "all", **kwargs: Unpack[GetFileKwargs]) -> FlowMetaDict:
    """Get the toplevel metadata from the ABS SDMX API.

    Args:
        flow_id (str): The ID of the dataflow to retrieve. Defaults to "all".
        **kwargs: Additional keyword arguments passed to acquire_url().

    Returns:
        dict[str, dict[str, str]]: A dictionary containing the dataflow IDs
            and their metadata in key=value pairs. Importantly, it includes
            the DATASTRUCTURE identifier, which is used to retrieve
            the dimensions and attributes metadata.

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML root is found in the response.

    Guarantees:
        - data_flows(): the returned inner dictionary from data_flows() will always contain the keys
          "flow_name" and "data_structure_id" for each dataflow. Any XML without these
          keys from the ABS is ignored.

    """
    tree = acquire_xml(f"{URL_STEM}/dataflow/ABS/{flow_id}", **kwargs)

    data_flows_dict: FlowMetaDict = {}
    for dataflow in tree.findall(".//str:Dataflow", NAME_SPACES):
        attributes: dict[str, str] = dataflow.attrib.copy()
        if "id" not in attributes:
            continue
        dataflow_id = attributes.pop("id")
        name_elem = dataflow.find("com:Name", NAME_SPACES)
        dataflow_name = name_elem.text if name_elem is not None else "(missing name)"
        attributes[FLOW_NAME] = str(dataflow_name)
        ds_elem = dataflow.find("str:Structure/Ref", NAME_SPACES)
        if ds_elem is None:
            continue  # skip if no data structure reference
        ds_id = ds_elem.get("id", "")
        if not ds_id:
            continue
        attributes[DATA_STRUCT_ID] = ds_id
        data_flows_dict[dataflow_id] = attributes
    return data_flows_dict


@cache
def structure_ident(flow_id: str, **kwargs: Unpack[GetFileKwargs]) -> str:
    """Get the data structure ID for a specific dataflow.

    Args:
        flow_id (str): The ID of the dataflow to retrieve the structure ID for.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Returns:
        str: The data structure ID for the specified dataflow.

    Raises:
        ValueError: If the flow_id is not found or has no associated structure ID.

    """
    flow = data_flows(flow_id, **kwargs)
    if flow_id not in flow or DATA_STRUCT_ID not in flow[flow_id]:
        raise ValueError(f"No data structure found for flow '{flow_id}'")
    return flow[flow_id][DATA_STRUCT_ID]


@cache
def data_structures(struct_id: str, **kwargs: Unpack[GetFileKwargs]) -> FlowMetaDict:
    """Get the data structure for a specific structure ID from the ABS SDMX API.

    Args:
        struct_id (str): The ID of the data structure to retrieve.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Returns:
        dict[str, dict[str, str]]: A dictionary containing the dimensions and
            their metadata in key=value pairs.

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML root is found in the response.

    Note:
        The dimensions metadata includes a "position" for each dimmension.
        The attributes metadata does not have "position" information.

    """
    tree = acquire_xml(f"{URL_STEM}/datastructure/ABS/{struct_id}", **kwargs)

    elements = {}
    for ident in ("Dimension", "Attribute"):
        for elem in tree.findall(f".//str:{ident}", NAME_SPACES):
            element_id = elem.get("id")
            if element_id is None:
                continue
            contents = {}
            if ident == "Dimension":
                contents[POSITION] = elem.get(POSITION, "")
            if (lr := elem.find("str:LocalRepresentation", NAME_SPACES)) is not None and (
                enumer := lr.find("str:Enumeration/Ref", NAME_SPACES)
            ) is not None:
                contents = contents | enumer.attrib
            # --- check we have a code list, and give it a better name
            code_list_id = contents.pop("id", "")
            if not code_list_id or contents.get("package") != "codelist":
                continue
            contents[CODE_LIST_ID] = code_list_id
            elements[element_id] = contents
    return elements


@cache
def code_lists(cl_id: str, **kwargs: Unpack[GetFileKwargs]) -> FlowMetaDict:
    """Get the code list metadata from the ABS SDMX API.

    Args:
        cl_id (str): The ID of the code list to retrieve.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Returns:
        FlowMetaDict: A dictionary containing the codes and
            their associated key=value pairs. A "name" key should always
            be present. A "parent" key may also be present.

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML root is found in the response.

    Note:
        You will get a CacheError if the codelist is not found on the ABS SDMX API.
        (This package tries the website first, then the cache.)

    Guarantees for the inner dictionary:
        - The inner dictionary will always have a "name" key.
        - The inner dictionary may have a "parent" key if the code has a parent.

    """
    tree = acquire_xml(f"{URL_STEM}/codelist/ABS/{cl_id}", **kwargs)

    codes: FlowMetaDict = {}
    for code in tree.findall(".//str:Code", NAME_SPACES):
        code_id = code.get("id", None)
        if code_id is None:
            continue
        elements: dict[str, str] = {}

        # - get the name
        name = code.find("com:Name", NAME_SPACES)
        if name is None or not name.text:
            # guarantee that we name key and value pair
            print(f"Warning: Code {code_id} in {cl_id}has no name, skipping.")
            continue  # skip if no name
        elements["name"] = name.text

        # - get the parent
        parent = code.find("str:Parent", NAME_SPACES)
        parent_id = ""
        if parent is not None:
            ref = parent.find("Ref", NAME_SPACES)
            if ref is not None:
                parent_id = str(ref.get("id", ""))
        if parent_id:  # Only add if not empty
            elements["parent"] = parent_id

        codes[code_id] = elements

    return codes


@cache
def code_list_for(struct_id: str, dim_name: str, **kwargs: Unpack[GetFileKwargs]) -> FlowMetaDict:
    """Get the code list for a specific dimension or attribute in a data structure.

    Args:
        struct_id (str): The data structure ID.
        dim_name (str): The dimension or attribute ID to retrieve the code list for.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Returns:
        FlowMetaDict: A dictionary containing the codes and their metadata.

    Raises:
        ValueError: If the dimension/attribute is not found in the structure.

    """
    structure = data_structures(struct_id, **kwargs)
    if not structure:
        raise ValueError(f"No structure found for structure ID '{struct_id}'")
    if dim_name not in structure:
        raise ValueError(f"Dimension/Attribute '{dim_name}' not found in structure: '{struct_id}'")

    codelist_id = structure[dim_name].get(CODE_LIST_ID, "")
    if not codelist_id:
        raise ValueError(
            f"No codelist found for dimension/attribute '{dim_name}' in structure ID '{struct_id}'"
        )

    return code_lists(codelist_id, **kwargs)


@cache
def structure_from_flow_id(flow_id: str, **kwargs: Unpack[GetFileKwargs]) -> FlowMetaDict:
    """Get the data structure directly from the flow identifier.

    Args:
        flow_id (str): The ID of the data flow to validate.
        **kwargs: Additional keyword arguments, ultimately passed to acquire_url().

    Returns:
        FlowMetaDict: Dictionary containing the flow's structure.

    Raises:
        ValueError: If the flow_id is not valid.
        ValueError: If the structure_id or structure cannot be found.

    """
    if flow_id not in data_flows(**kwargs):
        raise ValueError(f"Invalid flow_id: {flow_id}.")
    structure_id = structure_ident(flow_id, **kwargs)
    structure = data_structures(structure_id, **kwargs)
    if not structure:
        raise ValueError(f"No structure found for structure ID: {structure_id}.")
    return structure


def frame(f: FlowMetaDict) -> pd.DataFrame:
    """Convert a FlowMetaDict to a pandas DataFrame.

    Args:
        f (FlowMetaDict): The flow metadata dictionary to convert.

    Returns:
        pd.DataFrame: A DataFrame representation of the flow metadata.

    Note: This is a utility function to help visualize the flow metadata.

    """
    return pd.DataFrame(f).T


# --- private functions
def validate_code_value(dim_name: str, value: str, required: pd.DataFrame) -> str:
    """Check if a value for a dimension is in the codelist for the dimension.

    Args:
        dim_name (str): The dimension (name) to check.
        value (str): The value to check.
        required (pd.DataFrame): The required dimensions for the dataflow.

    Returns:
        str: The name of the codelist if the value is not found, otherwise an empty string.

    """
    if "package" not in required.columns or dim_name not in required.index:
        return ""
    package = required.loc[dim_name, "package"]
    if pd.isna(package):
        return ""
    if package == "codelist" and CODE_LIST_ID in required.columns:
        codelist_name = str(required.loc[dim_name, CODE_LIST_ID])
        if codelist_name and value not in code_lists(codelist_name):
            return f"Code '{value}' for dimension '{dim_name}' is not found in codelist '{codelist_name}'"
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


def build_key(flow_id: str, selection: dict[str, str] | None, *, validate: bool = False) -> str:
    """Build a key for a dataflow based on its data structure.

    Args:
        flow_id (str): The identifier for the dataflow.
        selection (dict[str, str] | None): A dictionary of dimension=value pairs
            to select the data items. If None, the returned key will be "all".
        validate (bool): If True, validate the dimensions against the required
            dimensions for the flow_id.

    Returns:
        str: A string representing the key for the requested data.

    """
    # --- check validity of inputs
    structure = structure_from_flow_id(flow_id)
    if not structure or selection is None:
        return "all"

    # --- convert our data structure MetaFlowDict to a DataFrame so we can sort by position
    required_df = frame(structure)
    if POSITION not in required_df.columns:
        return "all"  # no position means no sorting, so return "all"
    required_df = required_df[required_df[POSITION].notna()]  # ignore attributes without position
    if required_df.empty:
        return "all"
    required_df[POSITION] = required_df[POSITION].astype(int)  # for integer sorting below

    # --- build the sdmx key using the required dimensions in the data structure
    sdmx_keys = []
    wrong = []
    for dim_name in required_df.sort_values(by=POSITION).index:
        if dim_name in selection:
            value = selection[dim_name]
            issues = [
                issue for v in value.split("+") if (issue := validate_code_value(dim_name, v, required_df))
            ]
            if not issues:
                sdmx_keys.append(value)
                continue
            wrong += issues
        sdmx_keys.append("")  # empty-string means global match for this dimension

    # --- alert to any data structure coding issues
    if validate:
        missing = [k for k in required_df.index if k not in selection]
        extra = [k for k in selection if k not in required_df.index]
        publish_alerts(flow_id, missing, extra, wrong)

    # --- if there are no keys, return "all"
    if sdmx_keys and any(sdmx_keys):
        return ".".join(sdmx_keys)
    return "all"


if __name__ == "__main__":

    def heading(text: str) -> None:
        """Print a heading with a separator."""
        n = 20
        print("-" * n, text, "-" * n)

    def metadata_test() -> None:
        """Test the metadata functions."""
        # --- data_flows -- all dataflows
        heading("data_flows() - all dataflows")
        flows = data_flows(modality="prefer-cache")
        flows_df = frame(flows)
        mismatch = flows_df.index != flows_df[DATA_STRUCT_ID]
        print(
            f"Of the {len(flows)} flows, there are {mismatch.sum()}"
            " where the dataflow ID differs from the data structure ID"
        )
        print(flows_df[mismatch])

        # --- data_flows -- specific dataflow
        heading("data_flows() - WPI dataflow only")
        flows = data_flows(flow_id="WPI", modality="prefer-cache")
        print(len(flows))
        print(frame(flows))

        # --- structure_ident
        heading("structure_ident() - LF_UNDER dataflow")
        structure_id = structure_ident("LF_UNDER", modality="prefer-cache")
        print("Structure ID:", structure_id)

        # --- data_structures
        print("-" * 20, "data_structures() - WPI data structure", "-" * 20)
        wpi_struct_id = structure_ident("WPI", modality="prefer-cache")
        structure = data_structures(wpi_struct_id, modality="prefer-cache")
        print("Length =", len(structure))
        print(frame(structure))

        print("-" * 20, "structure_from_flow_id() - LF_UNDER", "-" * 20)
        structure = structure_from_flow_id("LF_UNDER", modality="prefer-cache")
        print("Length =", len(structure))
        print(frame(structure))

        print("-" * 20, "data_structures() - ANA_AGG data structure", "-" * 20)
        structure = data_structures("ANA_AGG", modality="prefer-cache")
        print("Length =", len(structure))
        print(frame(structure))

        # --- code list for a particular dimension in the data structure
        heading("code_list_for() - ANA_AGG data structure, code list for REGION")
        code_list_ = code_list_for("ANA_AGG", "REGION", modality="prefer-cache")
        print("Length =", len(code_list_))
        print(frame(code_list_))

        # --- code_lists
        heading("code_lists() - CL_WPI_PCI")
        code_list_1 = frame(code_lists("CL_WPI_PCI", modality="prefer-cache"))
        print("Length =", len(code_list_1))
        print(code_list_1)

        heading("code_lists() - CL_SECTOR")
        code_list_2 = frame(code_lists("CL_SECTOR", modality="prefer-cache"))
        print("Length =", len(code_list_2))
        print(code_list_2)

        # --- build_key
        heading("build_key()")
        key = build_key("WPI", {"FREQ": "Q", "REGION": "3"}, validate=True)
        print("Key:", key)

        heading("build_key()")
        key = build_key("WPI", {"FREQ": "T", "REGION": "1+2", "MEASURES": "CPI"}, validate=True)
        print("Key:", key)

    # --- run the quick test
    metadata_test()
