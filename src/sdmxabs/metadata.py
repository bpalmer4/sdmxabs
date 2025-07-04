"""Extract metadata from the ABS SDMX API."""

import xml.etree.ElementTree as ET
from typing import Unpack
import pandas as pd
from sdmxabs.download_cache import acquire_url, GetFileKwargs


URL_STEM = "https://data.api.abs.gov.au/rest"
# /{structureType}/{agencyId}/{structureId}/{structureVersion}
# ? references={reference value}& detail={level of detail}

agency_ids = ("ABS",)
STRUCTUERS = (
    "datastructure"
    "dataflow"
    "codelist"
    "conceptscheme"
    "categoryscheme"
    "contentconstraint"
    "actualconstraint"
    "agencyscheme"
    "categorisation"
    "hierarchicalcodelist"
)
stucture_ids = ("all",)
details = (
    "allstubs",  # All artefacts will be returned as stubs.
    "referencestubs",  # The referenced artefacts will be returned as stubs.
    "referencepartial",  # The referenced item schemes should only include
    # items used by the artefact to be returned. For example,
    # a concept scheme would only contain the concepts used in
    # a DSD, and its isPartial flag would be set to true. As
    # another example, if a dataflow is constrained, then the
    # codelists returned should only contain the subset of codes
    # allowed by that constraint.
    "allcompletestubs",  # All artefacts should be returned as complete stubs,
    # containing identification information, the artefacts' name,
    # description, annotations and isFinal information.
    "referencecompletestubs",  # The referenced artefacts should be returned as complete
    # stubs, containing identification information, the artefacts'
    # name, description, annotations and isFinal information.
    "full",  # All available information for all artefacts will be returned. This is the default.)
)

NAME_SPACES = {
    'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
    'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
}

def acquire_xml(url, **kwargs: Unpack[GetFileKwargs]) -> ET.ElementTree:
    """Acquire data from the ABS SDMX API."""

    kwargs["modality"] = kwargs.get("modality", "prefer_cache")
    xml = acquire_url(url, **kwargs)
    tree = ET.ElementTree(ET.fromstring(xml))
    if tree is None:
        raise ValueError("No XML tree found in the response.")

    return tree


def data_flows(flow_id="all", **kwargs: Unpack[GetFileKwargs]) -> pd.Series:
    """Get the toplevel metadata from the ABS SDMX API.
    
    Args:
        flow_id (str): The ID of the dataflow to retrieve. Defaults to "all".
        **kwargs: Additional keyword arguments passed to acquire_url().
        
    Returns:
        pd.Series: A Series containing the dataflow IDs and names.

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.

    """
    tree = acquire_xml(f"{URL_STEM}/dataflow/ABS/{flow_id}", **kwargs)

    df = {}
    for dataflow in tree.findall('.//str:Dataflow', NAME_SPACES):
        df_id = dataflow.get('id')
        name_elem = dataflow.find('com:Name', NAME_SPACES)
        df_name = name_elem.text if name_elem is not None else "(no name)"
        df[df_id] = df_name
    return pd.Series(df)


def data_dimensions(flow_id, **kwargs: Unpack[GetFileKwargs]) -> pd.DataFrame:
    """Get the data dimensions metadata from the ABS SDMX API.

    Args:
        **kwargs: Additional keyword arguments passed to acquire_url().

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.
        ValueError: If no XML root is found in the response.

    """
    tree = acquire_xml(f"{URL_STEM}/datastructure/ABS/{flow_id}", **kwargs)

    dimensions = {}
    for dim in tree.findall('.//str:Dimension', NAME_SPACES):
        dim_id = dim.get('id')
        dim_pos = dim.get('position')
        if dim_id is None or dim_pos is None:
            continue
        contents = {'position': dim_pos}
        if (lr := dim.find('str:LocalRepresentation', NAME_SPACES)) is not None:
            if (enumer := lr.find('str:Enumeration/Ref', NAME_SPACES)) is not None:
                value = enumer.get('id')
                contents['codelist'] = value if value is not None else ''
        dimensions[dim_id] = contents
    return pd.DataFrame(dimensions).T


def code_lists(cl_id: str, **kwargs: Unpack[GetFileKwargs]) -> pd.DataFrame:
    """Get the code list metadata from the ABS SDMX API.

    Args:
        cl_id (str): The ID of the code list to retrieve.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Raises:
        HttpError: If there is an issue with the HTTP request.
        CacheError: If there is an issue with the cache.

    """
    tree = acquire_xml(f"{URL_STEM}/codelist/ABS/{cl_id}", **kwargs)

    codes = {}
    for code in tree.findall('.//str:Code', NAME_SPACES):
        code_id = code.get('id')
        if code_id is None:
            continue
        elements = {}
        name = code.find('com:Name', NAME_SPACES)
        elements['name'] = name.text if name is not None else None
        parent = code.find('str:Parent', NAME_SPACES)
        parent_id = None
        if parent is not None:
            ref = parent.find('Ref', NAME_SPACES)
            if ref is not None:
                parent_id = ref.get('id')
        elements['parent'] = parent_id
        codes[code_id] = elements

    return pd.DataFrame(codes).T.sort_index()


if __name__ == "__main__":
    # --- data_flows
    FLOWS = data_flows(modality="prefer_cache")
    print(len(FLOWS))
    FLOWS = data_flows(flow_id="WPI", modality="prefer_cache")
    print(len(FLOWS))
    print(FLOWS)

    # --- data_dimensions
    DIMENSIONS = data_dimensions("WPI", modality="prefer_cache")
    print(len(DIMENSIONS))
    print(DIMENSIONS)

    # --- code lists
    CODE_LISTS = code_lists("CL_WPI_PCI", modality="prefer_cache")
    print(len(CODE_LISTS))
    print(CODE_LISTS)
