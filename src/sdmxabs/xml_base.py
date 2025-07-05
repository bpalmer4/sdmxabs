"""XML code for the ABS SDMX API."""

from typing import Unpack
import xml.etree.ElementTree as ET
from sdmxabs.download_cache import acquire_url, GetFileKwargs


# --- constants

URL_STEM = "https://data.api.abs.gov.au/rest"
# /{structureType}/{agencyId}/{structureId}/{structureVersion}
# ? references={reference value}& detail={level of detail}

NAME_SPACES = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    "gen": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
}


# === functions


def acquire_xml(url, **kwargs: Unpack[GetFileKwargs]) -> ET.ElementTree:
    """Acquire xml data from the ABS SDMX API.

    Args:
        url (str): The URL to retrieve the XML data from.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Returns:
        ET.ElementTree: An ElementTree object containing the XML data.

    Raises:
        ValueError: If no XML tree is found in the response.

    """
    kwargs["modality"] = kwargs.get("modality", "prefer_cache")
    xml = acquire_url(url, **kwargs)
    tree = ET.ElementTree(ET.fromstring(xml))
    if tree is None:
        raise ValueError("No XML tree found in the response.")
    return tree
