"""XML code for the ABS SDMX API."""

from typing import Unpack
from xml.etree.ElementTree import Element

from defusedxml import ElementTree

from sdmxabs.download_cache import GetFileKwargs, acquire_url

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
def acquire_xml(url: str, **kwargs: Unpack[GetFileKwargs]) -> Element:
    """Acquire xml data from the ABS SDMX API.

    Args:
        url (str): The URL to retrieve the XML data from.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Returns:
        An Element object containing the XML data.

    Raises:
        ValueError: If no XML tree is found in the response.

    """
    # Note: will need to "prefer-url" for data requests.
    # But "prefer-cache" should be fine for metadata requests.
    # And for development, "prefer-cache" is also fine.
    kwargs["modality"] = kwargs.get("modality", "prefer-cache")
    xml = acquire_url(url, **kwargs)
    root = ElementTree.fromstring(xml)
    if root is None:
        raise ValueError("No XML root found in the response.")
    return root


if __name__ == "__main__":
    URL = "https://data.api.abs.gov.au/rest/data/WPI"
    FOUND = acquire_xml(URL, modality="prefer-cache")
