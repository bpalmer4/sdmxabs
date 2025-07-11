"""Basic XML code for the ABS SDMX API."""

from typing import Unpack
from xml.etree.ElementTree import Element

from defusedxml import ElementTree

from sdmxabs.download_cache import CacheError, GetFileKwargs, HttpError, acquire_url

# --- constants - used in multiple other modules when parsing XML

URL_STEM = "https://data.api.abs.gov.au/rest"

NAME_SPACES = {
    "mes": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message",
    "str": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure",
    "com": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common",
    "gen": "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
}


# --- functions
def acquire_xml(url: str, **kwargs: Unpack[GetFileKwargs]) -> Element:
    """Acquire xml data from the ABS SDMX API.

    Args:
        url (str): The URL to retrieve the XML data from.
        **kwargs: Additional keyword arguments passed to acquire_url().

    Returns:
        An Element object containing the XML data.

    Raises:
        ValueError: If the response contains invalid XML.

    """
    kwargs["modality"] = kwargs.get("modality", "prefer-cache")
    xml = acquire_url(url, **kwargs)

    try:
        root = ElementTree.fromstring(xml)
    except ElementTree.ParseError as e:
        raise ValueError(f"Invalid XML received from {url}: {e}") from e

    return root


if __name__ == "__main__":

    def xml_test() -> None:
        """Test the acquire_xml function."""
        # Example URL for testing
        test_url = "https://data.api.abs.gov.au/rest/data/WPI?startPeriod=2024-Q1"
        try:
            _root_element = acquire_xml(test_url, verbose=False, modality="prefer-url")
            print(f"Test passed: XML acquired successfully from {test_url}.")
        except (ValueError, CacheError, HttpError) as e:
            print(f"Test FAILED: Error acquiring XML: {e}")

    xml_test()
