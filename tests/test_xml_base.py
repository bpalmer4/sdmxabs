"""Tests for xml_base module."""

from unittest.mock import patch
from xml.etree.ElementTree import Element

import pytest

from sdmxabs.download_cache import CacheError, HttpError
from sdmxabs.xml_base import NAME_SPACES, URL_STEM, acquire_xml


class TestNamespaces:
    """Test namespace constants."""

    def test_namespaces_defined(self):
        """Test that all required namespaces are defined."""
        required_namespaces = ["mes", "str", "com", "gen"]

        for ns in required_namespaces:
            assert ns in NAME_SPACES
            assert isinstance(NAME_SPACES[ns], str)
            assert NAME_SPACES[ns].startswith("http://")


class TestUrlStem:
    """Test URL_STEM constant."""

    def test_url_stem_format(self):
        """Test that URL_STEM is properly formatted."""
        assert URL_STEM == "https://data.api.abs.gov.au/rest"
        assert URL_STEM.startswith("https://")
        assert URL_STEM.endswith("/rest")


class TestAcquireXml:
    """Test acquire_xml function."""

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_success(self, mock_acquire_url):
        """Test successful XML acquisition and parsing."""
        xml_content = b'<?xml version="1.0"?><root><child>test</child></root>'
        mock_acquire_url.return_value = xml_content

        result = acquire_xml("http://test.com", verbose=False)

        assert isinstance(result, Element)
        assert result.tag == "root"
        assert result.find("child").text == "test"
        mock_acquire_url.assert_called_once_with("http://test.com", modality="prefer-cache", verbose=False)

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_with_namespace(self, mock_acquire_url):
        """Test XML acquisition with namespaced elements."""
        xml_content = b"""<?xml version="1.0"?>
        <mes:StructureSpecificData xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message">
            <mes:DataSet>
                <mes:Series>Test</mes:Series>
            </mes:DataSet>
        </mes:StructureSpecificData>"""
        mock_acquire_url.return_value = xml_content

        result = acquire_xml("http://test.com")

        assert isinstance(result, Element)
        # Check namespace handling
        assert result.tag.endswith("StructureSpecificData")

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_invalid_xml(self, mock_acquire_url):
        """Test handling of invalid XML content."""
        invalid_xml = b"<root><unclosed_tag></root>"
        mock_acquire_url.return_value = invalid_xml

        with pytest.raises(ValueError) as exc_info:
            acquire_xml("http://test.com")

        assert "Invalid XML received from http://test.com" in str(exc_info.value)

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_empty_content(self, mock_acquire_url):
        """Test handling of empty content."""
        mock_acquire_url.return_value = b""

        with pytest.raises(ValueError):
            acquire_xml("http://test.com")

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_non_xml_content(self, mock_acquire_url):
        """Test handling of non-XML content."""
        mock_acquire_url.return_value = b"This is not XML content"

        with pytest.raises(ValueError):
            acquire_xml("http://test.com")

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_modality_override(self, mock_acquire_url):
        """Test that modality can be overridden."""
        xml_content = b'<?xml version="1.0"?><root></root>'
        mock_acquire_url.return_value = xml_content

        acquire_xml("http://test.com", modality="prefer-url", verbose=True)

        mock_acquire_url.assert_called_once_with("http://test.com", modality="prefer-url", verbose=True)

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_propagates_cache_error(self, mock_acquire_url):
        """Test that CacheError is propagated."""
        mock_acquire_url.side_effect = CacheError("Cache error")

        with pytest.raises(CacheError):
            acquire_xml("http://test.com")

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_propagates_http_error(self, mock_acquire_url):
        """Test that HttpError is propagated."""
        mock_acquire_url.side_effect = HttpError("HTTP error")

        with pytest.raises(HttpError):
            acquire_xml("http://test.com")

    def test_acquire_xml_with_malformed_xml(self):
        """Test handling of malformed XML that could be a security risk."""
        # This test ensures defusedxml is working properly
        malicious_xml = b"""<?xml version="1.0"?>
        <!DOCTYPE root [
        <!ENTITY xxe SYSTEM "file:///etc/passwd">
        ]>
        <root>&xxe;</root>"""

        with patch("sdmxabs.xml_base.acquire_url") as mock_acquire_url:
            mock_acquire_url.return_value = malicious_xml

            # defusedxml should handle this safely
            with pytest.raises(ValueError):
                acquire_xml("http://test.com")

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_large_response(self, mock_acquire_url):
        """Test handling of large XML responses."""
        # Create a large but valid XML document
        large_xml = b'<?xml version="1.0"?><root>'
        for i in range(1000):
            large_xml += f'<item id="{i}">data_{i}</item>'.encode()
        large_xml += b"</root>"

        mock_acquire_url.return_value = large_xml

        result = acquire_xml("http://test.com")

        assert isinstance(result, Element)
        assert result.tag == "root"
        assert len(result) == 1000  # Should have 1000 child elements

    @patch("sdmxabs.xml_base.acquire_url")
    def test_acquire_xml_unicode_content(self, mock_acquire_url):
        """Test handling of Unicode content in XML."""
        unicode_xml = '<?xml version="1.0" encoding="UTF-8"?><root><text>测试数据</text></root>'.encode()
        mock_acquire_url.return_value = unicode_xml

        result = acquire_xml("http://test.com")

        assert isinstance(result, Element)
        assert result.find("text").text == "测试数据"


class TestXmlIntegration:
    """Integration tests for XML handling."""

    def test_xml_with_sdmx_namespaces(self):
        """Test XML parsing with actual SDMX namespaces."""
        sdmx_xml = f"""<?xml version="1.0"?>
        <mes:StructureSpecificData 
            xmlns:mes="{NAME_SPACES["mes"]}"
            xmlns:str="{NAME_SPACES["str"]}"
            xmlns:com="{NAME_SPACES["com"]}"
            xmlns:gen="{NAME_SPACES["gen"]}">
            <mes:DataSet>
                <gen:Series>
                    <gen:SeriesKey>
                        <gen:Value id="FREQ" value="Q"/>
                    </gen:SeriesKey>
                </gen:Series>
            </mes:DataSet>
        </mes:StructureSpecificData>""".encode()

        with patch("sdmxabs.xml_base.acquire_url") as mock_acquire_url:
            mock_acquire_url.return_value = sdmx_xml

            result = acquire_xml("http://test.com")

            # Test namespace-aware querying
            series = result.find(".//gen:Series", NAME_SPACES)
            assert series is not None

            value = series.find(".//gen:Value", NAME_SPACES)
            assert value is not None
            assert value.get("id") == "FREQ"
            assert value.get("value") == "Q"
