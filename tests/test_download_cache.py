"""Tests for download_cache module."""

from unittest.mock import Mock, patch

import pytest
import requests

from sdmxabs.download_cache import (
    DOWNLOAD_TIMEOUT,
    CacheError,
    HttpError,
    _check_for_bad_response,
    _get_data,
    _request_get,
    _retrieve_from_cache,
    _save_to_cache,
    acquire_url,
)


class TestHttpError:
    """Test HttpError exception."""

    def test_http_error_creation(self):
        error = HttpError("Test error")
        assert str(error) == "Test error"


class TestCacheError:
    """Test CacheError exception."""

    def test_cache_error_creation(self):
        error = CacheError("Cache test error")
        assert str(error) == "Cache test error"


class TestCheckForBadResponse:
    """Test _check_for_bad_response function."""

    def test_successful_response(self):
        response = Mock()
        response.status_code = 200
        response.headers = {"Content-Type": "application/xml"}

        # Should not raise an exception
        _check_for_bad_response("http://test.com", response)

    def test_bad_status_code(self):
        response = Mock()
        response.status_code = 404
        response.headers = {"Content-Type": "application/xml"}

        with pytest.raises(HttpError) as exc_info:
            _check_for_bad_response("http://test.com", response)

        assert "Problem 404 accessing: http://test.com" in str(exc_info.value)

    def test_no_headers(self):
        response = Mock()
        response.status_code = 200
        response.headers = None

        with pytest.raises(HttpError):
            _check_for_bad_response("http://test.com", response)


class TestSaveToCache:
    """Test _save_to_cache function."""

    def test_save_empty_content(self, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"
        _save_to_cache(file_path, b"", verbose=False)

        # Empty content should not create a file
        assert not file_path.exists()

    def test_save_content(self, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"
        content = b"test content"

        _save_to_cache(file_path, content, verbose=False)

        assert file_path.exists()
        assert file_path.read_bytes() == content

    def test_save_overwrites_existing(self, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"

        # Create initial file
        file_path.write_bytes(b"initial content")

        # Save new content
        new_content = b"new content"
        _save_to_cache(file_path, new_content, verbose=False)

        assert file_path.read_bytes() == new_content

    def test_save_creates_parent_dirs(self, temp_cache_dir):
        file_path = temp_cache_dir / "subdir" / "test_file"
        content = b"test content"

        _save_to_cache(file_path, content, verbose=False)

        assert file_path.exists()
        assert file_path.read_bytes() == content


class TestRequestGet:
    """Test _request_get function."""

    @patch("sdmxabs.download_cache.requests.get")
    def test_successful_request(self, mock_get, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"

        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/xml"}
        mock_response.content = b"test content"
        mock_get.return_value = mock_response

        result = _request_get("http://test.com", file_path, verbose=False)

        assert result == b"test content"
        mock_get.assert_called_once_with("http://test.com", allow_redirects=True, timeout=DOWNLOAD_TIMEOUT)

    @patch("sdmxabs.download_cache.requests.get")
    def test_request_exception(self, mock_get, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"
        mock_get.side_effect = requests.exceptions.RequestException("Network error")

        with pytest.raises(HttpError) as exc_info:
            _request_get("http://test.com", file_path, verbose=False)

        assert "there was a problem downloading http://test.com" in str(exc_info.value)

    @patch("sdmxabs.download_cache.requests.get")
    def test_empty_response(self, mock_get, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/xml"}
        mock_response.content = b""
        mock_get.return_value = mock_response

        result = _request_get("http://test.com", file_path, verbose=False)

        assert result == b""
        # File should not be created for empty content
        assert not file_path.exists()


class TestRetrieveFromCache:
    """Test _retrieve_from_cache function."""

    def test_retrieve_existing_file(self, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"
        content = b"cached content"
        file_path.write_bytes(content)

        result = _retrieve_from_cache(file_path, verbose=False)

        assert result == content

    def test_retrieve_nonexistent_file(self, temp_cache_dir):
        file_path = temp_cache_dir / "nonexistent_file"

        with pytest.raises(CacheError) as exc_info:
            _retrieve_from_cache(file_path, verbose=False)

        assert "Cached file not available" in str(exc_info.value)

    def test_retrieve_directory_instead_of_file(self, temp_cache_dir):
        # Create a directory with the expected file name
        dir_path = temp_cache_dir / "test_file"
        dir_path.mkdir()

        with pytest.raises(CacheError):
            _retrieve_from_cache(dir_path, verbose=False)


class TestGetData:
    """Test _get_data function."""

    def test_prefer_cache_with_existing_file(self, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"
        cached_content = b"cached content"
        file_path.write_bytes(cached_content)

        with patch("sdmxabs.download_cache._request_get") as mock_request:
            result = _get_data("http://test.com", file_path, modality="prefer-cache")

        assert result == cached_content
        mock_request.assert_not_called()

    def test_prefer_url_mode(self, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"

        with patch("sdmxabs.download_cache._request_get") as mock_request:
            mock_request.return_value = b"url content"
            result = _get_data("http://test.com", file_path, modality="prefer-url")

        assert result == b"url content"
        mock_request.assert_called_once()

    def test_fallback_to_cache_on_http_error(self, temp_cache_dir):
        file_path = temp_cache_dir / "test_file"
        cached_content = b"cached content"
        file_path.write_bytes(cached_content)

        with patch("sdmxabs.download_cache._request_get") as mock_request:
            mock_request.side_effect = HttpError("Network error")
            result = _get_data("http://test.com", file_path, modality="prefer-url")

        assert result == cached_content


class TestAcquireUrl:
    """Test acquire_url function."""

    @patch("sdmxabs.download_cache._get_data")
    def test_acquire_url_success(self, mock_get_data, temp_cache_dir):
        mock_get_data.return_value = b"test content"

        result = acquire_url("http://test.com", cache_dir=temp_cache_dir, verbose=False)

        assert result == b"test content"
        mock_get_data.assert_called_once()

    def test_acquire_url_creates_cache_dir(self, temp_cache_dir):
        # Remove the temp dir to test creation
        cache_dir = temp_cache_dir / "new_cache"

        with patch("sdmxabs.download_cache._get_data") as mock_get_data:
            mock_get_data.return_value = b"test content"
            acquire_url("http://test.com", cache_dir=cache_dir)

        assert cache_dir.exists()
        assert cache_dir.is_dir()

    def test_acquire_url_cache_dir_not_directory(self, temp_cache_dir):
        # Create a file where we expect a directory
        cache_file = temp_cache_dir / "cache_file"
        cache_file.write_text("not a directory")

        with pytest.raises(CacheError) as exc_info:
            acquire_url("http://test.com", cache_dir=cache_file)

        assert "Cache path is not a directory" in str(exc_info.value)

    def test_url_to_filename_conversion(self, temp_cache_dir):
        """Test that URLs are properly converted to cache filenames."""
        def mock_request_get_side_effect(url, file_path, **kwargs):
            # Simulate the real _request_get behavior - save to cache and return content
            from sdmxabs.download_cache import _save_to_cache
            content = b"test content"
            _save_to_cache(file_path, content, **kwargs)
            return content

        with patch("sdmxabs.download_cache._request_get", side_effect=mock_request_get_side_effect):
            url = "https://example.com/data/test?param=value"
            acquire_url(url, cache_dir=temp_cache_dir)

            # Check that the cache file was created with proper name conversion
            cache_files = list(temp_cache_dir.glob("cache--*"))
            assert len(cache_files) == 1

            # Filename should not contain problematic characters
            filename = cache_files[0].name
            assert not any(char in filename for char in '~"#%&*:<>?\\{|}')
