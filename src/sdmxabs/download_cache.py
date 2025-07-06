"""Download and cache material from the Internat.

The default cache directory can be specified by setting the environment
variable SDMXABS_CACHE_DIR.
"""

import re
from hashlib import sha256
from os import getenv
from pathlib import Path
from typing import Literal, NotRequired, TypedDict, Unpack

import requests

# --- constants
# define the default cache directory
SDMXABS_CACHE_DIR = "./.sdmxabs_cache"
SDMXABS_CACHE_DIR = getenv("READABS_CACHE_DIR", SDMXABS_CACHE_DIR)
SDMXABS_CACHE_PATH = Path(SDMXABS_CACHE_DIR)

DOWNLOAD_TIMEOUT = 120  # seconds


# --- Classes
class HttpError(Exception):
    """A problem retrieving data from HTTP."""


class CacheError(Exception):
    """A problem retrieving data from the cache."""


ModalityType = Literal["prefer-cache", "prefer-url"]


class GetFileKwargs(TypedDict):
    """TypedDict for acqure_url function arguments."""

    verbose: NotRequired[bool]
    """If True, print information about the data retrieval process."""
    modality: NotRequired[ModalityType]
    """Kind of retrieval: "prefer_cache", "prefer_url"."""


# --- functions
def check_for_bad_response(
    url: str,
    response: requests.Response,
) -> None:
    """Raise an Exception if we could not retrieve the URL.

    Args:
        url (str): The URL we tried to access.
        response (requests.Response): The response object from the request.

    Raises:
        HttpError: If the response status code is not 200 or if the headers are None

    """
    code = response.status_code
    success = 200
    if code != success or response.headers is None:
        problem = f"Problem {code} accessing: {url}."
        raise HttpError(problem)


def save_to_cache(
    file_path: Path,
    contents: bytes,
    **kwargs: Unpack[GetFileKwargs],
) -> None:
    """Save bytes to the file-system."""
    verbose = kwargs.get("verbose", False)
    if len(contents) == 0:
        # dont save empty files (possibly caused by ignoring errors)
        return
    if file_path.exists():
        if verbose:
            print("Removing old cache file.")
        file_path.unlink()
    if verbose:
        print(f"About to save to cache: {file_path}")
    file_path.open(mode="w", buffering=-1, encoding=None, errors=None, newline=None)
    file_path.write_bytes(contents)


def request_get(
    url: str,
    file_path: Path,
    **kwargs: Unpack[GetFileKwargs],
) -> bytes:
    """Get the contents of the specified URL."""
    # Initialise variables

    verbose = kwargs.get("verbose", False)
    if verbose:
        print(f"About to request/download: {url}")

    try:
        gotten = requests.get(url, allow_redirects=True, timeout=DOWNLOAD_TIMEOUT)
    except requests.exceptions.RequestException as e:
        error = f"request_get(): there was a problem downloading {url} --> ({e})."
        raise HttpError(error) from e

    check_for_bad_response(url, gotten)  # exception on error

    return_bytes = gotten.content
    if len(gotten.content) > 0:
        save_to_cache(file_path, return_bytes, **kwargs)

    return return_bytes


def retrieve_from_cache(file: Path, **kwargs: Unpack[GetFileKwargs]) -> bytes:
    """Retrieve bytes from file-system."""
    verbose = kwargs.get("verbose", False)

    if not file.exists() or not file.is_file():
        message = f"Cached file not available: {file.name}"
        raise CacheError(message)
    if verbose:
        print(f"Retrieving from cache: {file}")

    return file.read_bytes()


def get_data(url: str, file_path: Path, **kwargs: Unpack[GetFileKwargs]) -> bytes:
    """Select the source of the file based on the modality."""
    # --- set arguments
    verbose: bool = kwargs.get("verbose", False)
    if verbose:
        print(f"get_data called with: {kwargs}")
    modality: ModalityType = kwargs.get("modality", "prefer-cache")

    # --- prefer-cache
    tried_cache = False
    if file_path.exists() and file_path.is_file() and modality != "prefer-url":
        try:
            text = retrieve_from_cache(file_path, **kwargs)
            if len(text) > 0:
                return text
        except CacheError:
            pass
        tried_cache = True

    # --- prefer_url
    try:
        return request_get(url, file_path, **kwargs)
    except HttpError:
        if tried_cache:
            # if we tried the cache, then we have no choice but to raise the error
            raise

    # if we did not try the cache, then we can return the cached file
    return retrieve_from_cache(file_path, **kwargs)


def acquire_url(
    url: str,
    cache_dir: Path = SDMXABS_CACHE_PATH,
    cache_prefix: str = "cache",
    **kwargs: Unpack[GetFileKwargs],
) -> bytes:
    """Acquire the data at an URL or using the local file-system cache, depending on freshness.

    Args:
        url (str): The URL to retrieve the file from.
        cache_dir (Path): The directory where cached files are stored.
        cache_prefix (str): A prefix for the cached file names.
        kwargs (GetFileKwargs): Additional keyword arguments for the function.

    Returns:
        bytes: The content of the file retrieved from the URL or local cache.

    Raises:
        CacheError: If the cache directory is not a valid directory.
        HttpError: If there is a problem retrieving the URL.

    """

    def get_fpath() -> Path:
        """Convert URL string into a cache file name and return as a Path object."""
        bad_cache_pattern = r'[~"#%&*:<>?\\{|}]+'  # chars to remove from name
        hash_name = sha256(url.encode("utf-8")).hexdigest()
        tail_name = url.split("/")[-1].split("?")[0]
        file_name = re.sub(bad_cache_pattern, "", f"{cache_prefix}--{hash_name}--{tail_name}")
        return Path(cache_dir / file_name)

    # create and check cache_dir is a directory
    cache_dir.mkdir(parents=True, exist_ok=True)
    if not cache_dir.is_dir():
        msg = f"Cache path is not a directory: {cache_dir.name}"
        raise CacheError(msg)

    return get_data(url, get_fpath(), **kwargs)


# --- preliminary testing:
if __name__ == "__main__":

    def cache_test() -> None:
        """Test the retrieval and caching system.

        You may first want to clear the cache directory.

        """
        # prepare the test case
        url1 = (
            "https://www.abs.gov.au/statistics/labour/employment-and-"
            "unemployment/labour-force-australia/nov-2023/6202001.xlsx"
        )

        # implement - first retrieval is from the web, second from the cache
        width = 20
        print("Test commencing.")
        for u in (url1, url1):
            print("=" * width)
            content = acquire_url(u, verbose=True)
            print("-" * width)
            print(f"{len(content)} bytes retrieved from {u}.")
        print("=" * width)
        content = acquire_url(url1, verbose=True, modality="prefer-url")
        print(f"Byte count: {len(content)}")
        print("Test completed.")

    cache_test()
