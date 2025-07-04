"""Capture data from the Australian Bureau of Statistics (ABS) using the SDMX API."""

from importlib.metadata import PackageNotFoundError, version

from sdmxabs.download_cache import (
    CacheError,
    HttpError,
    acquire_url,
    GetFileKwargs,
    ModalityType,
)
from .metadata import data_flows, data_dimensions, code_lists

# --- version and author
try:
    __version__ = version(__name__)
except PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development mode
__author__ = "Bryan Palmer"

# --- establish the package contents
__all__ = [
    "CacheError",
    "GetFileKwargs",
    "HttpError",
    "ModalityType",
    "__author__",
    "__version__",
    "acquire_url",
    "data_flows",
    "data_dimensions",
    "code_lists",
]
