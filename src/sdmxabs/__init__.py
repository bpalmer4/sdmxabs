"""Capture data from the Australian Bureau of Statistics (ABS) using the SDMX API."""

from importlib.metadata import PackageNotFoundError, version

from .data import fetch
from .download_cache import (
    CacheError,
    GetFileKwargs,
    HttpError,
    ModalityType,
)
from .metadata import code_lists, data_dimensions, data_flows

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
    "code_lists",
    "data_dimensions",
    "data_flows",
    "fetch",
]
