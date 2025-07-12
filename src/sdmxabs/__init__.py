"""Capture data from the Australian Bureau of Statistics (ABS) using the SDMX API."""

from importlib.metadata import PackageNotFoundError, version

from .download_cache import (
    CacheError,
    GetFileKwargs,
    HttpError,
    ModalityType,
)
from .fetch import fetch
from .fetch_gdp import fetch_gdp
from .fetch_multi import fetch_multi
from .fetch_pop import fetch_pop
from .fetch_selection import MatchCriteria, MatchItem, MatchType, fetch_selection, make_wanted, match_item
from .flow_metadata import code_lists, data_dimensions, data_flows

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
    "MatchCriteria",
    "MatchItem",
    "MatchType",
    "ModalityType",
    "__author__",
    "__version__",
    "code_lists",
    "data_dimensions",
    "data_flows",
    "fetch",
    "fetch_gdp",
    "fetch_multi",
    "fetch_pop",
    "fetch_selection",
    "make_wanted",
    "match_item",
]
