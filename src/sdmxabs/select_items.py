"""Select items from the ABS Catalogue."""

from collections.abc import Sequence
from enum import Enum

import pandas as pd

from sdmxabs.flow_metadata import data_dimensions, data_flows


# --- some local types
class MatchType(Enum):
    """Enumeration for match types."""

    EXACT = 1
    PARTIAL = 2
    REGEX = 3


MatchElement = tuple[str, str, MatchType]
MatchElements = Sequence[MatchElement]


def select_items(
    flow_id: str,
    to_match: MatchElements,
) -> pd.DataFrame:
    """Build a 'wanted' Dataframe for use by fetch_multi() by matching data flow metadata.

    Args:
        flow_id (str): The ID of the data flow to select items from.
        to_match (MatchElements): A sequence of tuples containing the element name,
            the value to match, and the match type (exact, partial, or regex).

    Returns:
        pd.DataFrame: A DataFrame containing the selected items, which can be dropped
            into the call of the function fetch_multi().

    Raises:
        ValueError: If the flow_id is not valid or if no items match the criteria.

    """
    # --- some sanity checks
    if flow_id not in data_flows():
        raise ValueError(f"Invalid flow_id: {flow_id}.")
    dimensions = data_dimensions(flow_id)
    if not dimensions:
        raise ValueError(f"No dimensions found for flow_id: {flow_id}.")

    # --- not yet implemented
    _not_impleted = to_match
    return pd.DataFrame()
