sdmxabs
-------
sdmxabs is a small python package to download data from the Australian Bureau of Statistics using its SDMX API. SDMX stands for Statistical Data and Metadata eXchange. This package is designed to be used interactively within a Jupyter notebook.

Usage
-----
```
import sdmxabs as sa
from sdmxabs import MatchType as Mt
```

Before you fetch data from the ABS, you need to know three things:

-   the flow identifier (flow_id) for the data you want. These are short strings, like "CPI" for the 
    Consumer Price Index. You find these using the `data_flows()` function. The `data_flows()` function also 
    provides a data structure identifier, which is used to get the data structure for the flow.
-   the data structure for this flow_id, which provides information on data dimensions and data attributes.
    The data dimensions are used to select a specific data series. If no dimensions are set, the `fetch()` function will return all data series for a flow identifier. The dimensions can be found using the `data_structures()` function.
-   the codes the ABS uses to specify selected data series against these dimensions. The codes can be found in the
    relevant code_lists using either the `code_lists()` function or the `code_list_for()` function. The code list names are part of the information provided with the data dimensions.

```
 +----------------------------------+
 |           data flow              |
 |(flow_id, flow_name, structure_id)|
 +---------------------------------+
                  |
                  V
 +----------------------------------+
 |         data structure           |
 |    (dimensions, attributes,      |
 |          codelist_ids)           |
 +----------------------------------+
                  |
                  V
 +----------------------------------+
 |            code list             |
 |       (code, name, parent)       |
 +----------------------------------+
```

**Note**: it is much, much faster to fetch one or two series using the data dimensions and code lists, than to fetch every data series associated with a flow identifier, and then search through the meta data for the data you want. 

Quick Start Example
-------------------

```python
import sdmxabs as sa

# 1. Find the Consumer Price Index flow
flows = sa.data_flows("CPI")
print(sa.frame(flows))

# 2. Get the data structure to see available dimensions
structure = sa.data_structures("CPI")
dimensions_df = sa.frame(structure)
print(dimensions_df[dimensions_df['role'] == 'dimension'])

# 3. Get specific CPI data - All groups, Seasonally Adjusted, Australia
data, meta = sa.fetch("CPI", {
    "MEASURE": "1",      # All groups CPI
    "TSEST": "20",       # Seasonally adjusted  
    "REGION": "AUS"      # Australia
})
print(data.head())
```

Common Usage Patterns
---------------------

**Multiple time series estimation types:**
```python
# Get both seasonally adjusted and trend data
data, meta = sa.fetch("CPI", {
    "MEASURE": "1",
    "TSEST": "20+30",    # Seasonally adjusted + Trend
    "REGION": "AUS"
})
```

**Using text-based selection (more intuitive):**
```python
from sdmxabs import MatchType as Mt

# Find CPI data using descriptive text
criteria = [
    sa.match_item("All groups", "MEASURE", Mt.PARTIAL),
    sa.match_item("Seasonally", "TSEST", Mt.PARTIAL),
    sa.match_item("Australia", "REGION", Mt.EXACT)
]
data, meta = sa.fetch_selection("CPI", criteria)
```

**Fetching multiple series efficiently:**
```python
import pandas as pd

# Create a DataFrame specifying multiple data series
wanted = pd.DataFrame([
    {"flow_id": "CPI", "MEASURE": "1", "TSEST": "20", "REGION": "AUS"},
    {"flow_id": "CPI", "MEASURE": "1", "TSEST": "20", "REGION": "NSW"},
    {"flow_id": "CPI", "MEASURE": "1", "TSEST": "20", "REGION": "VIC"}
])
data, meta = sa.fetch_multi(wanted)
```

Performance Tips
----------------

- **Use specific dimensions**: Always specify dimensions rather than fetching all data and filtering later
- **Leverage caching**: Metadata calls use cached results by default (`modality="prefer-cache"`)
- **Batch requests**: Use `fetch_multi()` for multiple series instead of multiple `fetch()` calls
- **Check data structure first**: Use `data_structures()` to understand available dimensions before building queries

Key functions
-------------

**Metadata**

`data_flows(flow_id:str='all', **kwargs: Unpack[GetFileKwargs]) -> dict[str, dict[str, str]]` - returns the ABS data. The data is returned in a dictionary with the flow identifier as the key and the attributes of that flow in a dictionary of name-value pairs. The attribute of greatest interest will be the data structure identifier. You can turn the returned value from `data_flows()` into a pandas DataFrame, with the following: `frame(data_flows("all"))`

`structure_ident(flow_id:str) -> str` - use this function to get the structure identifier for a specific flow_identifier. More than 99% of the flow identifiers are the same as the relevant structure identifier (but not 100% unfortunately). 

`data_structures(structure_id: str, **kwargs: Unpack[GetFileKwargs]) -> dict[str, dict[str, str]]` - returns the data structure associated with a specific ABS structure identifier. The data is returned in a dictionary of dimension/attribute names, and their associated information in a dictionary. You can turn the returned value from `data_structures()` into a pandas DataFrame, with the following: `frame(data_structures(structure_id))`

`code_lists(cl_id: str, **kwargs: Unpack[GetFileKwargs])-> dict[str, dict[str, str]]` The data is returned in a dictionary of codes and their associated information. The code list identifiers (cl_id) can be found in the data dimensions (see previous). You can turn the returned value from code_lists() into a pandas DataFrame, with the following: `frame (code_lists(cl_id))`

`code_list_for(struct_id: str, dim_name: str, **kwargs: Unpack[GetFileKwargs]) -> dict[str, dict[str, str]]` provides a quick method for getting the code list associated with a particular dimension in a data structure.

`structure_from_flow_id(flow_id: str, **kwargs: Unpack[GetFileKwargs]) -> dict[str, dict[str, str]]` provides a convenient method to get the data structure directly from a flow identifier, combining `structure_ident()` and `data_structures()` in one call. 

`frame(f: dict[str, dict[str, str]]) -> pd.DataFrame`- a utility function to convert the output from the key flow metadata functions above to a more human readable pandas DataFrame. 



**The actual ABS data**

Once you know what data you want, you can specify that information in a fetch() request.

`fetch(flow_id: str, selection: dict[str, str] | None, parameters: dict[str, str] | None = None, validate: bool = False, **kwargs: Unpack[GetFileKwargs]) -> tuple[pd.DataFrame, pd.DataFrame]:` - this function returns two DataFrames, the first is for data. The second is for the associated meta data. The column names in the data DataFrame will match the row names in the meta DataFrame. The selection argument is a dictionary, where the key is a dimension, and the value one or more codes from the relevant code list. Multiple values are concatenated with the "+" symbol. For example, the key value pair for extracting Seasonally Adjusted and Trend data is typically, `{"TSEST": "20+30"}`, where "TSEST" is the data dimenion. The validate argument reports if there were any issues translating your dimensions dictionary into the SDMX key. 

`fetch_multi(wanted: pd.DataFrame, parameters: dict[str, str] | None = None, validate: bool = False, **kwargs: Unpack[GetFileKwargs],) -> tuple[pd.DataFrame, pd.DataFrame]` - allows for multiple items to be fetched and returned. Each selection is a row in a DataFrame. The column names are the data dimensions, and the `flow_id`. The function returns two DataFrames, the first for data and the second for metadata.

`fetch_selection(flow_id: str, criteria: MatchCriteria, parameters: dict[str, str] | None = None, validate: bool = False, **kwargs: Unpack[GetFileKwargs]) -> tuple[pd.DataFrame, pd.DataFrame]` is a function to fetch ABS data based on match text strings to the code names used by the ABS. It allows for a more human readable and intuitive selection of ABS data. The function returns two DataFrames, the first for data and the second for metadata.

`measure_names(meta: pd.DataFrame) -> pd.Series:` a convenience function to convert a metadata DataFrame into a series of y-axis labels.

`recalibrate(data: pd.DataFrame, units: pd.Series, as_a_whole: bool = False) -> tuple[pd.DataFrame, pd.Series]` - a convenience function to recalibrate a DataFrame returned from a `fetch` function so that the absolute maximum value is between 1 and 1000. The labels (from `measure_names()`) are also adjusted.

`recalibrate_series(series: pd.Series, label: str) -> tuple[pd.Series, str]` - similar to recalibrate, for a single series.



Other
-----
`FlowMetaDict` is a useful type-alias for dict[str, dict[str, str]], the type returned by all of the meta data functions.

`make_wanted(flow_id: str, criteria: MatchCriteria) -> pd.DataFrame` - convert a selection criteria into a one line DataFrame that can be used as the wanted argument in fetch_multi().

`match_item(pattern: str, dimension: str, match_type: MatchType = MatchType.PARTIAL) -> MatchItem` create a `MatchItem` from the arguments. 

`GetFileKwargs` is a TypedDict. It specifies the possible arguments for data retrieval from the ABS:

-    `verbose: bool` - provide step-by-step information about the data retrieval process.
-    `modality: str` - Which will be one of "prefer-cache" or "prefer-url". By default, the calls
                            to the metadata functions [data_flows(), data_structures(), and code_lists()]
                            are set to "prefer-cache". The fetch functions default to "prefer-url", which
                            means they get the latest data from the ABS. 

`MatchType` is an Enum for specifying the type of text-matching to be used in `fetch_selection()`.

- `MatchType.EXACT` - for exact matches.
- `MatchType.PARTIAL` - for partial (case-insensitive) matches, and
- `MatchType.REGEX` - for regular expression matches. 

`MatchItem: tuple[str, str, MatchType]` is a tuple use to select codes from a code list. It has three elements: the pattern to match against a code name (from a code list), The dimension being matched, and the `MatchType`.

`MatchCriteria: Sequence[MatchItem] ` is a sequence of ```MatchItem``` used by `select_items()` to build a one line DataFrame, that can be used as the wanted argument to `fetch_multi()`. 

