sdmxabs
-------
sdmxabs is a small python package to download data from the Australian Bureau of Statistics using its SDMX API. SDMX stands for Statistical Data and Metadata eXchange.

Usage
-----
```
import sdmxabs as sa
from sdmxabs import MatchType as Mt
```

Before you fetch data from the ABS, you need to know three things:

-   the flow identifier (flow_id) for the data you want. These are short strings, like "CPI" for the 
    Consumer Price Index. You find these using the ```data_flows()``` function
-   the dimensions for this flow_id, which are used to select a specific data series. If no dimensiosn are set, the fetch() function will return all data series for a flow identifier. The dimensions can be found using the ```data_dimensions()``` function.
-   the codes the ABS uses to specify selected data series against these dimensions. The codes can be found in the relevant code_lists using the ```code_lists()``` function. The code list names are part of the information provided with the data dimenions.

**Note**: it is much, much faster to fetch one or two series using the data dimensions and code lists, than to fetch every data series associated with a flow identifier, and then search through the meta data for the data you want. 

Key functions
-------------


```data_flows(flow_id:str='all', **kwargs: Unpack[GetFileKwargs]) -> dict[str, dict[str, str]]``` - returns the ABS data. The data is returned in a dictionary with the flow identifier as the key and the atrributes of that flow in a dictionary of name-value pairs. You can turn the returned value from data_flows() into a pandas DataFrame, with the following: ```pd.DataFrame(data_flows()).T```

```data_dimensions(flow_id: str, **kwargs: Unpack[GetFileKwargs]) -> dict[str, dict[str, str]]``` - returns the data dimensions associated with a specific ABS dataflow. The data is returned in a dictionary of dimension names, and their associated information. You can turn the returned value from data_dimensions() into a pandas DataFrame, with the following: ```pd.DataFrame(data_dimensions(flow_id)).T```

```code_lists(cl_id: str, **kwargs: Unpack[GetFileKwargs])-> dict[str, dict[str, str]]``` The data is returned in a dictionary of codes and their associated information. The code list identifiers (cl_id) can be found in the data dimensions (see previous). You can turn the returned value from code_lists() into a pandas DataFrame, with the following: ```pd.DataFrane(code_lists(cl_id)).T```

Once you know what data you want, you can specify that information in a fetch() request.

```fetch(flow_id: str, dims: dict[str, str] | None, validate: bool, **kwargs: Unpack[GetFileKwargs]) -> tuple[pd.DataFrame, pd.DataFrame]:``` - this function returns two DataFrames, the first is for data. The second is for the associated meta data. The column names in the data DataFrame will match the row names in the meta DataFrame. The dims argument is a dictionary, where the key is a dimension, and the value one or more codes from the relevant code list. Multiple values are concatenated with the "+" symbol. For example, the key value pair for extracting Seasonally Adjusted and Trend data is typically, ```{"TSEST": "20+30"}```, where "TSEST" is the data dimenion. The validate argument reports if there were any issues translating your dimensions dictionary into  the SDMX key. 

```fetch_multi( wanted: pd.DataFrame, validate: bool = False, **kwargs: Unpack[GetFileKwargs],) -> tuple[pd.DataFrame, pd.DataFrame]``` - allows for multiple items to be fetched and returned. Each selection is a row in a DataFrame. The column names are the data dimensions, and the ```flow_id```. 

```fetch_selection( flow_id: str, criteria: MatchCriteria, validate: bool, **kwargs: Unpack[GetFileKwargs]) -> tuple[pd.DataFrame, pd.DataFrame]``` is a function to fetch ABS data based on match text strings to the code names used by the ABS. It allows for a more human readable and intuitive selection of ABS data. 

Other
-----
```make_wanted(flow_id: str, criteria: MatchCriteria) -> pd.DataFrame``` - convert a selection criteria into a one line DataFrame that can be used as the wanted argument in fetch_multi().

```match_item(pattern: str, dimension: str, match_type: MatchType = MatchType.PARTIAL) -> MatchItem``` create a ```MatchItem``` from the arguments. 

```GetFileKwargs``` is a TypedDict. It specifies the possible arguments for data retrieval from the ABS:

-    ```verbose: bool``` - provide step-by-step information about the data retrieval process.
-    ```modaility: str``` - Which will be one of "prefer-cache" or "prefer-url". By defaulkt, the calls
                            to the metadata functions [data_flows(), data_dimensions(), and code_lists()]
                            are set to "prefer-cache". The fetch functions default to "prefer-url", which
                            means they get the latest data from the ABS. 

```MatchType``` is an Enum for specifying the type of text-matching to be used in ```fetch_selection()```.

- ```MatchType.EXACT``` - for exact matches.
- ```MatchType.PARTIAL``` - for partial (case-insensitive) matches, and
- ```MatchType.REGEX``` - for regular expression matches. 

```MatchItem: tuple[str, str, MatchType]``` is a tuple use to select codes from a code list. It has three elements: the pattern to match against a code name (from a code list), The dimension being matched, and the ```MatchType```.

```MatchCriteria: Sequence[MatchItem] ``` is a sequence of ```MatchItem``` used by ```select_items()``` to build a one line DataFrame, that can be used as the wanted argument to ```fetch_multi()```. 

