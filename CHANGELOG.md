Version 0.2.3 - 25 July 2025 (Canberra, Australia)

* Testing
    - fixed 9 failing integration tests, improving test reliability from 117/126 to 125/126 passing tests
    - improved mock patching and HTTP request mocking in integration tests
    - fixed pandas deprecation warning in data processing using `pd.to_numeric(errors="coerce", downcast="float")`
    - added comprehensive performance optimizations reducing test runtime by 80% (61s → 13s)
    - implemented parallel test execution and created fast test runner script

Version 0.2.2 - 21 July 2025 (Canberra, Australia)

* Documentation
    - enhanced README with Quick Start Example, Common Usage Patterns, and Performance Tips

Version 0.2.1 - 19 July 2025 (Canberra, Australia)

* Major changes
    - restructured the `data_flows()` function to include the data structure identifier in the returned object.
    - added a new `data_structures()` function to retrieve the data structures for a specific data_structure_id. 
    - deleted the old `data_dimensions()` function (effectively replaced by `data_structures()`)
    - added a new `structure_ident()` function to get the structure_id for a specific flow_id
    - changed `code_list_for_dim(flow_id, ...)` to `code_list_for(struct_id, ...)` - now takes structure IDs instead of flow IDs
    - added a new `structure_from_flow_id()` function that combines `structure_ident()` and `data_structures()` for convenience

* Internal changes
    - updated data structure format: `"id"` key is now `"codelist_id"` in returned dictionaries
    - updated `validate_code_value()` function to use the new structure format
    - updated `build_key()` function to use `structure_from_flow_id()` internally
    - improved error handling and validation throughout metadata functions

* Breaking changes
    - `data_dimensions()` function removed - use `data_structures()` instead
    - `code_list_for_dim()` function removed - use `code_list_for()` instead  
    - `code_list_for()` now requires structure IDs, not flow IDs - use `structure_ident()` to convert
    - returned dictionary structure changed: access codelist IDs using `"codelist_id"` key instead of `"id"`

* Testing
    - updated all tests to reflect new API structure
    - fixed XML namespace handling in test mocks
    - updated test imports and function calls
    - improved test coverage for metadata functions
    - added cache clearing in tests to prevent interference

* Documentation
    - updated README.md to reflect new API structure
    - corrected function signatures and examples
    - added documentation for new functions
    - fixed typos and improved clarity

* Migration guide
    - Replace `data_dimensions(flow_id)` with `data_structures(structure_ident(flow_id))` or use `structure_from_flow_id(flow_id)`
    - Replace `code_list_for_dim(flow_id, dim)` with `code_list_for(structure_ident(flow_id), dim)`
    - Update dictionary access: change `result[dim]["id"]` to `result[dim]["codelist_id"]`
    - All metadata functions now return consistent dictionary structures with standardized key names

