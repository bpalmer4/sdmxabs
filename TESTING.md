# Testing Framework Documentation

## Overview

This document describes the comprehensive testing regime implemented for the sdmxabs package.

## Test Structure

```
tests/
├── __init__.py                  # Test package initialization
├── conftest.py                  # Shared fixtures and configuration
├── data/                        # Test data and fixtures
│   └── README.md
├── test_basic.py               # Basic functionality tests (working)
├── test_download_cache.py      # HTTP/caching tests (needs fixes)
├── test_fetch.py              # Core data fetching tests (needs fixes)
├── test_flow_metadata.py      # Metadata extraction tests (needs fixes)
├── test_integration.py        # End-to-end workflow tests (needs fixes)
├── test_measures.py           # Data processing tests (needs fixes)
└── test_xml_base.py           # XML parsing tests (working)
```

## Test Types

### ✅ Working Tests
- **Basic functionality tests** (`test_basic.py`) - 10 tests covering package imports, basic functionality, and data structures
- **XML base tests** (`test_xml_base.py`) - Tests for secure XML parsing with defusedxml

### 🔧 Tests Needing Fixes
- **Download cache tests** - HTTP request and caching functionality
- **Fetch tests** - Core data fetching and XML processing
- **Flow metadata tests** - Metadata extraction from SDMX API
- **Integration tests** - End-to-end workflows
- **Measures tests** - Data processing and recalibration

## Configuration Files

### `pyproject.toml`
- Test dependencies: pytest, pytest-cov, pytest-mock, pytest-xdist
- Ruff configuration with test-specific ignore rules
- Coverage settings (currently set to 20% minimum)

### `pytest.ini`
- Test discovery configuration
- Coverage reporting options
- Test markers for categorization
- Logging configuration

### `.coveragerc`
- Coverage measurement configuration
- File exclusions and reporting options

## Running Tests

### Test Runner Script
```bash
# Full test suite with coverage
./run_tests.sh

# Fast mode (no coverage, stop on first failure)
./run_tests.sh --fast

# Run specific test types
./run_tests.sh --unit
./run_tests.sh --integration

# Parallel execution
./run_tests.sh --parallel

# Verbose output
./run_tests.sh --verbose
```

### Direct pytest Commands
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_basic.py

# Run with coverage
pytest --cov=sdmxabs

# Run without coverage
pytest --no-cov
```

## Test Dependencies

### Core Testing
- `pytest` - Testing framework
- `pytest-cov` - Coverage reporting
- `pytest-mock` - Mocking utilities
- `pytest-xdist` - Parallel test execution

### Development Tools
- `ruff` - Linting and formatting
- `mypy` - Type checking
- `coverage` - Coverage analysis

## Test Fixtures

Located in `conftest.py`:
- `temp_cache_dir` - Temporary directory for cache testing
- `mock_response` - Mock HTTP response objects
- `sample_xml_data` - Sample SDMX XML data
- `sample_metadata_xml` - Sample metadata XML
- `mock_requests_get` - Mock for requests.get
- `mock_acquire_xml` - Mock for XML acquisition
- `sample_dataframe_data` - Sample pandas DataFrames

## Test Categories

### Unit Tests
- Test individual functions and classes
- Mock external dependencies
- Fast execution

### Integration Tests
- Test module interactions
- May require network access (mocked)
- Test complete workflows

### Mock Tests
- Isolate external dependencies
- Test error handling
- Verify API contracts

## Known Issues

### Current Status
- **Total Tests**: 113 tests across 6 test files
- **Working Tests**: 24 tests (basic + XML tests)
- **Failing Tests**: 89 tests (require XML namespace and mocking fixes)
- **Coverage**: 20% (needs improvement)

### Main Issues to Fix
1. **XML Namespace Handling** - Tests need proper SDMX namespace handling
2. **Mock Configuration** - Complex XML mocking for SDMX API responses
3. **Pandas Compatibility** - Handle pandas deprecation warnings
4. **Test Logic** - Some tests have incorrect assumptions about data structures

## Development Workflow

### Adding New Tests
1. Create test file in `tests/` directory
2. Follow naming convention: `test_*.py`
3. Use appropriate test class names: `Test*`
4. Add docstrings for test functions
5. Use fixtures from `conftest.py`

### Test Guidelines
- Keep tests isolated and independent
- Use descriptive test names
- Test both success and failure cases
- Mock external dependencies
- Aim for high coverage of critical paths

### Pre-commit Checks
The test runner automatically performs:
1. Code formatting check (ruff)
2. Type checking (mypy)
3. Test execution
4. Coverage reporting

## Future Improvements

### Short Term
1. Fix XML namespace handling in existing tests
2. Improve mock configurations for SDMX API
3. Increase coverage threshold to 80%
4. Add test markers for slow/network tests

### Long Term
1. Add property-based testing with hypothesis
2. Add performance benchmarks
3. Add mutation testing
4. Integrate with CI/CD pipeline
5. Add test documentation generation

## Security Testing

The test suite includes security-focused tests:
- XML External Entity (XXE) attack prevention
- Secure XML parsing with defusedxml
- Input validation testing
- Error handling for malicious inputs

## Contributing

When contributing tests:
1. Follow the existing test structure
2. Add appropriate fixtures to `conftest.py`
3. Use the test runner script to verify changes
4. Ensure all linting and type checking passes
5. Maintain or improve test coverage