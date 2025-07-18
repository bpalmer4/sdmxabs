# Test Data Directory

This directory contains test data files used by the sdmxabs test suite.

## Contents

- **sample_responses/**: Sample XML responses from the ABS SDMX API
- **fixtures/**: Test fixtures and mock data
- **expected_outputs/**: Expected outputs for comparison tests

## Usage

Test data files are automatically loaded by test fixtures in `conftest.py` and used throughout the test suite to ensure consistent, reproducible test results without requiring actual API calls.