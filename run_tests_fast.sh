#!/bin/bash

# Fast test runner script - optimized for speed
# Usage: ./run_tests_fast.sh [pytest arguments]

echo "🚀 Running sdmxabs test suite (FAST MODE)"
echo "================================"

# Run tests in parallel with no coverage for maximum speed
pytest -n auto --no-cov --tb=short --disable-warnings "$@"

echo ""
echo "💡 Speed optimizations applied:"
echo "  - Parallel execution (-n auto)"
echo "  - No coverage collection (--no-cov)"
echo "  - Short traceback (--tb=short)"
echo "  - Warnings disabled (--disable-warnings)"
echo ""
echo "For full coverage reports, use: ./run_tests.sh"