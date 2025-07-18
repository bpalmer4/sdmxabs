#!/bin/bash
# Test runner script for sdmxabs

set -e  # Exit on any error

echo "🧪 Running sdmxabs test suite"
echo "================================"

# Change to project directory
cd "$(dirname "$0")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}▶${NC} $1"
}

print_success() {
    echo -e "${GREEN}✅${NC} $1"
}

print_error() {
    echo -e "${RED}❌${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠️${NC} $1"
}

# Check if virtual environment is activated
if [[ -z "${VIRTUAL_ENV}" ]]; then
    print_warning "No virtual environment detected. Consider activating one."
fi

# Install test dependencies if not present
print_status "Checking test dependencies..."
if ! python -c "import pytest" 2>/dev/null; then
    print_status "Installing test dependencies..."
    if command -v uv &> /dev/null; then
        uv sync --group test
    else
        pip install pytest pytest-cov pytest-mock pytest-xdist coverage
    fi
fi

# Default test options
TEST_OPTIONS="--cov=sdmxabs --cov-report=term-missing --cov-report=html:htmlcov"
PARALLEL_OPTIONS=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --fast)
            TEST_OPTIONS="${TEST_OPTIONS} --no-cov -x"
            print_status "Running in fast mode (no coverage, stop on first failure)"
            shift
            ;;
        --parallel)
            PARALLEL_OPTIONS="-n auto"
            print_status "Running tests in parallel"
            shift
            ;;
        --unit)
            TEST_OPTIONS="${TEST_OPTIONS} -m unit"
            print_status "Running unit tests only"
            shift
            ;;
        --integration)
            TEST_OPTIONS="${TEST_OPTIONS} -m integration"
            print_status "Running integration tests only"
            shift
            ;;
        --verbose)
            TEST_OPTIONS="${TEST_OPTIONS} -v"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --fast        Run tests without coverage and stop on first failure"
            echo "  --parallel    Run tests in parallel using pytest-xdist"
            echo "  --unit        Run only unit tests"
            echo "  --integration Run only integration tests"
            echo "  --verbose     Verbose output"
            echo "  --help, -h    Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Run all tests with coverage"
            echo "  $0 --fast           # Quick test run"
            echo "  $0 --unit --verbose # Run unit tests with verbose output"
            echo "  $0 --parallel       # Run tests in parallel"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for available options"
            exit 1
            ;;
    esac
done

# Run pre-test checks
print_status "Running pre-test checks..."

# Check code formatting
print_status "Checking code formatting with ruff..."
if ! ruff check src/ tests/; then
    print_error "Code formatting issues found. Run 'ruff check --fix src/ tests/' to fix."
    exit 1
fi
print_success "Code formatting checks passed"

# Check type annotations
print_status "Checking type annotations with mypy..."
if ! mypy src/sdmxabs/; then
    print_error "Type checking failed"
    exit 1
fi
print_success "Type checking passed"

# Run the tests
print_status "Running pytest..."
echo "Test command: pytest ${PARALLEL_OPTIONS} ${TEST_OPTIONS} tests/"

if pytest ${PARALLEL_OPTIONS} ${TEST_OPTIONS} tests/; then
    print_success "All tests passed!"
    
    # Show coverage summary if coverage was run
    if [[ ! "${TEST_OPTIONS}" == *"--no-cov"* ]]; then
        echo ""
        print_status "Coverage report generated:"
        echo "  - Terminal summary: shown above"
        echo "  - HTML report: htmlcov/index.html"
        echo "  - XML report: coverage.xml"
        
        # Check coverage threshold
        if command -v coverage &> /dev/null; then
            COVERAGE_PERCENT=$(coverage report --format=total)
            if (( $(echo "$COVERAGE_PERCENT >= 80" | bc -l) )); then
                print_success "Coverage is ${COVERAGE_PERCENT}% (above 80% threshold)"
            else
                print_warning "Coverage is ${COVERAGE_PERCENT}% (below 80% threshold)"
            fi
        fi
    fi
    
    echo ""
    print_success "🎉 Test suite completed successfully!"
    exit 0
else
    print_error "Some tests failed"
    echo ""
    echo "💡 Tips for debugging test failures:"
    echo "  - Run with --verbose for more detailed output"
    echo "  - Run specific test file: pytest tests/test_specific.py"
    echo "  - Run specific test: pytest tests/test_file.py::TestClass::test_method"
    echo "  - Use --fast to stop on first failure"
    exit 1
fi