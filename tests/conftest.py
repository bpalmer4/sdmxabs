"""Pytest configuration and fixtures for sdmxabs tests."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Test data directory
TEST_DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def temp_cache_dir():
    """Create a temporary cache directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_response():
    """Create a mock HTTP response."""
    response = Mock()
    response.status_code = 200
    response.headers = {"Content-Type": "application/xml"}
    response.content = b"<test>mock content</test>"
    return response


@pytest.fixture
def sample_xml_data():
    """Create sample XML data for testing."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
    <message:StructureSpecificData xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
                                   xmlns:gen="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic">
        <message:DataSet>
            <gen:Series>
                <gen:SeriesKey>
                    <gen:Value id="FREQ" value="Q"/>
                    <gen:Value id="REGION" value="AUS"/>
                </gen:SeriesKey>
                <gen:Attributes>
                    <gen:Value id="UNIT_MEASURE" value="INDEX"/>
                </gen:Attributes>
                <gen:Obs>
                    <gen:ObsDimension value="2023-Q1"/>
                    <gen:ObsValue value="100.5"/>
                </gen:Obs>
                <gen:Obs>
                    <gen:ObsDimension value="2023-Q2"/>
                    <gen:ObsValue value="101.2"/>
                </gen:Obs>
            </gen:Series>
        </message:DataSet>
    </message:StructureSpecificData>"""


@pytest.fixture
def sample_metadata_xml():
    """Create sample metadata XML for testing."""
    return b"""<?xml version="1.0" encoding="UTF-8"?>
    <message:Structure xmlns:message="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
                       xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
                       xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
        <message:Structures>
            <str:DataFlows>
                <str:DataFlow id="WPI">
                    <com:Name>Wage Price Index</com:Name>
                </str:DataFlow>
            </str:DataFlows>
            <str:Codelists>
                <str:Codelist id="FREQ">
                    <com:Name>Frequency</com:Name>
                    <str:Code id="Q">
                        <com:Name>Quarterly</com:Name>
                    </str:Code>
                </str:Codelist>
            </str:Codelists>
        </message:Structures>
    </message:Structure>"""


@pytest.fixture
def mock_requests_get():
    """Mock requests.get for testing."""
    with patch("requests.get") as mock_get:
        yield mock_get


@pytest.fixture
def mock_acquire_xml():
    """Mock xml_base.acquire_xml for testing."""
    with patch("sdmxabs.xml_base.acquire_xml") as mock_acquire:
        yield mock_acquire


@pytest.fixture
def sample_dataframe_data():
    """Sample data for DataFrame tests."""
    import pandas as pd

    data = {
        "series1": pd.Series([100.0, 101.0, 102.0], index=["2023-Q1", "2023-Q2", "2023-Q3"]),
        "series2": pd.Series([200.0, 201.0, 202.0], index=["2023-Q1", "2023-Q2", "2023-Q3"]),
    }

    meta = {
        "series1": pd.Series({"FREQ": "Quarterly", "UNIT": "Index", "REGION": "Australia"}),
        "series2": pd.Series({"FREQ": "Quarterly", "UNIT": "Index", "REGION": "Australia"}),
    }

    return pd.DataFrame(data), pd.DataFrame(meta).T
