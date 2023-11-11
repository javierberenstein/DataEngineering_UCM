import zipfile
from io import BytesIO, StringIO
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from EMT.objects import UrlEMT

FIXTURE_DIR = Path(__file__).parent.resolve() / 'data'


# FIXTURES
@pytest.fixture
def html_response():
    with open(FIXTURE_DIR / 'html_url_emt_response.txt', 'r') as file:
        return file.read().encode()


@pytest.fixture
def mock_html_requests_get(html_response):
    with patch('EMT.objects.requests.get') as mock_get:
        mock_get.return_value = Mock(status_code=200, content=html_response)
        yield mock_get


@pytest.fixture
def url_emt():

    emt_instance = UrlEMT()

    return emt_instance


# HELPER FUNCTIONS
def create_mock_zip_file():

    mock_zip_bytes = BytesIO()

    with zipfile.ZipFile(mock_zip_bytes, 'w') as mock_zip:

        mock_zip.writestr('a_csv_file.csv', "some;csv;file")

    mock_zip_bytes.seek(0)

    return mock_zip_bytes.getvalue()


# TESTS
@patch('EMT.objects.requests.get')
def test_valid_urls_raise_connection_error(mock_get, url_emt):

    mock_get.return_value = Mock(status_code=401, content='Forbidden'.encode())

    with pytest.raises(ConnectionError):
        url_emt.select_valid_urls()

    mock_get.assert_called_with(UrlEMT.EMT + UrlEMT.GENERAL)


def test_valid_urls_get_links(mock_html_requests_get, url_emt):

    assert len(url_emt.select_valid_urls()) == 21


def test_get_url_fails_with_invalid_input(mock_html_requests_get, url_emt):

    with pytest.raises(ValueError) as e:
        url_emt.get_url(year=12, month=12)

    assert "Year value not correct" in str(e.value)

    with pytest.raises(ValueError) as e:
        url_emt.get_url(year=23, month=23)

    assert "Month value not correct" in str(e.value)

    with pytest.raises(ValueError) as e:
        url_emt.get_url(year=12, month=23)

    assert "Both month and year values are incorrect" in str(e.value)


def test_get_url_gets_valid_url(mock_html_requests_get, url_emt):

    assert url_emt.get_url(year=22, month=2) == '/getattachment/a829afec-cac8-427f-80ab-b5cb441514e9/trips_22_02_February-csv.aspx'  # noqa:E501


@patch('EMT.objects.requests.get')
def test_get_csv_retries_when_connection_fail(mock_get, url_emt):
    mock_get.side_effect = ConnectionAbortedError
    max_attempts = 4

    with pytest.raises(ConnectionError):

        url_emt.get_csv(
            year=22,
            month=2,
            sleep_time=0.1,
            max_attempts=max_attempts)

    assert mock_get.call_count == max_attempts


@patch('EMT.objects.requests.get')
def test_get_csv_retries_and_gets_correct_data(mock_get, url_emt):
    mock_get.side_effect = [ConnectionAbortedError,
                            Mock(status_code=200,
                                 content=create_mock_zip_file())]

    data = url_emt.get_csv(year=22, month=2, sleep_time=0.1)

    assert mock_get.call_count == 2

    assert isinstance(data, StringIO)

    assert data.getvalue() == "some;csv;file"

    
