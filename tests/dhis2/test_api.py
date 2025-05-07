from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import requests
from requests.models import Response

from openhexa.sdk.workspaces.connection import DHIS2Connection
from openhexa.toolbox.dhis2 import Api


@pytest.fixture
def client():
    return Api(url="http://localhost:8080", username="admin", password="district")


@pytest.fixture
def client_with_cache(tmp_path: Path):
    return Api(url="http://localhost:8080", username="admin", password="district", cache_dir=tmp_path)


def test_api_auth_from_connection():
    con = DHIS2Connection(
        url="http://localhost:8080",
        username="admin",
        password="district",
    )
    api = Api(con)
    assert api.session.auth.username == "admin"


def test_api_auth_from_credentials():
    api = Api(
        url="http://localhost:8080",
        username="admin",
        password="district",
    )
    assert api.session.auth.username == "admin"


def test_api_parse_api_url(client):
    url = client.parse_api_url("http://localhost:8080/api")
    assert url == "http://localhost:8080/api"
    url = client.parse_api_url("http://localhost:8080/api/")
    assert url == "http://localhost:8080/api"
    url = client.parse_api_url("https://example.com")
    assert url == "https://example.com/api"
    url = client.parse_api_url("https://example.com/dhis/v2.40/api//")
    assert url == "https://example.com/dhis/v2.40/api"


@patch.object(requests.Session, "get")
def test_api_get(mock_get, client):
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"version": "2.41"}
    mock_get.return_value = mock_response

    response = client.get("system/info")
    assert response == {"version": "2.41"}
    assert mock_get.call_count == 1


@patch.object(requests.Session, "get")
def test_api_get_cache(mock_get, client_with_cache):
    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"version": "2.41"}
    mock_get.return_value = mock_response

    # 1st call set the cache
    response = client_with_cache.get("system/info")
    assert response == {"version": "2.41"}
    assert mock_get.call_count == 1

    # 2nd call should use the cache
    response = client_with_cache.get("system/info")
    assert response == {"version": "2.41"}
    assert mock_get.call_count == 1


@pytest.fixture()
def mock_get_paged():
    """Mock the GET method to return 3 pages of data in 3 successive responses.

    Most of the API endpoints handles pagination using a `pager` object in the response.
    """
    with patch.object(requests.Session, "get") as mock_get:
        response1 = {"pager": {"page": 1, "nextPage": "http://example.com/next_page"}, "items": ["one", "two"]}
        response2 = {"pager": {"page": 2, "nextPage": "http://example.com/next_page"}, "items": ["three", "four"]}
        response3 = {"pager": {"page": 3}, "items": ["five"]}

        responses = []
        for response in [response1, response2, response3]:
            mock_response = Mock(spec=Response)
            mock_response.status_code = 200
            mock_response.json.return_value = response
            responses.append(mock_response)

        mock_get.side_effect = responses
        yield mock_get
        mock_get.reset_mock()


@pytest.fixture()
def mock_get_paged_tracker():
    """Mock the GET method to return 3 pages of data in 3 successive responses.

    Contrary to other endpoints, the tracker API does not use a `pager` object in the response.
    Instead, it uses a `pageCount` item to indicate the number of pages.
    """
    with patch.object(requests.Session, "get") as mock_get:
        response1 = {"page": 1, "pageCount": 3, "items": ["one", "two"]}
        response2 = {"page": 2, "items": ["three", "four"]}
        response3 = {"page": 3, "items": ["five"]}

        responses = []
        for response in [response1, response2, response3]:
            mock_response = Mock(spec=Response)
            mock_response.status_code = 200
            mock_response.json.return_value = response
            responses.append(mock_response)

        mock_get.side_effect = responses
        yield mock_get
        mock_get.reset_mock()


def test_api_get_paged(mock_get_paged, client):
    pages = client.get_paged("dataValueSets")
    page = next(pages)
    assert page["items"] == ["one", "two"]
    page = next(pages)
    assert page["items"] == ["three", "four"]
    page = next(pages)
    assert page["items"] == ["five"]

    # no page anymore
    with pytest.raises(StopIteration):
        next(pages)


def test_api_get_paged_tracker(mock_get_paged_tracker, client):
    pages = client.get_paged("tracker/events")
    page = next(pages)
    assert page["items"] == ["one", "two"]
    page = next(pages)
    assert page["items"] == ["three", "four"]
    page = next(pages)
    assert page["items"] == ["five"]

    # no page anymore
    with pytest.raises(StopIteration):
        next(pages)
