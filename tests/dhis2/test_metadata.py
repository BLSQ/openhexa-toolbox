from unittest.mock import MagicMock, patch

import pytest
import requests
from requests.models import Response

from openhexa.toolbox.dhis2 import DHIS2


@pytest.fixture
def client():
    return DHIS2(url="http://localhost:8080", username="admin", password="district")


@patch.object(requests.Session, "get")
def test_org_unit_levels(mock_get, client):
    response = [
        {"name": "National", "level": 1, "id": "H1KlN4QIauv"},
        {"name": "District", "level": 2, "id": "wjP19dkFeIk"},
        {"name": "Chiefdom", "level": 3, "id": "tTUf91fCytl"},
        {"name": "Facility", "level": 4, "id": "m9lBJogzE95"},
    ]

    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 200
    mock_response.json.return_value = response
    mock_get.return_value = mock_response

    r = client.meta.organisation_unit_levels()
    assert mock_get.call_count == 1
    assert r == response
