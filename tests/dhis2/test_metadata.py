from unittest.mock import Mock, patch

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

    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_response.json.return_value = response
    mock_get.return_value = mock_response

    r = client.meta.organisation_unit_levels()
    assert mock_get.call_count == 1
    assert r == response


@patch.object(requests.Session, "get")
def test_organisation_units(mock_get, client):
    response = {
        "pager": {"page": 1, "total": 3, "pageSize": 1000, "pageCount": 1},
        "organisationUnits": [
            {
                "name": "Adonkia CHP",
                "path": "/ImspTQPwCqd/at6UHUQatSo/qtr8GGlm4gg/Rp268JB6Ne4",
                "id": "Rp268JB6Ne4",
                "level": 4,
            },
            {
                "name": "Afro Arab Clinic",
                "path": "/ImspTQPwCqd/at6UHUQatSo/qtr8GGlm4gg/cDw53Ej8rju",
                "id": "cDw53Ej8rju",
                "level": 4,
            },
            {
                "name": "Agape CHP",
                "path": "/ImspTQPwCqd/O6uvpzGd5pu/U6Kr7Gtpidn/GvFqTavdpGE",
                "id": "GvFqTavdpGE",
                "level": 4,
            },
        ],
    }

    mock_response = Mock(spec=Response)
    mock_response.status_code = 200
    mock_response.json.return_value = response
    mock_get.return_value = mock_response

    r = client.meta.organisation_units()
    assert mock_get.call_count == 1

    assert r == response
