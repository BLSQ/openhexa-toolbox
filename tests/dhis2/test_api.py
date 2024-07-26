from collections import namedtuple
from pathlib import Path

import pytest
import responses

from openhexa.toolbox.dhis2 import Api

VERSIONS = ["2.36", "2.37", "2.38", "2.39", "2.40", "2.41"]


@pytest.fixture
def con():
    Connection = namedtuple("Connection", ["url", "username", "password"])
    return Connection("http://localhost:8080", "admin", "district")


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_authenticate(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "api_authenticate.yaml"))
    Api(con, cache_dir=None)


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_get(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "api_authenticate.yaml"))
    responses._add_from_file(Path(responses_dir, "api_get.yaml"))
    api = Api(con, cache_dir=None)
    r = api.get("system/info")
    assert "version" in r


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_get_paged(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "api_authenticate.yaml"))
    responses._add_from_file(Path(responses_dir, "api_get_paged.yaml"))
    api = Api(con, cache_dir=None)
    r = api.get_paged(endpoint="organisationUnits", params={"fields": "id,name"})
    for page in r:
        assert page
