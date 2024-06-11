import pytest
import responses

from openhexa.toolbox.iaso import IASO
from openhexa.toolbox.iaso.api_client import ApiClient
from tests.iaso.fixtures.iaso_api_fixtures import (
    iaso_mocked_auth_token,
    iaso_mocked_forms,
    iaso_mocked_orgunits,
    iaso_mocked_refreshed_auth_token,
    iaso_mocked_projects,
    iaso_mocked_instances,
)

IASO_CONNECTION_IDENTIFIER = "IASO_BRU"


class TestIasoAPI:
    @pytest.fixture
    def mock_responses(self):
        with responses.RequestsMock() as rsps:
            yield rsps

    def test_authenticate(self, mock_responses):
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/token/", json=iaso_mocked_auth_token, status=200
        )

        iaso_api_client = ApiClient("https://iaso-staging.bluesquare.org", "nfilipchukpathways", "uvz*wbg5jht1fxr0WCQ")
        iaso_api_client.authenticate()
        assert iaso_api_client.token == iaso_mocked_auth_token["access"]

    def test_get_projects(self, mock_responses):
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/token/", json=iaso_mocked_auth_token, status=200
        )
        mock_responses.add(
            responses.GET, "https://iaso-staging.bluesquare.org/api/projects/", json=iaso_mocked_projects, status=200
        )

        iaso = IASO("https://iaso-staging.bluesquare.org", "username", "password")
        r = iaso.get_projects()
        assert len(r) > 0

    def test_get_org_units(self, mock_responses):
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/token/", json=iaso_mocked_auth_token, status=200
        )
        mock_responses.add(
            responses.GET, "https://iaso-staging.bluesquare.org/api/orgunits/", json=iaso_mocked_orgunits, status=200
        )
        iaso = IASO("https://iaso-staging.bluesquare.org", "username", "password")
        r = iaso.get_org_units()
        assert len(r) > 0

    def test_get_forms(self, mock_responses):
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/token/", json=iaso_mocked_auth_token, status=200
        )
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/forms/", json=iaso_mocked_forms, status=200
        )
        iaso = IASO("https://iaso-staging.bluesquare.org", "username", "password")
        r = iaso.get_forms(org_units=[781], projects=[149])
        assert len(r) > 0

    def test_get_form_instances(self, mock_responses):
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/token/", json=iaso_mocked_auth_token, status=200
        )
        mock_responses.add(
            responses.GET, "https://iaso-staging.bluesquare.org/api/instances/", json=iaso_mocked_instances, status=200
        )
        iaso = IASO("https://iaso-staging.bluesquare.org", "user", "test")
        form_instances = iaso.get_form_instances(form_ids=276)
        assert len(form_instances) > 0

    def test_failing_forms(self, mock_responses):
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/token/", json=iaso_mocked_auth_token, status=200
        )
        mock_responses.add(
            responses.POST,
            "https://iaso-staging.bluesquare.org/api/forms/",
            json={"message": "Form submission failed"},
            status=500,
        )
        iaso = IASO("https://iaso-staging.bluesquare.org", "user", "test")
        try:
            iaso.get_forms([781], [149])
        except Exception as e:
            assert str(e) == "{'message': 'Form submission failed'}"

    def test_verify_expired_token(self, mock_responses):
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/token/", json=iaso_mocked_auth_token, status=200
        )
        mock_responses.add(
            responses.POST,
            "https://iaso-staging.bluesquare.org/api/forms/",
            json={"message": "No authorized"},
            status=401,
        )
        mock_responses.add(
            responses.POST,
            "https://iaso-staging.bluesquare.org/api/token/refresh/",
            json=iaso_mocked_refreshed_auth_token,
            status=200,
        )
        iaso = IASO("https://iaso-staging.bluesquare.org", "user", "test")
        iaso.get_forms([781], [149])
        assert mock_responses.calls[2].request.url == "https://iaso-staging.bluesquare.org/api/token/refresh/"
        assert iaso.api_client.token == iaso_mocked_refreshed_auth_token["access"]
