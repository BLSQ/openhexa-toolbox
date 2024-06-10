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

        iaso_api_client = ApiClient("https://iaso-staging.bluesquare.org", "username", "password")
        iaso = IASO(iaso_api_client)
        r = iaso.get_projects()
        assert len(r) > 0

    def test_get_org_units(self, mock_responses):
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/token/", json=iaso_mocked_auth_token, status=200
        )
        mock_responses.add(
            responses.GET, "https://iaso-staging.bluesquare.org/api/orgunits/", json=iaso_mocked_orgunits, status=200
        )
        iaso_api_client = ApiClient("https://iaso-staging.bluesquare.org", "user", "test")
        iaso = IASO(iaso_api_client)
        r = iaso.get_org_units()
        assert len(r) > 0

    def test_get_submission_forms(self, mock_responses):
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/token/", json=iaso_mocked_auth_token, status=200
        )
        mock_responses.add(
            responses.POST, "https://iaso-staging.bluesquare.org/api/forms/", json=iaso_mocked_forms, status=200
        )
        iaso_api_client = ApiClient("https://iaso-staging.bluesquare.org", "user", "test")
        iaso = IASO(iaso_api_client)
        r = iaso.post_for_forms(org_units=[781], projects=[149])
        assert len(r) > 0

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
        iaso_api_client = ApiClient("https://iaso-staging.bluesquare.org", "user", "test")
        iaso = IASO(iaso_api_client)
        try:
            iaso.post_for_forms([781], [149])
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
        iaso_api_client = ApiClient("https://iaso-staging.bluesquare.org", "user", "test")
        iaso = IASO(iaso_api_client)
        iaso.post_for_forms([781], [149])
        assert mock_responses.calls[2].request.url == "https://iaso-staging.bluesquare.org/api/token/refresh/"
        assert iaso_api_client.token == iaso_mocked_refreshed_auth_token["access"]
