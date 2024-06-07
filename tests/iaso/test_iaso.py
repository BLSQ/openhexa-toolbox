import pytest
import responses
import os

from openhexa.toolbox.iaso import IASO
from openhexa.toolbox.iaso.api import IASOConnection, Api
from tests.iaso.fixtures.iaso_api_fixtures import iaso_mocked_auth_token, iaso_mocked_forms, iaso_mocked_orgunits, \
    iaso_mocked_projects

IASO_CONNECTION_IDENTIFIER = "IASO_BRU"


class TestIasoAPI:
    @pytest.fixture(autouse=True)
    def set_env_variables(self):
        os.environ[IASO_CONNECTION_IDENTIFIER + "_URL"] = "https://iaso-staging.bluesquare.org"
        os.environ[IASO_CONNECTION_IDENTIFIER + "_USERNAME"] = "user"
        os.environ[IASO_CONNECTION_IDENTIFIER + "_PASSWORD"] = "test"

        self.iaso_connection = IASOConnection("IASO_BRU")

    @pytest.fixture
    def mock_responses(self):
        with responses.RequestsMock() as rsps:
            yield rsps

    def test_authenticate(self, mock_responses):
        mock_responses.add(
            responses.POST,
            "https://iaso-staging.bluesquare.org/api/token/",
            json=iaso_mocked_auth_token,
            status=200
        )

        iaso_api_client = Api(self.iaso_connection)
        r = iaso_api_client.authenticate()

        assert r is not None

    def test_get_projects(self, mock_responses):
        mock_responses.add(
            responses.POST,
            "https://iaso-staging.bluesquare.org/api/token/",
            json= iaso_mocked_auth_token,
            status=200
        )
        mock_responses.add(
            responses.GET,
            "https://iaso-staging.bluesquare.org/api/projects",
            json=iaso_mocked_projects,
            status=200
        )
        iaso = IASO(self.iaso_connection)
        r = iaso.get_projects()
        assert len(r) > 0

    def test_get_org_units(self, mock_responses):
        mock_responses.add(
            responses.POST,
            "https://iaso-staging.bluesquare.org/api/token/",
            json=iaso_mocked_auth_token,
            status=200
        )
        mock_responses.add(
            responses.GET,
            "https://iaso-staging.bluesquare.org/api/orgunits",
            json=iaso_mocked_orgunits,
            status=200
        )
        iaso = IASO(self.iaso_connection)
        r = iaso.get_org_units()
        assert len(r) > 0

    def test_get_submission_forms(self, mock_responses):
        mock_responses.add(
            responses.POST,
            "https://iaso-staging.bluesquare.org/api/token/",
            json=iaso_mocked_auth_token,
            status=200
        )
        mock_responses.add(
            responses.POST,
            "https://iaso-staging.bluesquare.org/api/forms",
            json=iaso_mocked_forms,
            status=200
        )
        iaso = IASO(self.iaso_connection)
        r = iaso.get_submissions_forms([781], [149])
        assert len(r) > 0
