import pytest

from unittest import mock
from uuid import uuid4

from openhexa.toolbox.hexa import OpenHEXAClient, OpenHEXA, NotFound
from .fixtures.openhexa_api_fixtures import (
    openhexa_mocked_workspaces,
    openhexa_mocked_workspaces_pipelines,
    openhexa_mocked_pipeline,
    openhexa_mocked_pipeline_run,
)


class TestOpenHEXAClient:
    def _mock_response(self, data):
        mock_response = mock.MagicMock()
        mock_response.json.return_value = data
        return mock_response

    def test_authenticate_with_creds_success(self):
        hexa = OpenHEXAClient("https://app.demo.openhexa.org")
        mock_response = self._mock_response(
            {
                "data": {
                    "login": {
                        "success": True,
                    }
                }
            },
        )

        with mock.patch.object(hexa, "_graphql_request", return_value=mock_response):
            assert hexa.authenticate(with_credentials=("username", "password")) is True

    def test_authenticate_with_creds_failed(self):
        hexa = OpenHEXAClient("https://app.demo.openhexa.org")
        mock_response = self._mock_response(
            {"data": {"login": {"success": False, "errors": ["INVALID_CREDENTIALS"]}}},
        )

        with mock.patch.object(hexa, "_graphql_request", return_value=mock_response):
            with pytest.raises(Exception) as e:
                hexa.authenticate(with_credentials=("username", "password"))
                assert str(e) == "Login failed : ['INVALID_CREDENTIALSS']"

    def test_authenticate_with_otq_enabled_failed(self):
        hexa = OpenHEXAClient("https://app.demo.openhexa.org")
        mock_response = self._mock_response(
            {"data": {"login": {"success": False, "errors": ["OTP_REQUIRED"]}}},
        )
        with mock.patch.object(hexa, "_graphql_request", return_value=mock_response):
            with pytest.raises(Exception) as e:
                hexa.authenticate(with_credentials=("username", "password"))
                assert str(e) == "Login failed : you need to disable two-factor authentication."


@mock.patch("openhexa.toolbox.hexa.hexa.OpenHEXAClient")
class TestOpenHEXA:
    def test_get_workspaces(self, mock_hexa_client):
        hexa = OpenHEXA("http://localhost:3000", token="token")

        with mock.patch.object(hexa, "query", return_value=openhexa_mocked_workspaces):
            assert hexa.get_workspaces() == openhexa_mocked_workspaces

    def test_get_pipelines(self, mock_hexa_client):
        hexa = OpenHEXA("http://localhost:3000", token="token")

        with mock.patch.object(hexa, "query", return_value=openhexa_mocked_workspaces_pipelines):
            assert hexa.get_pipelines("slug") == openhexa_mocked_workspaces_pipelines

    def test_get_pipeline_not_found(self, mock_hexa_client):
        hexa = OpenHEXA("http://localhost:3000", token="token")

        with mock.patch.object(hexa, "query", return_value=None):
            with pytest.raises(NotFound):
                hexa.get_pipeline("workspace", "code")

    def test_get_pipeline(self, mock_hexa_client):
        hexa = OpenHEXA("http://localhost:3000", token="token")

        with mock.patch.object(hexa, "query", return_value=openhexa_mocked_pipeline):
            assert hexa.get_pipeline("workspace", "code") == openhexa_mocked_pipeline

    def test_get_pipeline_run_not_found(self, mock_hexa_client):
        hexa = OpenHEXA("http://localhost:3000", token="token")

        with mock.patch.object(hexa, "query", return_value={"pipelineRun": None}):
            with pytest.raises(NotFound):
                hexa.get_pipeline_run(uuid4())

    def test_get_pipeline_run(self, mock_hexa_client):
        hexa = OpenHEXA("http://localhost:3000", token="token")

        with mock.patch.object(hexa, "query", return_value=openhexa_mocked_pipeline_run):
            assert hexa.get_pipeline_run(uuid4()) == openhexa_mocked_pipeline_run["pipelineRun"]

    def test_run_pipeline_not_found(self, mock_hexa_client):
        hexa = OpenHEXA("http://localhost:3000", token="token")
        data = {"runPipeline": {"success": False, "errors": ["PIPELINE_NOT_FOUND"]}}
        with mock.patch.object(hexa, "query", return_value=data):
            with pytest.raises(NotFound):
                hexa.run_pipeline(uuid4())

    def test_run_pipeline(self, mock_hexa_client):
        hexa = OpenHEXA("http://localhost:3000", token="token")
        run_id = uuid4()
        data = {"runPipeline": {"success": True, "errors": [], "run": {"id": run_id}}}

        with mock.patch.object(hexa, "query", return_value=data):
            assert hexa.run_pipeline(uuid4()) == {"id": run_id}
