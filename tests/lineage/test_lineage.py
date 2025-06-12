import json
from datetime import timezone, datetime

import pytest
import responses

from tests.lineage.fixtures.lineage import emit_event_example


class TestLineage:
    @pytest.fixture
    def mock_responses(self):
        with responses.RequestsMock() as rsps:
            yield rsps

    def test_init_client(self):
        from openhexa.toolbox import lineage

        lineage.init_client(url="http://localhost:8080", workspace_slug="default", pipeline_slug="test_pipeline")
        assert lineage._client is not None
        assert lineage._client.client is not None
        assert lineage._client.namespace == "default"

    def test_init_client_from_env(self, monkeypatch):
        from openhexa.toolbox import lineage

        monkeypatch.setenv("OPENLINEAGE_URL", "http://localhost:8080")
        monkeypatch.setenv("OPENLINEAGE_API_KEY", "test_api_key")

        lineage.init_client_from_env(
            workspace_slug="default",
            pipeline_slug="test_pipeline",
            pipeline_run_id="12345",
        )

        assert lineage._client is not None
        assert lineage._client.client is not None
        assert lineage._client.namespace == "default"
        assert lineage._client.run_id == "12345"


    def test_emit_run_event(self, mock_responses):
        from openhexa.toolbox import lineage
        from openhexa.toolbox.lineage.client import OpenHexaOpenLineageClient
        from openlineage.client.event_v2 import RunState

        lineage.init_client(
            url="http://localhost:3000",
            workspace_slug="default",
            pipeline_slug="test_pipeline",
            pipeline_run_id="abe36e6a-8af7-4718-a753-c6d2054d1ecf",
        )

        mock_responses.add(responses.POST, "http://localhost:3000/api/v1/lineage", json={}, status=200)

        lineage.event(
            event_type=RunState.START,
            task_name="test_task",
            inputs=[],
            outputs=[],
            start_time=datetime(2025, 6, 12, 7, 24, 48, 727726, tzinfo=timezone.utc),
            end_time=datetime(2025, 6, 12, 7, 27, 48, 727726, tzinfo=timezone.utc),
        )

        assert mock_responses.calls[0].request.url == "http://localhost:3000/api/v1/lineage"
        assert mock_responses.calls[0].request.method == "POST"
        assert json.loads(mock_responses.calls[0].request.body) == emit_event_example
