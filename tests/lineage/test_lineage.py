from http.client import responses

import pytest


class TestLineage():
    @pytest.fixture
    def mock_responses(self):
        with responses.RequestsMock() as rsps:
            yield rsps

    def test_init_client(self):
        from  openhexa.toolbox import lineage
        lineage.init_client(url="http://localhost:8080", workspace_slug="default", pipeline_slug="test_pipeline")
        assert lineage._client is not None
        assert lineage._client.client is not None
        assert lineage._client.namespace == "default"


