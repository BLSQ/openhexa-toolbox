import pytest
import responses
import os

from openhexa.toolbox.iaso import IASO
from openhexa.toolbox.iaso.api import IASOConnection, Api

IASO_CONNECTION_IDENTIFIER = "IASO_BRU"


class TestIasoAPI:
    @pytest.fixture(autouse=True)
    def set_env_variables(self):
        os.environ[IASO_CONNECTION_IDENTIFIER + "_URL"] = "https://iaso-staging.bluesquare.org/"
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
            json={
                "access": 'eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzY5MDEwNCwiaWF0IjoxNzE3NjkwMTA0fQ.WsmnKvyKFR2eWNL4wD4yrnd6F9CDBV2dCaMx9lE6V84',
                "refresh": 'eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzY5MDEwNCwiaWF0IjoxNzE3NjkwMTA0fQ.WsmnKvyKFR2eWNL4wD4yrnd6F9CDBV2dCaMx9lE6V84'},
            status=200
        )

        iaso_api_client = Api(self.iaso_connection)
        r = iaso_api_client.authenticate()

        assert r is not None

    def test_get_projects(self, mock_responses):
        mock_responses.add(
            responses.GET,
            "https://iaso-staging.bluesquare.org/api/projects/",
            json={'projects': [{'id': 149, 'name': 'Pathways', 'app_id': 'pathways', 'feature_flags': [
                {'id': 3, 'name': 'GPS point for each form', 'code': 'TAKE_GPS_ON_FORM'},
                {'id': 7, 'name': 'Mobile: Show data collection screen', 'code': 'DATA_COLLECTION'},
                {'id': 12, 'name': 'Mobile: Finalized forms are read only', 'code': 'MOBILE_FINALIZED_FORM_ARE_READ'},
                {'id': 4, 'name': 'Authentication', 'code': 'REQUIRE_AUTHENTICATION'}],
                                'created_at': 1710153966.532745, 'updated_at': 1717664805.185712,
                                'needs_authentication': True
                                }]},
            status=200
        )
        iaso = IASO(self.iaso_connection)
        r = iaso.get_projects()
        assert len(r) > 0

    def test_get_org_units(self, mock_responses):
        mock_responses.add(
            responses.GET,
            "https://iaso-staging.bluesquare.org/api/orgunits/",
            json={'orgUnits': [{'name': 'ACEH', 'id': 1978297, 'parent_id': 1978331, 'org_unit_type_id': 781,
                                'org_unit_type_name': 'Province',
                                'validation_status': 'VALID', 'created_at': 1712825023.085615,
                                'updated_at': 1712828860.665764}]},
            status=200
        )
        iaso = IASO(self.iaso_connection)
        r = iaso.get_org_units()
        assert len(r) > 0

    def test_get_submission_forms(self, mock_responses):
        mock_responses.add(
            responses.POST,
            "https://iaso-staging.bluesquare.org/api/forms/",
            json={'forms': [{'id': 278, 'name': 'Test (form styling)', 'form_id': 'pathways_indonesia_survey_1',
                             'device_field': 'deviceid',
                             'location_field': '', 'org_unit_types': [
                    {'id': 781, 'name': 'Province', 'short_name': 'Prov', 'created_at': 1712825023.047433}],
                             'projects': [{'id': 149, 'name': 'Pathways'}], 'created_at': 1713171086.141424}]},
            status=200
        )
        iaso = IASO(self.iaso_connection)
        r = iaso.get_submissions_forms([781], [149])
        assert len(r) > 0
