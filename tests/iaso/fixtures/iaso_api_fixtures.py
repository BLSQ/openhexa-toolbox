iaso_mocked_auth_token = {
    "access": "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzY5MDEwNCwiaWF0IjoxNzE3NjkwMTA0fQ.WsmnKvyKFR2eWNL4wD4yrnd6F9CDBV2dCaMx9lE6V84", # noqa: E501
    "refresh": "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzY5MDEwNCwiaWF0IjoxNzE3NjkwMTA0fQ.WsmnKvyKFR2eWNL4wD4yrnd6F9CDBV2dCaMx9lE6V84", # noqa: E501
}
iaso_mocked_refreshed_auth_token = {
    "access": "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzY5MDEwNCwiaWF0IjoxNzE3NzYwMTA0fQ._pXcqDw0QgvznvNuhVPwYyIms3H5imH-q6A7lIQJjYQ", # noqa: E501
    "refresh": "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzY5MDEwNCwiaWF0IjoxNzE3NjkwMTA0fQ.WsmnKvyKFR2eWNL4wD4yrnd6F9CDBV2dCaMx9lE6V84", # noqa: E501
}


iaso_mocked_forms = {
    "forms": [
        {
            "id": 278,
            "name": "Test (form styling)",
            "form_id": "pathways_indonesia_survey_1",
            "device_field": "deviceid",
            "location_field": "",
            "org_unit_types": [{"id": 781, "name": "Province", "short_name": "Prov", "created_at": 1712825023.047433}],
            "projects": [{"id": 149, "name": "Pathways"}],
            "created_at": 1713171086.141424,
        }
    ]
}

iaso_mocked_projects = {
    "projects": [
        {
            "id": 149,
            "name": "Pathways",
            "app_id": "pathways",
            "feature_flags": [
                {"id": 3, "name": "GPS point for each form", "code": "TAKE_GPS_ON_FORM"},
                {"id": 7, "name": "Mobile: Show data collection screen", "code": "DATA_COLLECTION"},
                {"id": 12, "name": "Mobile: Finalized forms are read only", "code": "MOBILE_FINALIZED_FORM_ARE_READ"},
                {"id": 4, "name": "Authentication", "code": "REQUIRE_AUTHENTICATION"},
            ],
            "created_at": 1710153966.532745,
            "updated_at": 1717664805.185712,
            "needs_authentication": True,
        }
    ]
}
iaso_mocked_orgunits = {
    "orgUnits": [
        {
            "name": "ACEH",
            "id": 1978297,
            "parent_id": 1978331,
            "org_unit_type_id": 781,
            "org_unit_type_name": "Province",
            "validation_status": "VALID",
            "created_at": 1712825023.085615,
            "updated_at": 1712828860.665764,
        }
    ]
}
