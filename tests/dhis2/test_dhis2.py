import pytest
import responses
import os

from openhexa.toolbox.dhis2 import DHIS2


class DHIS2Connection:
    """dummy connection class"""

    def __init__(self, url, username, password):
        self.url = url
        self.username = username
        self.password = password


URL = [
    ("2.37", "https://play.dhis2.org/2.37.9.1"),
    ("2.38", "https://play.dhis2.org/2.38.4.3"),
    ("2.39", "https://play.dhis2.org/2.39.2.1"),
    ("2.40", "https://play.dhis2.org/40.0.1"),
]
USERNAME = "admin"
DISTRICT = "district"


@responses.activate
@pytest.mark.parametrize("version,url", URL)
def test_authenticate(version, url):
    responses.patch(url)
    responses._add_from_file(file_path=os.path.join("tests", "dhis2", "responses", version, "authenticate.yaml"))
    con = DHIS2Connection(url, USERNAME, DISTRICT)
    dhis2 = DHIS2(con)
    assert dhis2.version.startswith(version)


@responses.activate
@pytest.mark.parametrize("version,url", URL)
def test_data_value_sets_get_start_end(version, url):
    responses.patch(url)
    responses._add_from_file(file_path=os.path.join("tests", "dhis2", "responses", version, "authenticate.yaml"))
    responses._add_from_file(
        file_path=os.path.join("tests", "dhis2", "responses", version, "data_value_sets", "get_start_end.yaml")
    )

    con = DHIS2Connection(url, USERNAME, DISTRICT)
    playground = DHIS2(con)

    playground.data_value_sets.MAX_DATA_ELEMENTS = 2
    playground.data_value_sets.MAX_ORG_UNITS = 6
    playground.data_value_sets.MAX_PERIODS = 1

    data = playground.data_value_sets.get(
        datasets=["QX4ZTUbOt3a"],
        org_units=[
            "JQr6TJx5KE3",
            "KbO0JnhiMwl",
            "f90eISKFm7P",
            "HNv1aLPdMYb",
            "hHKKi9WNoBG",
            "kpDoH80fwdX",
            "sFgNRYS5pBo",
            "vELbGdEphPd",
            "OTn9VMNEkdo",
            "jKZ0U8Og5aV",
        ],
        start_date="2022-01-01",
        end_date="2022-03-01",
    )

    assert len(data) == 607


@responses.activate
@pytest.mark.parametrize("version,url", URL)
def test_data_value_sets_get_period(version, url):
    responses.patch(url)
    responses._add_from_file(file_path=os.path.join("tests", "dhis2", "responses", version, "authenticate.yaml"))
    responses._add_from_file(
        file_path=os.path.join("tests", "dhis2", "responses", version, "data_value_sets", "get_period.yaml")
    )

    con = DHIS2Connection(url, USERNAME, DISTRICT)
    playground = DHIS2(con)

    playground.data_value_sets.MAX_DATA_ELEMENTS = 2
    playground.data_value_sets.MAX_ORG_UNITS = 6
    playground.data_value_sets.MAX_PERIODS = 1

    data = playground.data_value_sets.get(
        datasets=["QX4ZTUbOt3a"],
        org_units=[
            "JQr6TJx5KE3",
            "KbO0JnhiMwl",
            "f90eISKFm7P",
            "HNv1aLPdMYb",
            "hHKKi9WNoBG",
            "kpDoH80fwdX",
            "sFgNRYS5pBo",
            "vELbGdEphPd",
            "OTn9VMNEkdo",
            "jKZ0U8Og5aV",
        ],
        periods=["202201", "202202"],
    )

    assert len(data) == 607


@responses.activate
@pytest.mark.parametrize("version,url", URL)
def test_analytics_data_elements(version, url):
    responses.patch(url)
    responses._add_from_file(file_path=os.path.join("tests", "dhis2", "responses", version, "authenticate.yaml"))
    responses._add_from_file(
        file_path=os.path.join("tests", "dhis2", "responses", version, "analytics", "get_data_elements.yaml")
    )

    con = DHIS2Connection(url, USERNAME, DISTRICT)
    playground = DHIS2(con)

    playground.analytics.MAX_DX = 2
    playground.analytics.MAX_ORG_UNITS = 6
    playground.analytics.MAX_PERIODS = 1

    data = playground.analytics.get(
        data_elements=["FJs8ZjlQE6f", "yJwdE6XJbrF", "JMKtVQ5HasH"],
        periods=["202201", "202202"],
        org_units=[
            "JQr6TJx5KE3",
            "KbO0JnhiMwl",
            "f90eISKFm7P",
            "HNv1aLPdMYb",
            "hHKKi9WNoBG",
            "kpDoH80fwdX",
            "sFgNRYS5pBo",
            "vELbGdEphPd",
            "OTn9VMNEkdo",
            "jKZ0U8Og5aV",
        ],
    )

    assert len(data) == 14


@responses.activate
@pytest.mark.parametrize("version,url", URL)
def test_analytics_data_element_groups(version, url):
    responses.patch(url)
    responses._add_from_file(file_path=os.path.join("tests", "dhis2", "responses", version, "authenticate.yaml"))
    responses._add_from_file(
        file_path=os.path.join("tests", "dhis2", "responses", version, "analytics", "get_data_element_groups.yaml")
    )

    con = DHIS2Connection(url, USERNAME, DISTRICT)
    playground = DHIS2(con)

    playground.analytics.MAX_DX = 2
    playground.analytics.MAX_ORG_UNITS = 6
    playground.analytics.MAX_PERIODS = 1

    data = playground.analytics.get(
        data_element_groups=["IUZ0GidX0jh"],
        periods=["202201", "202202"],
        org_units=[
            "JQr6TJx5KE3",
            "KbO0JnhiMwl",
            "f90eISKFm7P",
            "HNv1aLPdMYb",
            "hHKKi9WNoBG",
            "kpDoH80fwdX",
            "sFgNRYS5pBo",
            "vELbGdEphPd",
            "OTn9VMNEkdo",
            "jKZ0U8Og5aV",
        ],
    )

    assert len(data) == 14


@responses.activate
@pytest.mark.parametrize("version,url", URL)
def test_analytics_org_unit_groups(version, url):
    responses.patch(url)
    responses._add_from_file(file_path=os.path.join("tests", "dhis2", "responses", version, "authenticate.yaml"))
    responses._add_from_file(
        file_path=os.path.join("tests", "dhis2", "responses", version, "analytics", "get_org_unit_group.yaml")
    )

    con = DHIS2Connection(url, USERNAME, DISTRICT)
    playground = DHIS2(con)

    playground.analytics.MAX_DX = 2
    playground.analytics.MAX_ORG_UNITS = 6
    playground.analytics.MAX_PERIODS = 1

    data = playground.analytics.get(
        data_elements=["FJs8ZjlQE6f", "yJwdE6XJbrF", "JMKtVQ5HasH"],
        periods=["202201", "202202"],
        org_unit_groups=["b0EsAxm8Nge"],
    )

    assert len(data) == 57


@responses.activate
@pytest.mark.parametrize("version,url", URL)
def test_analytics_org_unit_level(version, url):
    responses.patch(url)
    responses._add_from_file(file_path=os.path.join("tests", "dhis2", "responses", version, "authenticate.yaml"))
    responses._add_from_file(
        file_path=os.path.join("tests", "dhis2", "responses", version, "analytics", "get_org_unit_level.yaml")
    )

    con = DHIS2Connection(url, USERNAME, DISTRICT)
    playground = DHIS2(con)

    playground.analytics.MAX_DX = 2
    playground.analytics.MAX_ORG_UNITS = 6
    playground.analytics.MAX_PERIODS = 1

    data = playground.analytics.get(
        data_elements=["FJs8ZjlQE6f", "yJwdE6XJbrF", "JMKtVQ5HasH"],
        periods=["202201", "202202"],
        org_unit_levels=[1, 2],
    )

    assert len(data) == 252
