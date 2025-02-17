from collections import namedtuple
from pathlib import Path

import polars as pl
import pytest
import responses

from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.api import DHIS2Error
from openhexa.toolbox.dhis2 import dataframe

VERSIONS = ["2.36", "2.37", "2.38", "2.39", "2.40", "2.41"]


@pytest.fixture
def con():
    Connection = namedtuple("Connection", ["url", "username", "password"])
    return Connection("http://localhost:8080", "admin", "district")


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_connection_from_object(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    api = DHIS2(con, cache_dir=None)
    assert api is not None


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_connection_from_kwargs(version):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    api = DHIS2(url="http://localhost:8080", username="admin", password="district", cache_dir=None)
    assert api is not None


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_connection_from_kwargs_fails(version):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    with pytest.raises(DHIS2Error):
        DHIS2(url="http://localhost:8080", cache_dir=None)


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_data_elements(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "data_elements.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.data_elements()
    df = pl.DataFrame(r)
    assert len(df) > 1000


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_data_element_groups(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "data_element_groups.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.data_element_groups()
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_data_elements_paged(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "data_elements.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.data_elements(page=2, pageSize=1000)
    assert r.get("pager") is not None
    df = pl.DataFrame(r.get("items"))
    assert len(df) > 30


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_datasets(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "datasets.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.datasets()
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_organisation_unit_levels(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "organisation_unit_levels.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.organisation_unit_levels()
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_organisation_units(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "organisation_units.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.organisation_units()
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_organisation_units_paged(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "organisation_units.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.organisation_units(page=2, pageSize=1000)
    assert r.get("pager") is not None
    df = pl.DataFrame(r.get("items"))
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_indicators(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "indicators.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.indicators()
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_category_option_combos(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "category_option_combos.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.category_option_combos()
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_indicator_groups(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "indicator_groups.yaml"))
    api = DHIS2(con, cache_dir=None)
    r = api.meta.indicator_groups()
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_data_value_sets_get_data_elements(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "data_value_sets_get_data_elements.yaml"))
    api = DHIS2(con, cache_dir=None)

    if api.version < "2.39":
        with pytest.raises(DHIS2Error):
            r = api.data_value_sets.get(
                data_elements=["fbfJHSPpUQD", "cYeuwXTCPkU"],
                periods=["202306", "202307", "202308"],
                org_units=["uNEhNuBUr0i"],
            )

    else:
        r = api.data_value_sets.get(
            data_elements=["fbfJHSPpUQD", "cYeuwXTCPkU"],
            periods=["202306", "202307", "202308"],
            org_units=["uNEhNuBUr0i"],
        )
        df = pl.DataFrame(r)
        assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_data_value_sets_get_dataset(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "data_value_sets_get_dataset.yaml"))
    api = DHIS2(con, cache_dir=None)

    r = api.data_value_sets.get(
        datasets=["BfMAe6Itzgt"], periods=["202306", "202307", "202308"], org_units=["uNEhNuBUr0i"]
    )
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_data_value_sets_get_start_end(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "data_value_sets_get_start_end.yaml"))
    api = DHIS2(con, cache_dir=None)

    r = api.data_value_sets.get(
        datasets=["BfMAe6Itzgt"], start_date="2023-06-01", end_date="2023-09-01", org_units=["uNEhNuBUr0i"]
    )
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_analytics_get_data_elements(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "analytics_get_data_elements.yaml"))
    api = DHIS2(con, cache_dir=None)

    r = api.analytics.get(
        data_elements=["fbfJHSPpUQD", "cYeuwXTCPkU"], periods=["202306", "202307", "202308"], org_units=["uNEhNuBUr0i"]
    )
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_analytics_get_indicators(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "analytics_get_indicators.yaml"))
    api = DHIS2(con, cache_dir=None)

    r = api.analytics.get(
        indicators=["Uvn6LCg7dVU", "OdiHJayrsKo"],
        periods=["202306", "202307", "202308"],
        org_units=["uNEhNuBUr0i"],
        include_cocs=False,
    )
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_analytics_get_org_unit_levels(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "analytics_get_org_unit_levels.yaml"))
    api = DHIS2(con, cache_dir=None)

    r = api.analytics.get(
        data_elements=["fbfJHSPpUQD", "cYeuwXTCPkU"], periods=["202306", "202307", "202308"], org_unit_levels=[2]
    )
    df = pl.DataFrame(r)
    assert not df.is_empty()


@responses.activate
@pytest.mark.parametrize("version", VERSIONS)
def test_analytics_get_datasets(version, con):
    responses_dir = Path("tests", "dhis2", "responses", version)
    responses._add_from_file(Path(responses_dir, "dhis2_init.yaml"))
    responses._add_from_file(Path(responses_dir, "analytics_get_datasets.yaml"))
    api = DHIS2(con, cache_dir=None)

    r = api.analytics.get(datasets=["BfMAe6Itzgt"], periods=["202306", "202307", "202308"], org_units=["uNEhNuBUr0i"])
    df = pl.DataFrame(r)
    assert not df.i
