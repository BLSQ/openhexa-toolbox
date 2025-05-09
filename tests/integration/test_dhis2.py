"""Test DHIS2 module against a local DHIS2 instance."""

from datetime import datetime

import polars as pl
import pytest

from openhexa.toolbox.dhis2 import DHIS2, dataframe


@pytest.fixture
def client():
    return DHIS2(
        url="http://localhost:8080",
        username="admin",
        password="district",
    )


def test_get_organisation_units(client):
    df = dataframe.get_organisation_units(client)
    assert len(df) > 100
    assert df.schema == {
        "id": pl.String,
        "name": pl.String,
        "level": pl.Int64,
        "opening_date": pl.Datetime(time_unit="ms", time_zone=None),
        "closed_date": pl.Datetime(time_unit="ms", time_zone=None),
        "level_1_id": pl.String,
        "level_1_name": pl.String,
        "level_2_id": pl.String,
        "level_2_name": pl.String,
        "level_3_id": pl.String,
        "level_3_name": pl.String,
        "level_4_id": pl.String,
        "level_4_name": pl.String,
        "geometry": pl.String,
    }


def test_get_data_elements(client):
    df = dataframe.get_data_elements(client)
    assert len(df) > 100
    assert df.schema == {"id": pl.String, "name": pl.String, "value_type": pl.String}


def test_get_category_option_combos(client):
    df = dataframe.get_category_option_combos(client)
    assert len(df) > 100
    assert df.schema == {"id": pl.String, "name": pl.String}


def test_data_element_groups(client):
    df = dataframe.get_data_element_groups(client)
    assert len(df) > 10
    assert df.schema == {
        "id": pl.String,
        "name": pl.String,
        "data_elements": pl.List(pl.String),
    }


def test_get_organisation_unit_groups(client):
    df = dataframe.get_organisation_unit_groups(client)
    assert len(df) > 10
    assert df.schema == {"id": pl.String, "name": pl.String, "organisation_units": pl.List(pl.String)}


def test_get_datasets(client):
    df = dataframe.get_datasets(client)
    assert len(df) > 10
    assert df.schema == {
        "id": pl.String,
        "name": pl.String,
        "organisation_units": pl.List(pl.String),
        "data_elements": pl.List(pl.String),
        "indicators": pl.List(pl.String),
        "period_type": pl.String,
    }


def test_get_programs(client):
    df = dataframe.get_programs(client)
    assert len(df) > 10
    assert df.schema == {
        "id": pl.String,
        "name": pl.String,
    }


def get_tracked_entity_types(client):
    df = dataframe.get_tracked_entity_types(client)
    assert len(df) > 5
    assert df.schema == {
        "id": pl.String,
        "name": pl.String,
    }


def get_attributes(client):
    df = dataframe.get_attributes(client)
    assert len(df) > 5
    assert df.schema == {
        "id": pl.String,
        "name": pl.String,
    }


def test_get_indicator_groups(client):
    df = dataframe.get_indicator_groups(client)
    assert len(df) > 10
    assert df.schema == {
        "id": pl.String,
        "name": pl.String,
        "indicators": pl.List(pl.String),
    }


def test_get_indicators(client):
    df = dataframe.get_indicators(client)
    assert len(df) > 10
    assert df.schema == {
        "id": pl.String,
        "name": pl.String,
        "numerator": pl.String,
        "denominator": pl.String,
    }


def test_extract_dataset(client):
    df = dataframe.extract_dataset(
        dhis2=client,
        dataset="BfMAe6Itzgt",
        start_date=datetime(2022, 7, 1),
        end_date=datetime(2022, 8, 1),
        org_units=["DiszpKrYNg8"],
        include_children=False,
    )

    assert len(df) > 50
    assert df.schema == {
        "data_element_id": pl.String,
        "period": pl.String,
        "organisation_unit_id": pl.String,
        "category_option_combo_id": pl.String,
        "attribute_option_combo_id": pl.String,
        "value": pl.String,
        "created": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "last_updated": pl.Datetime(time_unit="ms", time_zone="UTC"),
    }


def test_extract_data_element_group(client):
    df = dataframe.extract_data_element_group(
        dhis2=client,
        data_element_group="h9cuJOkOwY2",
        start_date=datetime(2020, 11, 1),
        end_date=datetime(2021, 2, 5),
        org_units=["jPidqyo7cpF"],
        include_children=True,
    )

    assert len(df) > 20
    assert df.schema == {
        "data_element_id": pl.String,
        "period": pl.String,
        "organisation_unit_id": pl.String,
        "category_option_combo_id": pl.String,
        "attribute_option_combo_id": pl.String,
        "value": pl.String,
        "created": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "last_updated": pl.Datetime(time_unit="ms", time_zone="UTC"),
    }


def test_extract_data_elements(client):
    df = dataframe.extract_data_elements(
        client,
        data_elements=["pikOziyCXbM", "x3Do5e7g4Qo"],
        start_date=datetime(2020, 11, 1),
        end_date=datetime(2021, 2, 5),
        org_units=["vELbGdEphPd", "UugO8xDeLQD"],
    )
    assert len(df) > 5
    assert df.schema == {
        "data_element_id": pl.String,
        "period": pl.String,
        "organisation_unit_id": pl.String,
        "category_option_combo_id": pl.String,
        "attribute_option_combo_id": pl.String,
        "value": pl.String,
        "created": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "last_updated": pl.Datetime(time_unit="ms", time_zone="UTC"),
    }


def test_extract_analytics(client):
    df = dataframe.extract_analytics(
        client, periods=["2021"], data_elements=["pikOziyCXbM", "x3Do5e7g4Qo"], org_unit_levels=[2]
    )
    assert len(df) > 50
    assert df.schema == {
        "data_element_id": pl.String,
        "category_option_combo_id": pl.String,
        "organisation_unit_id": pl.String,
        "period": pl.String,
        "value": pl.String,
    }


def test_import_data_values(client):
    df = pl.DataFrame(
        [
            {
                "data_element_id": "pikOziyCXbM",
                "period": "202401",
                "organisation_unit_id": "O6uvpzGd5pu",
                "category_option_combo_id": "psbwp3CQEhs",
                "attribute_option_combo_id": "HllvX50cXC0",
                "value": "100",
            },
            {
                "data_element_id": "pikOziyCXbM",
                "period": "202402",
                "organisation_unit_id": "O6uvpzGd5pu",
                "category_option_combo_id": "psbwp3CQEhs",
                "attribute_option_combo_id": "HllvX50cXC0",
                "value": "150",
            },
        ]
    )
    report = dataframe.import_data_values(client, data=df, import_strategy="CREATE_AND_UPDATE", dry_run=True)
    for status in ("imported", "updated", "ignored", "deleted"):
        assert report[status] >= 0


def test_import_data_values_with_mapping(client):
    df = pl.DataFrame(
        [
            {
                "data_element_id": "yyy",
                "period": "202401",
                "organisation_unit_id": "xxx",
                "category_option_combo_id": "psbwp3CQEhs",
                "attribute_option_combo_id": "HllvX50cXC0",
                "value": "100",
            },
            {
                "data_element_id": "yyy",
                "period": "202402",
                "organisation_unit_id": "xxx",
                "category_option_combo_id": "psbwp3CQEhs",
                "attribute_option_combo_id": "HllvX50cXC0",
                "value": "150",
            },
        ]
    )

    org_units_mapping = {"xxx": "O6uvpzGd5pu"}

    data_elements_mapping = {"yyy": "pikOziyCXbM"}

    report = dataframe.import_data_values(
        client,
        data=df,
        org_units_mapping=org_units_mapping,
        data_elements_mapping=data_elements_mapping,
        import_strategy="CREATE_AND_UPDATE",
        dry_run=True,
    )

    for status in ("imported", "updated", "ignored", "deleted"):
        assert report[status] >= 0
