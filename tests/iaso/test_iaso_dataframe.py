import json
from unittest.mock import Mock, patch

import polars as pl
import pytest

from openhexa.toolbox.iaso import IASO
from openhexa.toolbox.iaso.dataframe import (
    extract_submissions,
    get_form_metadata,
    get_organisation_units,
    replace_labels,
)


@pytest.fixture
@patch("openhexa.toolbox.iaso.api_client.ApiClient.authenticate")
def api_client(authenticate: Mock):
    authenticate.return_value = None
    return IASO("https://example.com", "user", "password")


@patch("openhexa.toolbox.iaso.dataframe._get_org_units_gpkg")
@patch("openhexa.toolbox.iaso.dataframe._get_org_units_csv")
def test_get_organisation_units(get_csv_mock: Mock, get_gpkg_mock: Mock, api_client: IASO):
    with open("tests/iaso/responses/dataframe/org_units.csv", "r") as f:
        get_csv_mock.return_value = f.read()
    with open("tests/iaso/responses/dataframe/org_units.gpkg", "rb") as f:
        get_gpkg_mock.return_value = f.read()
    df = get_organisation_units(api_client)
    expected_schema = pl.Schema(
        {
            "id": pl.Int64,
            "name": pl.String,
            "org_unit_type": pl.String,
            "latitude": pl.Float64,
            "longitude": pl.Float64,
            "opening_date": pl.Date,
            "closing_date": pl.Date,
            "created_at": pl.Datetime(time_unit="us", time_zone=None),
            "updated_at": pl.Datetime(time_unit="us", time_zone=None),
            "source": pl.String,
            "validation_status": pl.String,
            "source_ref": pl.String,
            "level_1_ref": pl.String,
            "level_2_ref": pl.String,
            "level_3_ref": pl.String,
            "level_4_ref": pl.String,
            "level_1_name": pl.String,
            "level_2_name": pl.String,
            "level_3_name": pl.String,
            "level_4_name": pl.String,
            "geometry": pl.String,
        }
    )
    assert df.schema == expected_schema


@patch("openhexa.toolbox.iaso.dataframe._get_form_versions")
def test_get_form_metadata(get_form_versions_mock: Mock, api_client: IASO):
    with open("tests/iaso/responses/dataframe/form_versions.json", "r") as f:
        get_form_versions_mock.return_value = json.load(f)
    questions, choices = get_form_metadata(api_client, form_id=503)
    assert len(questions) > 10
    assert len(choices) > 3


@patch("openhexa.toolbox.iaso.dataframe._get_form_versions")
@patch("openhexa.toolbox.iaso.dataframe._get_instances_csv")
def test_extract_submissions(get_instances_mock: Mock, get_form_versions_mock: Mock, api_client: IASO):
    with open("tests/iaso/responses/dataframe/form_versions.json", "r") as f:
        get_form_versions_mock.return_value = json.load(f)
    with open("tests/iaso/responses/dataframe/form_instances.csv", "r") as f:
        get_instances_mock.return_value = f.read()
    df = extract_submissions(api_client, form_id=503)
    assert len(df) > 10


@patch("openhexa.toolbox.iaso.dataframe._get_form_versions")
@patch("openhexa.toolbox.iaso.dataframe._get_instances_csv")
def test_replace_labels(get_instances_mock: Mock, get_form_versions_mock: Mock, api_client: IASO):
    with open("tests/iaso/responses/dataframe/form_versions.json", "r") as f:
        get_form_versions_mock.return_value = json.load(f)
    with open("tests/iaso/responses/dataframe/form_instances.csv", "r") as f:
        get_instances_mock.return_value = f.read()
    df = extract_submissions(api_client, form_id=503)
    questions, choices = get_form_metadata(api_client, form_id=503)
    df = replace_labels(df, questions, choices, language="French")
    assert len(df) > 10
