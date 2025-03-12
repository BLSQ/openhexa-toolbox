from pathlib import Path

import polars as pl
import pytest
import responses

from openhexa.toolbox.iaso import IASO
from openhexa.toolbox.iaso.dataframe import (
    extract_submissions,
    get_form_metadata,
    get_organisation_units,
    replace_labels,
)


@pytest.fixture
@responses.activate
def client() -> IASO:
    mocked_token = {
        "access": "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzY5MDEwNCwiaWF0IjoxNzE3NjkwMTA0fQ.WsmnKvyKFR2eWNL4wD4yrnd6F9CDBV2dCaMx9lE6V84",  # noqa: E501
        "refresh": "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFtZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzY5MDEwNCwiaWF0IjoxNzE3NjkwMTA0fQ.WsmnKvyKFR2eWNL4wD4yrnd6F9CDBV2dCaMx9lE6V84",  # noqa: E501
    }

    responses.add(
        responses.POST,
        "https://iaso-staging.bluesquare.org/api/token/",
        json=mocked_token,
        status=200,
    )
    return IASO("https://iaso-staging.bluesquare.org", "test", "test")


@responses.activate
def test_get_organisation_units(client: IASO):
    responses._add_from_file(Path("tests/iaso/responses/dataframe/get_organisation_units.yaml"))
    df = get_organisation_units(client)
    assert len(df) > 10
    expected_schema = pl.Schema(
        {
            "id": pl.Int64,
            "name": pl.String,
            "short_name": pl.String,
            "level": pl.UInt32,
            "level_1_id": pl.Int64,
            "level_1_name": pl.String,
            "level_2_id": pl.Int64,
            "level_2_name": pl.String,
            "level_3_id": pl.Int64,
            "level_3_name": pl.String,
            "level_4_id": pl.Int64,
            "level_4_name": pl.String,
            "source": pl.String,
            "source_id": pl.Int64,
            "source_ref": pl.String,
            "org_unit_type_id": pl.Int64,
            "org_unit_type_name": pl.String,
            "created_at": pl.Datetime(time_unit="us", time_zone=None),
            "updated_at": pl.Datetime(time_unit="us", time_zone=None),
            "validation_status": pl.String,
            "opening_date": pl.Datetime(time_unit="us", time_zone=None),
            "closed_date": pl.Datetime(time_unit="us", time_zone=None),
            "geometry": pl.String,
        }
    )
    assert df.schema == expected_schema


@responses.activate
def test_get_form_metadata(client: IASO) -> None:
    responses._add_from_file(Path("tests/iaso/responses/dataframe/get_form_metadata.yaml"))
    questions, choices = get_form_metadata(client, form_id=505)
    assert len(questions) > 10
    assert len(choices) > 5
    assert questions["longueur"]["type"] == "decimal"
    assert questions["longueur"]["label"]["English"] == "Length (cm)"
    assert choices["yes_no"][0]["name"] == "yes"
    assert choices["yes_no"][1]["label"]["English"] == "No"


@responses.activate
def test_extract_submissions(client: IASO) -> None:
    responses._add_from_file(Path("tests/iaso/responses/dataframe/extract_submissions.yaml"))
    df = extract_submissions(client, form_id=505)
    assert len(df) > 10


@responses.activate
def test_replace_labels(client: IASO) -> None:
    responses._add_from_file(Path("tests/iaso/responses/dataframe/extract_submissions.yaml"))
    responses._add_from_file(Path("tests/iaso/responses/dataframe/replace_labels.yaml"))
    df = extract_submissions(client, form_id=505)
    questions, choices = get_form_metadata(client, form_id=505)
    df = replace_labels(df, questions, choices, language="French")
    assert len(df) > 10
