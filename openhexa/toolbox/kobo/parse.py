"""Utility functions to cast survey data responses from the API into structured
dataframes.
"""

from typing import List

import polars as pl
from shapely.geometry import Point

from .api import Field, Survey


def cast_integer(value: str):
    try:
        return int(value)
    except ValueError:
        return None


def cast_decimal(value: str):
    try:
        return float(value)
    except ValueError:
        return None


def cast_select_one(value: str, field: Field, survey: Survey) -> str:
    """Cast value of a field of type `select_one`."""
    assert field.type == "select_one"
    if not value:
        return None
    choices = survey.choices.get(field.list_name)
    for choice in choices:
        if choice.get("name") == value:
            return choice.get("label")[0]


def cast_select_multiple(value: str, field: Field, survey: Survey) -> List[str]:
    """Cast value of a field of type `select_multiple`."""
    assert field.type == "select_multiple"
    if not value:
        return None
    choices = survey.choices.get(field.list_name)
    labels = []
    for choice in choices:
        if choice.get("name") in value.split(" "):
            labels.append(choice.get("label")[0])
    return labels


def cast_geopoint(value: str) -> dict:
    if not value:
        return None
    y, x, _, _ = value.split(" ")
    return Point(x, y).__geo_interface__


def cast_calculate(value: str) -> str:
    if not value:
        return None
    if value == "NaN":
        return None
    try:
        return float(value)
    except ValueError:
        return value


def cast_values(df: pl.DataFrame, survey: Survey) -> pl.DataFrame:
    """Cast field values according to field metadata.

    Parameters
    ----------
    df : dataframe
        A Polars dataframe with KoboToolbox field names as column names.
    survey : Survey
        A KoboToolbox survey.

    Return
    ------
    df : dataframe
        Input dataframe with casted values.
    """
    names = [field.get("name") for field in survey.fields]

    for column in df.columns:
        if column not in names:
            continue

        field = survey.get_field_from_name(column)

        if field.type == "integer":
            df = df.with_columns(pl.col(column).apply(lambda x: cast_integer(x)))

        elif field.type == "decimal":
            df = df.with_columns(pl.col(column).apply(lambda x: cast_decimal(x)))

        elif field.type == "select_one":
            df = df.with_columns(pl.col(column).apply(lambda x: cast_select_one(x, field, survey)))

        elif field.type == "select_multiple":
            df = df.with_columns(pl.col(column).apply(lambda x: cast_select_multiple(x, field, survey)))

        elif field.type == "geopoint":
            df = df.with_columns(pl.col(column).apply(lambda x: cast_geopoint(x)))

        elif field.type == "calculate":
            df = df.with_columns(pl.col(column).apply(lambda x: cast_calculate(x)))

        elif field.type == "date":
            df = df.with_columns(pl.col(column).str.strptime(dtype=pl.Date))

    return df
