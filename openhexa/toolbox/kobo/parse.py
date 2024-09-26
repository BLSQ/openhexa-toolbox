"""Utility functions to cast survey data responses from the API into structured
dataframes.
"""

import logging
from typing import List

import polars as pl
from shapely.geometry import Point

from .api import Field, Survey

logging.basicConfig(level=logging.INFO)


def cast_integer(value: str):
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def cast_decimal(value: str):
    if value is None:
        return None
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
            if choice.get("label"):
                return choice["label"][0]
            else:
                return None


def cast_select_multiple(value: str, field: Field, survey: Survey) -> List[str]:
    """Cast value of a field of type `select_multiple`."""
    assert field.type == "select_multiple"
    if not value:
        return None
    choices = survey.choices.get(field.list_name)
    labels = []
    for choice in choices:
        if choice.get("name") in value.split(" "):
            if choice.get("label"):
                labels.append(choice.get("label")[0])
            else:
                labels.append(None)
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
        return str(value)
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
    names = [field.name for field in survey.fields]

    for column in df.columns:
        if column not in names:
            continue

        logging.debug(f"casting {column} values")

        field = survey.get_field_from_name(column)

        if field.type == "integer":
            df = df.with_columns(
                pl.col(column).map_elements(lambda x: cast_integer(x), return_dtype=pl.Int64, skip_nulls=False)
            )

        elif field.type == "decimal":
            df = df.with_columns(
                pl.col(column).map_elements(lambda x: cast_decimal(x), return_dtype=pl.Float64, skip_nulls=False)
            )

        elif field.type == "select_one":
            df = df.with_columns(
                pl.col(column).map_elements(
                    lambda x: cast_select_one(x, field, survey), return_dtype=pl.Utf8, skip_nulls=False
                )
            )

        elif field.type == "select_multiple":
            df = df.with_columns(
                pl.col(column).map_elements(
                    lambda x: cast_select_multiple(x, field, survey),
                    return_dtype=pl.List(pl.Utf8),
                    skip_nulls=False,
                )
            )

        elif field.type == "geopoint":
            df = df.with_columns(
                pl.col(column).map_elements(lambda x: cast_geopoint(x), return_dtype=pl.Struct, skip_nulls=False)
            )

        elif field.type == "calculate":
            df = df.with_columns(
                pl.col(column).map_elements(lambda x: cast_calculate(x), return_dtype=pl.String, skip_nulls=False)
            )

        elif field.type == "date":
            df = df.with_columns(pl.col(column).str.strptime(dtype=pl.Date))

    return df
