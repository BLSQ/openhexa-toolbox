"""Utility functions to parse survey data responses from the API into structured
dataframes.
"""

from typing import List

import polars as pl
from shapely.geometry import Point

from .api import Field, Survey


def parse_integer(value: str):
    try:
        return int(value)
    except ValueError:
        return None


def parse_decimal(value: str):
    try:
        return float(value)
    except ValueError:
        return None


def parse_select_one(value: str, field: Field, survey: Survey) -> str:
    """Parse value of a field of type `select_one`."""
    assert field.type == "select_one"
    if not value:
        return None
    choices = survey.choices.get(field.list_name)
    for choice in choices:
        if choice.get("name") == value:
            return choice.get("label")[0]


def parse_select_multiple(value: str, field: Field, survey: Survey) -> List[str]:
    """Parse value of a field of type `select_multiple`."""
    assert field.type == "select_multiple"
    if not value:
        return None
    choices = survey.choices.get(field.list_name)
    labels = []
    for choice in choices:
        if choice.get("name") in value.split(" "):
            labels.append(choice.get("label")[0])
    return labels


def parse_geopoint(value: str) -> Point:
    if not value:
        return None
    y, x, _, _ = value.split(" ")
    return Point(x, y)


def parse_calculate(value: str) -> str:
    if not value:
        return None
    if value == "NaN":
        return None
    try:
        return float(value)
    except ValueError:
        return value


def parse_values(df: pl.DataFrame, survey: Survey) -> pl.DataFrame:
    """Try to parse survey fields in a dataframe."""
    xpaths = [field.xpath for field in survey.fields]

    for column in df.columns:
        if column not in xpaths:
            continue

        field = survey.get_field_from_xpath(column)

        if field.type == "integer":
            df = df.with_columns(pl.col(column).apply(lambda x: parse_integer(x)))

        elif field.type == "decimal":
            df = df.with_columns(pl.col(column).apply(lambda x: parse_decimal(x)))

        elif field.type == "select_one":
            df = df.with_columns(pl.col(column).apply(lambda x: parse_select_one(x, field, survey)))

        elif field.type == "select_multiple":
            df = df.with_columns(pl.col(column).apply(lambda x: parse_select_multiple(x, field, survey)))

        elif field.type == "geopoint":
            df = df.with_columns(pl.col(column).apply(lambda x: parse_geopoint(x)))

        elif field.type == "calculate":
            df = df.with_columns(pl.col(column).apply(lambda x: parse_calculate(x)))

    return df
