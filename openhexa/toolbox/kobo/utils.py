import os

import geopandas as gpd
import polars as pl
from shapely.geometry import Point

from .api import Api, Field, Survey


def field_from_name(name: str, survey: Survey) -> Field:
    """Get field in survey from its name."""
    for field in survey.fields:
        if field.name == name:
            return field
    return None


def lists_to_string(df: pl.DataFrame) -> pl.DataFrame:
    """Convert lists in a dataframe into comma-separated strings."""
    for column in df.columns:
        if df[column].dtype == pl.List(pl.Utf8):
            df = df.with_columns(pl.col(column).list.join(", ").alias(column))
    return df


def _download(url: str, dst_dir: str, api: Api):
    with api.session.get(url, stream=True) as r:
        fname = r.headers["Content-Disposition"].split("filename=")[-1]
        fpath = os.path.join(dst_dir, fname)
        if os.path.exists(fpath):
            return
        os.makedirs(dst_dir, exist_ok=True)
        with open(fpath, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024**2):
                if chunk:
                    f.write(chunk)


def download_attachments(df: pl.DataFrame, dst_dir: str, api: Api):
    """Download all survey attachments referenced in a dataframe."""
    for row in df.iter_rows(named=True):
        attachments = row.get("_attachments")
        for attachment in attachments:
            url = attachment.get("download_url")
            if url:
                _download(url, dst_dir, api)


def rename_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Use field names instead of xpaths as columns.

    E.g. "group1/DATE" becomes "DATE".
    """
    mapping = {}
    for column in df.columns:
        if "/" in column:
            mapping[column] = column.split("/")[-1]
    df = df.rename(mapping)
    return df


# def get_formatted_survey(survey_uid: str, api: Api) -> pl.DataFrame:
#     """Get formatted dataframe of a given survey."""
#     survey = api.get_survey(survey_uid)
#     data = api.get_data(survey)
#     df = pl.DataFrame(data)
#     df = parse_values(df, survey)
#     mapping = {}
#     for column in df.columns:
#         if "/" in column:
#             mapping[column] = column.split("/")[-1]
#     df = df.rename(mapping)
#     return df


def to_geodataframe(df: pl.DataFrame) -> gpd.GeoDataFrame:
    """Get geodataframe from formatted survey dataframe."""
    geodf = gpd.GeoDataFrame(
        data=df.to_pandas(),
        crs="EPSG:4326",
        geometry=[Point(coords[1], coords[0]) if all(coords) else None for coords in df["_geolocation"]],
    )
    geodf = geodf.drop(columns=["_geolocation"])
    return geodf


def get_fields_mapping(survey: Survey) -> pl.DataFrame:
    """Get a mapping of fields names, types and labels as a dataframe."""
    mapping = []
    for field in survey.fields:
        f = survey.get_field(field["uid"])
        if f.label and f.name and f.type:
            mapping.append({"name": f.name, "type": f.type, "label": f.label})
    return pl.DataFrame(mapping)
