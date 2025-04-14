"""A set of opinionated functions to extract data and metadata from a IASO account into dataframes."""

from __future__ import annotations

import json
from datetime import datetime
from io import BytesIO, StringIO
from typing import Iterable

import fiona
import polars as pl

from openhexa.toolbox.iaso import IASO


def _get_org_units_csv(iaso: IASO) -> str:
    """Extract org units in CSV format."""
    r = iaso.api_client.get(url="api/orgunits", params={"csv": True}, stream=True, timeout=30)
    r.raise_for_status()
    return r.content.decode("utf8")


def _get_org_units_gpkg(iaso: IASO) -> bytes:
    """Extract org units in GPKG format."""
    r = iaso.api_client.get(url="api/orgunits", params={"gpkg": True}, stream=True, timeout=30)
    r.raise_for_status()
    return r.content


def _get_org_units_geometries(iaso: IASO) -> dict[int, str]:
    """Get the org units geometries from IASO.

    Org unit geometries are absent from the CSV export, so we need to fetch them separately.

    Parameters
    ----------
    iaso: IASO
        The IASO client.

    Returns
    -------
    dict[int, str]
        A dict with org unit ids as keys and GeoJSON geometries as values.
    """
    gpkg = _get_org_units_gpkg(iaso)
    features = {}
    layers = fiona.listlayers(BytesIO(gpkg))
    for layer in layers:
        with fiona.open(BytesIO(gpkg), layer=layer) as src:
            for feature in src:
                if not feature.geometry:
                    continue
                ou_id = int(feature["properties"]["id"])
                geom = json.dumps(feature["geometry"].__geo_interface__)
                features[ou_id] = geom
    return features


def get_organisation_units(iaso: IASO) -> pl.DataFrame:
    """Get the organisation units from IASO.

    Parameters
    ----------
    iaso: IASO
        The IASO client.

    Returns
    -------
    pl.DataFrame
        The organisation units dataframe.
    """
    csv = _get_org_units_csv(iaso)
    df = pl.read_csv(StringIO(csv))

    df = df.select(
        pl.col("ID").alias("id"),
        pl.col("Nom").alias("name"),
        pl.col("Type").alias("org_unit_type"),
        pl.col("Latitude").alias("latitude"),
        pl.col("Longitude").alias("longitude"),
        pl.col("Date d'ouverture").str.to_date("%Y-%m-%d").alias("opening_date"),
        pl.col("Date de fermeture").str.to_date("%Y-%m-%d").alias("closing_date"),
        pl.col("Date de création").str.to_datetime("%Y-%m-%d %H:%M").alias("created_at"),
        pl.col("Date de modification").str.to_datetime("%Y-%m-%d %H:%M").alias("updated_at"),
        pl.col("Source").alias("source"),
        pl.col("Validé").alias("validation_status"),
        pl.col("Référence externe").alias("source_ref"),
        *[
            pl.col(f"Ref Ext parent {lvl}").alias(f"level_{lvl}_ref")
            for lvl in range(1, 10)
            if f"Ref Ext parent {lvl}" in df.columns
        ],
        *[pl.col(f"parent {lvl}").alias(f"level_{lvl}_name") for lvl in range(1, 10) if f"parent {lvl}" in df.columns],
    )

    geoms = _get_org_units_geometries(iaso)
    df = df.with_columns(
        pl.col("id").map_elements(lambda x: geoms.get(x, None), return_dtype=pl.String).alias("geometry")
    )

    return df


def _iter_children(children: list[dict]) -> Iterable[dict]:
    for child in children:
        if child.get("type"):
            yield child
            if child.get("children"):
                yield from _iter_children(child["children"])


def _get_form_versions(iaso: IASO, form_id: int) -> dict:
    """Extract form versions metadata from IASO."""
    r = iaso.api_client.get(url="api/formversions", params={"form_id": form_id, "fields": "descriptor"}, timeout=5)
    r.raise_for_status()
    return r.json()


def _get_questions(descriptor: dict) -> dict:
    """Extract questions metadata from form descriptor.

    Parameters
    ----------
    descriptor: dict
        The form descriptor.

    Returns
    -------
    dict
        A dict with question names as keys and metadata as values.
    """
    questions = {}
    for child in _iter_children(descriptor["children"]):
        questions[child["name"]] = {
            "name": child["name"],
            "type": child["type"],
            "label": child.get("label"),
            "list_name": child.get("list_name"),
            "calculate": child["bind"].get("calculate") if child["type"] == "calculate" else None,
        }
    return questions


def _get_choices(descriptor: dict) -> dict:
    """Extract choices metadata from form descriptor.

    Parameters
    ----------
    descriptor: dict
        The form descriptor.

    Returns
    -------
    dict
        A dict with choice list names as keys and a list of choices as values.
    """
    all_choices = {}
    for child in _iter_children(descriptor["children"]):
        if child["type"].startswith("select"):
            question_choices = child["children"]
            list_name = child["list_name"]
            if list_name not in all_choices:
                all_choices[list_name] = []
            for choice in question_choices:
                if choice not in all_choices[list_name]:
                    all_choices[list_name].append(choice)
    return all_choices


def get_form_metadata(iaso: IASO, form_id: int) -> tuple[dict, dict]:
    """Get form metadata from IASO.

    Return two JSON-like dicts:
      * questions: a dict with metadata for each question, with question name as key
        includes name, type, label, list_name and calculate expression
      * choices: a dict with metadata for each choice list, with list_name as key
        includes choice name and label for each choice

    Parameters
    ----------
    iaso: IASO
        The IASO client.
    form_id: int
        The form id.

    Returns
    -------
    tuple[dict, dict]
        The questions and choices metadata.
    """
    form_versions = _get_form_versions(iaso=iaso, form_id=form_id)
    descriptor = form_versions["form_versions"][0]["descriptor"]

    questions = _get_questions(descriptor=descriptor)
    choices = _get_choices(descriptor=descriptor)

    return questions, choices


def _get_instances_csv(iaso: IASO, form_id: int, last_updated: str | None = None) -> str:
    """Extract form instances in CSV format."""
    params = {"form_id": form_id, "csv": True}
    if last_updated is not None:
        params["modificationDateFrom"] = last_updated
    r = iaso.api_client.get(url="api/instances", params=params, stream=True, timeout=30)
    r.raise_for_status()
    return r.content.decode("utf8")


def _process_instance(instance: dict, questions: dict, mapping: dict) -> dict:
    """Create a dict row from a submission instance.

    Also handles casting of values based on ODK question type.

    Parameters
    ----------
    instance: dict
        The submission instance.
    questions: dict
        Form questions metadata (with question names as keys).
    mapping: dict
        Mapping between ODK question types and Polars data types.

    Returns
    -------
    dict
        The row as a dict (with column names as key).
    """
    row = {
        "id": instance.get("ID du formulaire"),
        "form_version": instance.get("Version du formulaire"),
        "created_at": datetime.strptime(
            instance.get("Date de création"),
            "%Y-%m-%d %H:%M:%S",
        ),
        "updated_at": datetime.strptime(
            instance.get("Date de modification"),
            "%Y-%m-%d %H:%M:%S",
        ),
        "org_unit_id": instance.get("Org unit id"),
        "org_unit_name": instance.get("Org unit"),
        "org_unit_ref": instance.get("Référence externe"),
        "latitude": instance.get("Latitude"),
        "longitude": instance.get("Longitude"),
    }

    for name, question in questions.items():
        type = question["type"]
        if type not in mapping:
            continue

        src_value = instance.get(name)
        if src_value == "" or src_value is None:
            dst_value = None

        elif type == "integer":
            dst_value = int(src_value)
        elif type == "decimal":
            dst_value = float(src_value)
        elif type == "select all that apply":
            dst_value = [str(v) for v in src_value.split(" ")]
        elif type == "date":
            dst_value = datetime.strptime(src_value, "%Y-%m-%d").date()
        elif type == "time":
            dst_value = datetime.strptime(src_value, "%H:%M:%S.%f%z").time()
        elif type == "datetime":
            dst_value = datetime.strptime(src_value, "%Y-%m-%dT%H:%M:%S%z")
        elif type == "geopoint":
            coords = [float(v) for v in src_value.split(" ")]
            feature = {"type": "Point", "coordinates": [coords[1], coords[0]]}
            dst_value = json.dumps(feature)
        elif type == "rank":
            dst_value = [str(v) for v in src_value.split(" ")]
        elif type == "repeat":
            dst_value = json.dumps(src_value)
        else:
            dst_value = str(src_value)

        row[name] = dst_value

    return row


def extract_submissions(iaso: IASO, form_id: int, last_updated: str | None = None) -> pl.DataFrame:
    """Extract submissions instances for a IASO form.

    Parameters
    ----------
    iaso: IASO
        The IASO client.
    form_id: int
        The form id.
    last_updated: str, optional
        The last updated date to fetch in ISO format.

    Returns
    -------
    pl.DataFrame
        The submissions dataframe with one row per submission.
    """
    csv = _get_instances_csv(iaso=iaso, form_id=form_id, last_updated=last_updated)
    questions, _ = get_form_metadata(iaso=iaso, form_id=form_id)
    rows = []

    # Polars schema for default instance properties
    schema = {
        "id": pl.String,
        "form_version": pl.String,
        "created_at": pl.Datetime(time_unit="us", time_zone=None),
        "updated_at": pl.Datetime(time_unit="us", time_zone=None),
        "org_unit_id": pl.Int64,
        "org_unit_name": pl.String,
        "org_unit_ref": pl.String,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
    }

    # mapping between ODK question types and target Polars data types
    mapping = {
        "text": pl.String,
        "integer": pl.Int64,
        "decimal": pl.Float64,
        "select one": pl.String,
        "select all that apply": pl.List(pl.String),
        "date": pl.Date,
        "time": pl.Time,
        "datetime": pl.Datetime,
        "geopoint": pl.String,
        "photo": pl.String,
        "file": pl.String,
        "audio": pl.String,
        "video": pl.String,
        "barcode": pl.String,
        "acknowledge": pl.String,
        "calculate": pl.String,
        "repeat": pl.String,
        "rank": pl.List(pl.String),
        "range": pl.String,
    }

    instances = pl.read_csv(StringIO(csv))
    for instance in instances.iter_rows(named=True):
        row = _process_instance(instance=instance, questions=questions, mapping=mapping)
        rows.append(row)

    # expand Polars schema with question columns
    for name, question in questions.items():
        if question["type"] not in mapping:
            continue
        schema[name] = mapping[question["type"]]

    return pl.DataFrame(rows, schema=schema)


def replace_labels(
    submissions: pl.DataFrame, questions: dict, choices: dict, language: str | None = None
) -> pl.DataFrame:
    """Replace choice list values with labels.

    Parameters
    ----------
    submissions: pl.DataFrame
        The submissions dataframe.
    questions: dict
        The questions metadata.
    choices: dict
        The choices metadata.
    language: str, optional
        The language code for the labels. Must be specified for multi-language forms.

    Returns
    -------
    pl.DataFrame
        The submissions dataframe with choice values replaced by labels.
    """
    for name, question in questions.items():
        type = question["type"]
        if type not in ["select one", "select all that apply", "rank"]:
            continue

        mapping = {}
        for choice in choices[question["list_name"]]:
            # in single-language forms, choice labels are strings
            if isinstance(choice["label"], str):
                mapping[choice["name"]] = choice["label"]
            # in multi-language forms, choice labels are dicts with language as keys
            else:
                if language not in choice["label"]:
                    raise ValueError(f"Language {language} not found in choice list labels")
                mapping[choice["name"]] = choice["label"].get(language)

        if type == "select one":
            submissions = submissions.with_columns(pl.col(name).replace(mapping))

        if type in ["select all that apply", "rank"]:
            submissions = submissions.with_columns(
                pl.col(name).map_elements(lambda x: [mapping.get(v, v) for v in x], return_dtype=pl.List(str))
            )

    return submissions
