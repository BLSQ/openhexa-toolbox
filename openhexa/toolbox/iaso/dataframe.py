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


def get_form_metadata(iaso: IASO, form_id: int) -> dict:
    """Get form metadata from IASO.

    Return a dict with form versions as keys and, for each version, two dicts:
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
    dict
        A dict with form versions as keys and, for each version, questions and choices metadata.
    """
    form_versions = _get_form_versions(iaso=iaso, form_id=form_id)
    meta = {}

    for version in form_versions["form_versions"]:
        descriptor = version["descriptor"]
        ver = int(descriptor["version"])
        meta[ver] = {}
        meta[ver]["questions"] = _get_questions(descriptor=descriptor)
        meta[ver]["choices"] = _get_choices(descriptor=descriptor)

    return meta


def _get_instances_csv(iaso: IASO, form_id: int, last_updated: str | None = None) -> str:
    """Extract form instances in CSV format."""
    params = {"form_id": form_id, "csv": True}
    if last_updated is not None:
        params["modificationDateFrom"] = last_updated
    r = iaso.api_client.get(url="api/instances", params=params, stream=True, timeout=30)
    r.raise_for_status()
    return r.content.decode("utf8")


def _process_instance(instance: dict, form_metadata: dict, mapping: dict) -> dict:
    """Create a dict row from a submission instance.

    Also handles casting of values based on ODK question type.

    Parameters
    ----------
    instance: dict
        The submission instance.
    form_metadata: dict
        The form metadata for all versions.
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

    form_version = instance.get("Version du formulaire")
    questions = form_metadata[form_version]["questions"]

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


def _merge_schemas(schemas: list[dict]) -> dict:
    """Merge multiple schemas into one.

    Merge data types of questions with the same name. If data types differ between form versions,
    then the most permissive type is used (e.g. if one version is int and another is str,
    then the merged type is str).

    Parameters
    ----------
    schemas: list[dict]
        A list of schemas to merge.

    Returns
    -------
    dict
        The merged schema.
    """
    merged_schema = {}

    for schema in schemas:
        for column in schema:
            if column not in merged_schema:
                merged_schema[column] = []  # use a list to store all types
            merged_schema[column].append(schema[column])

    final_schema = {}
    for column, dtypes in merged_schema.items():
        dtype = dtypes[0]
        for dtype_ in dtypes[1:]:
            if dtype_ != dtype:
                dtype = pl.String  # use string as the most permissive type
        final_schema[column] = dtype

    return final_schema


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
    form_metadata = get_form_metadata(iaso=iaso, form_id=form_id)
    rows = []

    # Polars schema for default instance properties
    base_schema = {
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
        row = _process_instance(instance=instance, form_metadata=form_metadata, mapping=mapping)
        rows.append(row)

    # build polars schemas for all form versions that are found in the data
    versions_with_submissions = instances["Version du formulaire"].unique().to_list()
    schemas = []
    for form_version, form_meta in form_metadata.items():
        if form_version not in versions_with_submissions:
            continue
        schema = base_schema.copy()
        for name, question in form_meta["questions"].items():
            if question["type"] not in mapping:
                continue
            schema[name] = mapping[question["type"]]
        schemas.append(schema)

    # schemas are merged - if multiple data types are found for the same question across versions,
    # the most permissive type is used (e.g. str)
    schema = _merge_schemas(schemas=schemas)

    return pl.DataFrame(rows, schema=schema)


def replace_labels(submissions: pl.DataFrame, form_metadata: dict, language: str | None = None) -> pl.DataFrame:
    """Replace choice list values with labels.

    Parameters
    ----------
    submissions: pl.DataFrame
        The submissions dataframe.
    form_metadata: dict
        The form metadata for all versions.
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
    # build label mapping for all form versions
    # keep "select one", "select all that apply" and "rank" types separate because they
    # need to be handled differently
    mapping = {}
    for form_version, form_meta in form_metadata.items():
        mapping[form_version] = {"select one": {}, "select all that apply": {}, "rank": {}}
        questions = form_meta["questions"]
        choices = form_meta["choices"]
        for name, question in questions.items():
            qtype = question["type"]
            if qtype not in ["select one", "select all that apply", "rank"]:
                continue
            if question not in submissions.columns:
                continue
            mapping[form_version][qtype][name] = {}
            for choice in choices[question["list_name"]]:
                # in single-language forms, choice labels are strings
                if isinstance(choice["label"], str):
                    mapping[form_version][qtype][name][choice["name"]] = choice["label"]
                # in multi-language forms, choice labels are dicts with language as keys
                else:
                    if language not in choice["label"]:
                        raise ValueError(f"Language {language} not found in choice list labels")
                    mapping[form_version][qtype][name][choice["name"]] = choice["label"].get(language)

    # replace values with labels (select one)
    for form_version, label_mapping in mapping.items():
        form_version = str(form_version)
        for question, choices in label_mapping["select one"].items():
            submissions = submissions.with_columns(
                pl.when(pl.col("form_version") == form_version)
                .then(pl.col(question).map_elements(lambda x: choices.get(x, x), return_dtype=pl.String))
                .otherwise(pl.col(question))
                .alias(question)
            )
        for question, choices in label_mapping["select all that apply"].items():
            submissions = submissions.with_columns(
                pl.when(pl.col("form_version") == form_version)
                .then(
                    pl.col(question).map_elements(lambda x: [choices.get(v, v) for v in x], return_dtype=pl.List(str))
                )
                .otherwise(pl.col(question))
                .alias(question)
            )
        for question, choices in label_mapping["rank"].items():
            submissions = submissions.with_columns(
                pl.when(pl.col("form_version") == form_version)
                .then(
                    pl.col(question).map_elements(lambda x: [choices.get(v, v) for v in x], return_dtype=pl.List(str))
                )
                .otherwise(pl.col(question))
                .alias(question)
            )

    return submissions
