"""A set of opinionated functions to extract data and metadata from a IASO account into dataframes."""

import json
from datetime import datetime, timezone
from typing import Iterable

import polars as pl

from openhexa.toolbox.iaso import IASO


def _get_parents(org_unit_id: int, mapping: dict[int, int]) -> list[int]:
    """Get the list of parent org units.

    Parameters
    ----------
    org_unit_id: int
        The org unit id.
    mapping: dict[int, int]
        A mapping of org unit ids to their parent ids.

    Returns
    -------
    list[int]
        The list of parent org unit ids.
    """
    if not mapping.get(org_unit_id):
        return []
    parents = [mapping.get(org_unit_id)]
    while mapping.get(parents[-1]):
        parents.append(mapping[parents[-1]])
    return reversed(parents)


def _add_hierarchy_levels(org_units: pl.DataFrame) -> pl.DataFrame:
    """Add id ane name columns for each level of the hierarchy.

    Parameters
    ----------
    org_units: pl.DataFrame
        The org units dataframe.

    Returns
    -------
    pl.DataFrame
        The org units dataframe with the hierarchy levels.
    """
    mapping = {row["id"]: row["parent_id"] for row in org_units.iter_rows(named=True)}

    # store list of parent ids in a column
    df = org_units.with_columns(
        pl.col("id").map_elements(lambda x: _get_parents(x, mapping), return_dtype=pl.List(int)).alias("parents")
    )

    # number of hierarchy levels defined as max number of parents
    levels = df["parents"].list.len().max()

    # add id and name columns for each level
    for lvl in range(1, levels + 1):
        df = df.with_columns(
            [
                pl.col("parents").list.get(lvl - 1, null_on_oob=True).alias(f"level_{lvl}_id"),
            ]
        )
        df = df.join(
            other=df.select(pl.col("id").alias(f"level_{lvl}_id"), pl.col("name").alias(f"level_{lvl}_name")),
            on=f"level_{lvl}_id",
            how="left",
        )

    # add a column with the org unit level in the hierarchy
    df = df.with_columns(pl.col("parents").list.len().alias("level") + 1)

    return df


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
    r = iaso.api_client.get("/api/orgunits", params={"withShapes": True})
    r.raise_for_status()

    rows = []
    for ou in r.json()["orgUnits"]:
        row = {
            "id": ou["id"],
            "name": ou["name"],
            "short_name": ou.get("short_name"),
            "source": ou.get("source"),
            "source_id": ou.get("source_id"),
            "source_ref": ou.get("source_ref"),
            "parent_id": ou.get("parent_id"),
            "org_unit_type_id": ou.get("org_unit_type_id"),
            "org_unit_type_name": ou.get("org_unit_type_name"),
            "created_at": datetime.fromtimestamp(ou["created_at"], tz=timezone.utc),
            "updated_at": datetime.fromtimestamp(ou["updated_at"], tz=timezone.utc),
            "validation_status": ou.get("validation_status"),
        }

        if ou.get("opening_date"):
            row["opening_date"] = datetime.strptime(ou["opening_date", "%d/%m/%Y"])
        else:
            row["opening_date"] = None

        if ou.get("closed_date"):
            row["closed_date"] = datetime.strptime(ou["closed_date"], "%d/%m/%Y")
        else:
            row["closed_date"] = None

        if ou.get("geo_json"):
            row["geometry"] = json.dumps(ou["geo_json"]["features"][0]["geometry"])
        else:
            row["geometry"] = None

        rows.append(row)

    schema = {
        "id": int,
        "name": str,
        "short_name": str,
        "source": str,
        "source_id": int,
        "source_ref": str,
        "parent_id": int,
        "org_unit_type_id": int,
        "org_unit_type_name": str,
        "created_at": datetime,
        "updated_at": datetime,
        "validation_status": str,
        "opening_date": datetime,
        "closed_date": datetime,
        "geometry": str,
    }

    df = pl.DataFrame(rows, schema=schema)
    df = _add_hierarchy_levels(df)
    df = df.select(
        [
            "id",
            "name",
            "short_name",
            "level",
            *[col for col in df.columns if col.startswith("level_")],
            "source",
            "source_id",
            "source_ref",
            "org_unit_type_id",
            "org_unit_type_name",
            "created_at",
            "updated_at",
            "validation_status",
            "opening_date",
            "closed_date",
            "geometry",
        ]
    )

    return df


def _iter_children(children: list[dict]) -> Iterable[dict]:
    for child in children:
        if child.get("type"):
            yield child
            if child.get("children"):
                yield from _iter_children(child["children"])


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
    params = {"form_id": form_id, "fields": "descriptor"}
    r = iaso.api_client.get("/api/formversions", params=params, timeout=3)
    r.raise_for_status()

    descriptor = r.json()["form_versions"][0]["descriptor"]

    questions = {}
    for child in _iter_children(descriptor["children"]):
        questions[child["name"]] = {
            "name": child["name"],
            "type": child["type"],
            "label": child.get("label"),
            "list_name": child.get("list_name"),
            "calculate": child["bind"].get("calculate") if child["type"] == "calculate" else None,
        }

    choices = descriptor["choices"]

    return questions, choices


def _get_instances(iaso: IASO, form_id: int, last_updated: str | None = None) -> list[dict]:
    """Get submissions instances for a IASO form.

    Parameters
    ----------
    iaso: IASO
        The IASO client.
    form_id: int
        The form id.
    last_updated: str, optional
        The last updated date in ISO format.

    Returns
    -------
    list[dict]
        List of submissions as dicts.
    """
    instances = []
    has_next = True

    params = {"form_id": form_id, "page": 1, "limit": 10}

    if last_updated:
        params["modificationDateFrom"] = last_updated

    while has_next:
        r = iaso.api_client.get("/api/instances", params=params, timeout=5)
        r.raise_for_status()
        instances.extend(r.json()["instances"])
        has_next = r.json()["has_next"]
        params["page"] += 1

    return instances


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
        "uuid": instance.get("uuid"),
        "id": instance.get("id"),
        "form_id": instance.get("form_id"),
        "created_at": datetime.fromtimestamp(instance["created_at"], tz=timezone.utc),
        "updated_at": datetime.fromtimestamp(instance["updated_at"], tz=timezone.utc),
        "org_unit_id": instance["org_unit"].get("id") if "org_unit" in instance else None,
        "org_unit_name": instance["org_unit"].get("name") if "org_unit" in instance else None,
        "latitude": instance.get("latitude"),
        "longitude": instance.get("longitude"),
    }

    for name, question in questions.items():
        type = question["type"]
        if type not in mapping:
            continue

        src_value = instance["file_content"].get(name)
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
    instances = _get_instances(iaso=iaso, form_id=form_id, last_updated=last_updated)
    questions, _ = get_form_metadata(iaso=iaso, form_id=form_id)
    rows = []

    # Polars schema for default instance properties
    schema = {
        "uuid": str,
        "id": int,
        "form_id": int,
        "created_at": datetime,
        "updated_at": datetime,
        "org_unit_id": int,
        "org_unit_name": str,
        "latitude": float,
        "longitude": float,
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

    for instance in instances:
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
