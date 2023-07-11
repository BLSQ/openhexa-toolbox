from typing import List

import polars as pl


def _get_uid_from_level(path: str, level: int):
    """Extract org unit uid from a path string."""
    parents = path.split("/")[1:-1]
    if len(parents) >= level:
        return parents[level - 1]
    else:
        return None


def transform_org_units_metadata(
    organisation_units: List[dict], organisation_unit_levels: List[dict]
) -> pl.DataFrame:
    """Add columns with parent id and name.

    Parameters
    ----------
    organisation_units : list of dict
        Organisation unit metadata as returned by dhis2.meta.organisation_units()
    organisation_unit_levels : list of dict
        Organisation unit levels metadata as returned by
        dhis2.meta.organisation_unit_levels()

    Return
    ------
    organisation_units : polars dataframe
        Transformed organisation unit metadata dataframe
    """
    org_units = pl.DataFrame(organisation_units)

    for lvl in range(1, len(organisation_unit_levels)):
        org_units = org_units.with_columns(
            pl.col("path")
            .apply(lambda path: _get_uid_from_level(path, lvl))
            .alias(f"parent_level_{lvl}_id")
        )

        org_units = org_units.join(
            other=org_units.filter(pl.col("level") == lvl).select(
                [
                    pl.col("id").alias(f"parent_level_{lvl}_id"),
                    pl.col("name").alias(f"parent_level_{lvl}_name"),
                ]
            ),
            on=f"parent_level_{lvl}_id",
            how="left",
        )

    columns_sorted = (
        ["id", "name", "level"]
        + [col for col in org_units.columns if col.startswith("parent_")]
        + ["geometry"]
    )

    org_units = org_units.select(columns_sorted)
    return org_units
