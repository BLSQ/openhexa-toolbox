"""A set of opinionated functions to extract DHIS2 metadata & data values into dataframes."""

from datetime import datetime

import polars as pl

from openhexa.toolbox.dhis2 import DHIS2


class MissingParameter(Exception):
    """Exception raised when a required parameter is missing."""


class InvalidParameter(Exception):
    """Exception raised when a parameter is invalid."""


DHIS2_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%3f%z"


def get_datasets(dhis2: DHIS2) -> pl.DataFrame:
    """Extract datasets metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.

    Returns
    -------
    pl.DataFrame
        Dataframe containing datasets metadata with the following columns: id, name,
        organisation_units, data_elements, indicators, period_type.
    """
    meta = dhis2.meta.datasets(fields="id,name,organisationUnits,dataSetElements,indicators,periodType,lastUpdated")

    schema = {
        "id": str,
        "name": str,
        "organisation_units": list[str],
        "data_elements": list[str],
        "indicators": list[str],
        "periodType": str,
    }

    df = pl.DataFrame(data=meta, schema=schema, strict=True).select(
        "id",
        "name",
        "organisation_units",
        "data_elements",
        "indicators",
        pl.col("periodType").alias("period_type"),
    )

    return df.sort(by="name")


def get_data_elements(dhis2: DHIS2) -> pl.DataFrame:
    """Extract data elements metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data elements metadata with the following columns: id, name, value_type.
    """
    meta = dhis2.meta.data_elements(fields="id,name,valueType")
    schema = {"id": str, "name": str, "valueType": str}
    df = pl.DataFrame(meta, schema=schema)
    return df.select("id", "name", pl.col("valueType").alias("value_type"))


def get_data_element_groups(dhis2: DHIS2) -> pl.DataFrame:
    """Extract data element groups metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data element groups metadata with the following columns: id, name,
        data_elements.
    """
    meta = dhis2.meta.data_element_groups(fields="id,name,dataElements")
    schema = {"id": str, "name": str, "dataElements": list[str]}
    df = pl.DataFrame(meta, schema=schema)
    df = df.select("id", "name", pl.col("dataElements").alias("data_elements"))
    return df.sort(by="name")


def get_organisation_unit_levels(dhis2: DHIS2) -> pl.DataFrame:
    """Extract organisation unit levels metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.

    Returns
    -------
    pl.DataFrame
        Dataframe containing organisation unit levels metadata with the following columns: id, name,
        level.
    """
    meta = dhis2.meta.organisation_unit_levels()
    schema = {"id": str, "name": str, "level": int}
    return pl.DataFrame(data=meta, schema=schema)


def get_organisation_units(dhis2: DHIS2, max_level: int | None = None) -> pl.DataFrame:
    """Extract organisation units metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    max_level : int, optional
        Maximum level of organisation units to extract. If None, all levels are extracted.

    Returns
    -------
    pl.DataFrame
        Dataframe containing organisation units metadata with the following columns: id, name,
        level, level_{level}_id, level_{level}_name, geometry.

    Raises
    ------
    InvalidParameter
        If max_level is greater than the maximum level of the organisation units.
    """
    levels = get_organisation_unit_levels(dhis2)

    if max_level:
        if max_level > levels["level"].max():
            raise InvalidParameter(f"max_level cannot be greater than {levels['level'].max()}")

    filter = None
    if max_level is not None:
        filter = f"level:le:{max_level}"
    meta = dhis2.meta.organisation_units(fields="id,name,level,path,geometry", filter=filter)

    schema = {"id": str, "name": str, "level": int, "path": str, "geometry": str}
    df = pl.DataFrame(data=meta, schema=schema)

    for row in levels.iter_rows(named=True):
        lvl = row["level"]
        if max_level:
            if lvl > max_level:
                continue

        df = df.with_columns(
            pl.col("path").str.split("/").list.slice(1).list.get(lvl - 1, null_on_oob=True).alias(f"level_{lvl}_id")
        )

        df = df.join(
            other=df.select("id", pl.col("name").alias(f"level_{lvl}_name")),
            left_on=f"level_{lvl}_id",
            right_on="id",
            how="left",
        )

    df = df.select("id", "name", "level", *[col for col in df.columns if col.startswith("level_")], "geometry")

    return df.sort(by=["level", "name"], descending=False)


def get_organisation_unit_groups(dhis2: DHIS2) -> pl.DataFrame:
    """Extract organisation unit groups metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.

    Returns
    -------
    pl.DataFrame
        Dataframe containing organisation unit groups metadata with the following columns: id, name,
        organisation_units.
    """
    meta = dhis2.meta.organisation_unit_groups(fields="id,name,organisationUnits")
    schema = {"id": str, "name": str, "organisationUnits": list[str]}
    df = pl.DataFrame(meta, schema=schema)
    df = df.select("id", "name", pl.col("organisationUnits").alias("organisation_units"))
    return df.sort(by="name")


def get_category_option_combos(dhis2: DHIS2) -> pl.DataFrame:
    """Extract category option combos metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.

    Returns
    -------
    pl.DataFrame
        Dataframe containing category option combos metadata with the following columns: id, name.
    """
    meta = dhis2.meta.category_option_combos()
    schema = {"id": str, "name": str}
    df = pl.DataFrame(meta, schema=schema)
    return df.sort(by="name")


def _data_values_to_dataframe(values: list[dict]) -> pl.DataFrame:
    """Convert a list of raw DHIS2 data values to a Polars DataFrame.

    Parameters
    ----------
    values : list[dict]
        List of data values.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data values with the following columns: data_element_id, period,
        organisation_unit, category_option_combo, attribute_option_combo, value, created,
        last_updated.
    """
    schema = {
        "dataElement": str,
        "period": str,
        "orgUnit": str,
        "categoryOptionCombo": str,
        "attributeOptionCombo": str,
        "value": str,
        "created": str,
        "lastUpdated": str,
    }

    df = pl.DataFrame(data=values, schema=schema)
    df = df.select(
        pl.col("dataElement").alias("data_element_id"),
        pl.col("period"),
        pl.col("orgUnit").alias("organisation_unit_id"),
        pl.col("categoryOptionCombo").alias("category_option_combo_id"),
        pl.col("attributeOptionCombo").alias("attribute_option_combo_id"),
        pl.col("value"),
        pl.col("created").str.to_datetime(DHIS2_DATE_FORMAT).alias("created"),
        pl.col("lastUpdated").str.to_datetime(DHIS2_DATE_FORMAT).alias("last_updated"),
    )

    return df


def extract_dataset(
    dhis2: DHIS2,
    dataset: str,
    start_date: datetime,
    end_date: datetime,
    org_units: list[str] = None,
    org_unit_groups: list[str] = None,
    include_children: bool = False,
    last_updated: datetime | None = None,
) -> pl.DataFrame:
    """Extract dataset data values.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    dataset : str
        Dataset ID.
    start_date : str
        Start date in the format "YYYY-MM-DD".
    end_date : str
        End date in the format "YYYY-MM-DD".
    org_units : list[str], optional
        Organisation units IDs.
    org_unit_groups : list[str], optional
        Organisation unit groups IDs.
    include_children : bool, optional
        Include children organisation units.
    last_updated : datetime, optional
        Extract only data values that have been updated since this date.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data values with the following columns: data_element_id, period,
        organisation_unit, category_option_combo, attribute_option_combo, value, created,
        last_updated.

    Raises
    ------
    MissingParameter
        If org_units or org_unit_groups is not provided.
    InvalidParameter
        If org_units and org_unit_groups are provided at the same time.
    """
    if org_units is None and org_unit_groups is None:
        raise MissingParameter("org_units or org_unit_groups must be provided")

    if org_units is not None and org_unit_groups is not None:
        raise InvalidParameter("org_units and org_unit_groups cannot be provided at the same time")

    values = dhis2.data_value_sets.get(
        datasets=[dataset],
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        org_units=org_units if org_units else None,
        org_unit_groups=org_unit_groups if org_unit_groups else None,
        children=include_children,
        last_updated=last_updated.isoformat() if last_updated else None,
    )

    df = _data_values_to_dataframe(values)
    return df


def extract_data_element_group(
    dhis2: DHIS2,
    data_element_group: str,
    start_date: datetime,
    end_date: datetime,
    org_units: list[str] = None,
    org_unit_groups: list[str] = None,
    include_children: bool = False,
    last_updated: datetime | None = None,
) -> pl.DataFrame:
    """Extract dataset data values.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    data_element_group : str
        Data element group ID.
    start_date : str
        Start date in the format "YYYY-MM-DD".
    end_date : str
        End date in the format "YYYY-MM-DD".
    org_units : list[str], optional
        Organisation units IDs.
    org_unit_groups : list[str], optional
        Organisation unit groups IDs.
    include_children : bool, optional
        Include children organisation units.
    last_updated : datetime, optional
        Extract only data values that have been updated since this date.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data values with the following columns: data_element_id, period,
        organisation_unit, category_option_combo, attribute_option_combo, value, created,
        last_updated.

    Raises
    ------
    MissingParameter
        If org_units or org_unit_groups is not provided.
    InvalidParameter
        If org_units and org_unit_groups are provided at the same time.
    """
    if org_units is None and org_unit_groups is None:
        raise MissingParameter("org_units or org_unit_groups must be provided")

    if org_units is not None and org_unit_groups is not None:
        raise InvalidParameter("org_units and org_unit_groups cannot be provided at the same time")

    values = dhis2.data_value_sets.get(
        data_element_groups=[data_element_group],
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        org_units=org_units if org_units else None,
        org_unit_groups=org_unit_groups if org_unit_groups else None,
        children=include_children,
        last_updated=last_updated.isoformat() if last_updated else None,
    )

    df = _data_values_to_dataframe(values)
    return df


def extract_data_elements(
    dhis2: DHIS2,
    data_elements: list[str],
    start_date: datetime,
    end_date: datetime,
    org_units: list[str] = None,
    org_unit_groups: list[str] = None,
    include_children: bool = False,
    last_updated: datetime | None = None,
) -> pl.DataFrame:
    """Extract data elements data values.

    Only DHIS2 versions >= 2.39 are supported.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    data_elements : list[str]
        Data elements IDs.
    start_date : str
        Start date in the format "YYYY-MM-DD".
    end_date : str
        End date in the format "YYYY-MM-DD".
    org_units : list[str], optional
        Organisation units IDs.
    org_unit_groups : list[str], optional
        Organisation unit groups IDs.
    include_children : bool, optional
        Include children organisation units.
    last_updated : datetime, optional
        Extract only data values that have been updated since this date.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data values with the following columns: data_element_id, period,
        organisation_unit, category_option_combo, attribute_option_combo, value, created,
        last_updated.

    Raises
    ------
    MissingParameter
        If org_units or org_unit_groups is not provided.
    InvalidParameter
        If org_units and org_unit_groups are provided at the same time.
    """
    if org_units is None and org_unit_groups is None:
        raise MissingParameter("org_units or org_unit_groups must be provided")

    if org_units is not None and org_unit_groups is not None:
        raise InvalidParameter("org_units and org_unit_groups cannot be provided at the same time")

    values = dhis2.data_value_sets.get(
        data_elements=data_elements,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
        org_units=org_units if org_units else None,
        org_unit_groups=org_unit_groups if org_unit_groups else None,
        children=include_children,
        last_updated=last_updated.isoformat() if last_updated else None,
    )

    df = _data_values_to_dataframe(values)
    return df


def extract_analytics(
    dhis2: DHIS2,
    periods: list[str],
    data_elements: list[str] = None,
    data_element_groups: list[str] = None,
    indicators: list[str] = None,
    indicator_groups: list[str] = None,
    org_units: list[str] = None,
    org_unit_groups: list[str] = None,
    org_unit_levels: list[int] = None,
) -> pl.DataFrame:
    """Extract aggregated data values using an Analytics query.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    periods : list[str]
        Periods to extract data values for (ex: ["202101", "202102", "202103"]). Periods must be
        provided in DHIS2 period format.
    data_elements : list[str], optional
        Data elements IDs.
    data_element_groups : list[str], optional
        Data element groups IDs.
    indicators : list[str], optional
        Indicators IDs.
    indicator_groups : list[str], optional
        Indicator groups IDs.
    org_units : list[str], optional
        Organisation units IDs.
    org_unit_groups : list[str], optional
        Organisation unit groups IDs.
    org_unit_levels : list[int], optional
        Organisation unit levels.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data values with the following columns: data_element_id or
        indicator_id, category_option_combo_id, organisation_unit_id, period, value.
    """
    # always include COCs by default, except when extracting indicators
    include_cocs = False if indicators or indicator_groups else True

    values = dhis2.analytics.get(
        data_elements=data_elements,
        data_element_groups=data_element_groups,
        indicators=indicators,
        indicator_groups=indicator_groups,
        periods=periods,
        org_units=org_units,
        org_unit_groups=org_unit_groups,
        org_unit_levels=org_unit_levels,
        include_cocs=include_cocs,
    )

    schema = {"dx": str, "ou": str, "pe": str, "value": str}

    if include_cocs:
        schema["co"] = str

    df = pl.DataFrame(data=values, schema=schema)

    # dx is either data_element_id or indicator_id, depending on the analytics query
    dx = "data_element_id" if data_elements or data_element_groups else "indicator_id"

    if include_cocs:
        return df.select(
            pl.col("dx").alias(dx),
            pl.col("co").alias("category_option_combo_id"),
            pl.col("ou").alias("organisation_unit_id"),
            pl.col("pe").alias("period"),
            pl.col("value"),
        )
    return df.select(
        pl.col("dx").alias(dx),
        pl.col("ou").alias("organisation_unit_id"),
        pl.col("pe").alias("period"),
        pl.col("value"),
    )
