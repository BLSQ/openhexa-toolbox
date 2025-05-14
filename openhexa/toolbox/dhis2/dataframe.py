"""A set of opinionated functions to extract DHIS2 metadata & data values into dataframes."""

import logging
from datetime import datetime
from typing import Literal

import polars as pl

from openhexa.toolbox.dhis2 import DHIS2

logger = logging.getLogger(__name__)


class MissingParameterError(Exception):
    """Exception raised when a required parameter is missing."""


class MissingColumnError(Exception):
    """Exception raised when a required column is missing."""


class InvalidDataTypeError(Exception):
    """Exception raised when a column has an invalid data type."""


class InvalidParameterError(Exception):
    """Exception raised when a parameter is invalid."""


DHIS2_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%3f%z"


def get_datasets(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract datasets metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing datasets metadata with the following columns: id, name,
        organisation_units, data_elements, indicators, period_type.
    """
    meta = dhis2.meta.datasets(
        fields="id,name,organisationUnits,dataSetElements,indicators,periodType,lastUpdated",
        filters=filters,
    )

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


def get_data_elements(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract data elements metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data elements metadata with the following columns: id, name, value_type.
    """
    meta = dhis2.meta.data_elements(fields="id,name,valueType", filters=filters)
    schema = {"id": str, "name": str, "valueType": str}
    df = pl.DataFrame(meta, schema=schema)
    return df.select("id", "name", pl.col("valueType").alias("value_type"))


def get_data_element_groups(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract data element groups metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing data element groups metadata with the following columns: id, name,
        data_elements.
    """
    meta = dhis2.meta.data_element_groups(fields="id,name,dataElements", filters=filters)
    schema = {"id": str, "name": str, "dataElements": list[str]}
    df = pl.DataFrame(meta, schema=schema)
    df = df.select("id", "name", pl.col("dataElements").alias("data_elements"))
    return df.sort(by="name")


def get_indicators(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract indicators metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing indicators metadata with the following columns: id, name, value_type.
    """
    meta = dhis2.meta.indicators(fields="id,name,numerator,denominator", filters=filters)
    schema = {"id": str, "name": str, "numerator": str, "denominator": str}
    df = pl.DataFrame(meta, schema=schema)
    return df.select("id", "name", "numerator", "denominator")


def get_indicator_groups(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract indicator groups metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing indicator groups metadata with the following columns: id, name,
        data_elements.
    """
    meta = dhis2.meta.indicator_groups(fields="id,name,indicators", filters=filters)
    schema = {"id": str, "name": str, "indicators": list[str]}
    df = pl.DataFrame(meta, schema=schema)
    df = df.select("id", "name", pl.col("indicators").alias("indicators"))
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


def get_organisation_units(
    dhis2: DHIS2, max_level: int | None = None, filters: list[str] | None = None
) -> pl.DataFrame:
    """Extract organisation units metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    max_level : int, optional
        Maximum level of organisation units to extract. If None, all levels are extracted.
    filters : list[str], optional
        DHIS2 query filter expressions.

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
            msg = f"max_level cannot be greater than {levels['level'].max()}"
            logger.error(msg)
            raise InvalidParameterError(msg)
        level_filter = f"level:le:{max_level}"
        if filters:
            filters = [*filters, max_level]
        else:
            filters = [level_filter]

    meta = dhis2.meta.organisation_units(fields="id,name,level,path,openingDate,closedDate,geometry", filters=filters)

    schema = {
        "id": str,
        "name": str,
        "level": int,
        "path": str,
        "openingDate": str,
        "closedDate": str,
        "geometry": str,
    }
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

    df = df.select(
        "id",
        "name",
        "level",
        pl.col("openingDate").str.to_datetime("%Y-%m-%dT%H:%M:%S.%3f").alias("opening_date"),
        pl.col("closedDate").str.to_datetime("%Y-%m-%dT%H:%M:%S.%3f").alias("closed_date"),
        *[col for col in df.columns if col.startswith("level_")],
        "geometry",
    )

    return df.sort(by=["level", "name"], descending=False)


def get_organisation_unit_groups(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract organisation unit groups metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing organisation unit groups metadata with the following columns: id, name,
        organisation_units.
    """
    meta = dhis2.meta.organisation_unit_groups(fields="id,name,organisationUnits", filters=filters)
    schema = {"id": str, "name": str, "organisationUnits": list[str]}
    df = pl.DataFrame(meta, schema=schema)
    df = df.select("id", "name", pl.col("organisationUnits").alias("organisation_units"))
    return df.sort(by="name")


def get_category_option_combos(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract category option combos metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : str, optional
        DHIS2 query filter expression.

    Returns
    -------
    pl.DataFrame
        Dataframe containing category option combos metadata with the following columns: id, name.
    """
    meta = dhis2.meta.category_option_combos(filters=filters)
    schema = {"id": str, "name": str}
    df = pl.DataFrame(meta, schema=schema)
    return df.sort(by="name")


def get_attributes(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract attributes metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing attributes metadata with the following columns: id, name.
    """
    r = dhis2.api.get("attributes", params={"paging": False, "fields": "id,name", "filter": filters})
    rows = r["attributes"]
    schema = {"id": str, "name": str}
    df = pl.DataFrame(rows, schema=schema)
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
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    periods: list[str] | None = None,
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
    start_date : str, optional
        Start date in the format "YYYY-MM-DD".
        Use either start_date and end_date or periods.
    end_date : str, optional
        End date in the format "YYYY-MM-DD".
        Use either start_date and end_date or periods.
    periods : list[str], optional
        Periods to extract data values for (ex: ["202101", "202102", "202103"]). Periods must be
        provided in DHIS2 period format. Use either start_date and end_date or periods.
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
        msg = "org_units or org_unit_groups must be provided"
        logger.error(msg)
        raise MissingParameterError(msg)

    if org_units is not None and org_unit_groups is not None:
        msg = "org_units and org_unit_groups cannot be provided at the same time"
        logger.error(msg)
        raise InvalidParameterError(msg)

    if not (start_date and end_date) and not periods:
        msg = "Either start_date and end_date or periods must be provided"
        logger.error(msg)
        raise MissingParameterError(msg)

    if (start_date or end_date) and periods:
        msg = "Either start_date and end_date or periods must be provided, not both"
        logger.error(msg)
        raise InvalidParameterError(msg)

    if start_date:
        start_date = start_date.strftime("%Y-%m-%d")
    if end_date:
        end_date = end_date.strftime("%Y-%m-%d")

    values = dhis2.data_value_sets.get(
        datasets=[dataset],
        periods=periods if periods else None,
        start_date=start_date if start_date else None,
        end_date=end_date if end_date else None,
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
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    periods: list[str] | None = None,
    org_units: list[str] = None,
    org_unit_groups: list[str] = None,
    include_children: bool = False,
    last_updated: datetime | None = None,
) -> pl.DataFrame:
    """Extract data element group data values.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    data_element_group : str
        Data element group ID.
    start_date : str, optional
        Start date in the format "YYYY-MM-DD".
        Use either start_date and end_date or periods.
    end_date : str, optional
        End date in the format "YYYY-MM-DD".
        Use either start_date and end_date or periods.
    periods : list[str], optional
        Periods to extract data values for (ex: ["202101", "202102", "202103"]). Periods must be
        provided in DHIS2 period format. Use either start_date and end_date or periods.
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
        msg = "org_units or org_unit_groups must be provided"
        logger.error(msg)
        raise MissingParameterError(msg)

    if org_units is not None and org_unit_groups is not None:
        msg = "org_units and org_unit_groups cannot be provided at the same time"
        logger.error(msg)
        raise InvalidParameterError(msg)

    if not (start_date and end_date) and not periods:
        msg = "Either start_date and end_date or periods must be provided"
        logger.error(msg)
        raise MissingParameterError(msg)

    if (start_date or end_date) and periods:
        msg = "Either start_date and end_date or periods must be provided, not both"
        logger.error(msg)
        raise InvalidParameterError(msg)

    if start_date:
        start_date = start_date.strftime("%Y-%m-%d")
    if end_date:
        end_date = end_date.strftime("%Y-%m-%d")

    values = dhis2.data_value_sets.get(
        data_element_groups=[data_element_group],
        periods=periods if periods else None,
        start_date=start_date if start_date else None,
        end_date=end_date if end_date else None,
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
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    periods: list[str] | None = None,
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
    start_date : str, optional
        Start date in the format "YYYY-MM-DD".
        Use either start_date and end_date or periods.
    end_date : str, optional
        End date in the format "YYYY-MM-DD".
        Use either start_date and end_date or periods.
    periods : list[str], optional
        Periods to extract data values for (ex: ["202101", "202102", "202103"]). Periods must be
        provided in DHIS2 period format. Use either start_date and end_date or periods.
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
        msg = "org_units or org_unit_groups must be provided"
        logger.error(msg)
        raise MissingParameterError(msg)

    if org_units is not None and org_unit_groups is not None:
        msg = "org_units and org_unit_groups cannot be provided at the same time"
        logger.error(msg)
        raise InvalidParameterError(msg)

    if not (start_date and end_date) and not periods:
        msg = "Either start_date and end_date or periods must be provided"
        logger.error(msg)
        raise MissingParameterError(msg)

    if (start_date or end_date) and periods:
        msg = "Either start_date and end_date or periods must be provided, not both"
        logger.error(msg)
        raise InvalidParameterError(msg)

    if start_date:
        start_date = start_date.strftime("%Y-%m-%d")
    if end_date:
        end_date = end_date.strftime("%Y-%m-%d")

    values = dhis2.data_value_sets.get(
        data_elements=data_elements,
        periods=periods if periods else None,
        start_date=start_date if start_date else None,
        end_date=end_date if end_date else None,
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

    if (data_elements or data_element_groups) and (indicators or indicator_groups):
        msg = "Data elements and indicators cannot be requested at the same time"
        logger.error(msg)
        raise ValueError(msg)

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


def _validate_data_values(df: pl.DataFrame) -> None:
    """Validate data values dataframe for import into DHIS2.

    Parameters
    ----------
    df : pl.DataFrame
        Dataframe containing data values.

    Raises
    ------
    MissingColumn
        If a required column is missing.
    InvalidDataType
        If a column has an invalid data type.
    """
    if "data_element_id" not in df.columns:
        msg = "Missing data_element_id column"
        logger.error(msg)
        raise MissingColumnError(msg)
    if "organisation_unit_id" not in df.columns:
        msg = "Missing organisation_unit_id column"
        logger.error(msg)
        raise MissingColumnError(msg)
    if "period" not in df.columns:
        msg = "Missing period column"
        logger.error(msg)
        raise MissingColumnError(msg)
    if "category_option_combo_id" not in df.columns:
        msg = "Missing category_option_combo_id column"
        logger.error(msg)
        raise MissingColumnError(msg)
    if "attribute_option_combo_id" not in df.columns:
        msg = "Missing attribute_option_combo_id column"
        logger.error(msg)
        raise MissingColumnError(msg)
    if "value" not in df.columns:
        msg = "Missing value column"
        logger.error(msg)
        raise MissingColumnError(msg)
    if df["data_element_id"].dtype != pl.Utf8:
        msg = "data_element_id must be of type String"
        logger.error(msg)
        raise InvalidDataTypeError(msg)
    if df["organisation_unit_id"].dtype != pl.Utf8:
        msg = "organisation_unit_id must be of type String"
        logger.error(msg)
        raise InvalidDataTypeError(msg)
    if df["period"].dtype != pl.Utf8:
        msg = "period must be of type String"
        logger.error(msg)
        raise InvalidDataTypeError(msg)
    if df["category_option_combo_id"].dtype != pl.Utf8:
        msg = "category_option_combo_id must be of type String"
        logger.error(msg)
        raise InvalidDataTypeError(msg)
    if df["attribute_option_combo_id"].dtype != pl.Utf8:
        msg = "attribute_option_combo_id must be of type String"
        logger.error(msg)
        raise InvalidDataTypeError(msg)
    if df["value"].dtype != pl.Utf8:
        msg = "value must be of type String"
        logger.error(msg)
        raise InvalidDataTypeError(msg)


def _map_uids(df: pl.DataFrame, **mappings) -> pl.DataFrame:
    """Replace UIDs in a dataframe based on mappings.

    Replacements are strict, which means that if a uid is not found in the mapping,
    it will be assigned to None.

    Parameters
    ----------
    df : pl.DataFrame
        Dataframe containing data values.
    mappings : dict
        Mappings to replace UIDs, with column name as key and dict mapping as value.

    Returns
    -------
    pl.DataFrame
        Dataframe with UIDs replaced based on mappings.
    """
    for col, mapping in mappings.items():
        if mapping:
            if col not in df.columns:
                msg = f"Missing {col} column"
                logger.error(msg)
                raise MissingColumnError(msg)
            df = df.with_columns(pl.col(col).replace_strict(mapping, default=None))
    return df


def import_data_values(
    dhis2: DHIS2,
    data: pl.DataFrame,
    org_units_mapping: dict | None = None,
    data_elements_mapping: dict | None = None,
    category_option_combos_mapping: dict | None = None,
    attribute_option_combos_mapping: dict | None = None,
    import_strategy: Literal["CREATE", "UPDATE", "CREATE_AND_UPDATE", "DELETE"] = "CREATE",
    dry_run: bool = True,
) -> dict:
    """Import data values to a DHIS2 instance.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    data : pl.DataFrame
        Dataframe containing data values with the following columns: data_element_id, period,
        organisation_unit_id, category_option_combo_id, attribute_option_combo_id, value.
    org_units_mapping : dict, optional
        Organisation units mapping with old UID as key and new UID as value.
    data_elements_mapping : dict, optional
        Data elements mapping with old UID as key and new UID as value.
    category_option_combos_mapping : dict, optional
        Category option combos mapping with old UID as key and new UID as value.
    attribute_option_combos_mapping : dict, optional
        Attribute option combos mapping with old UID as key and new UID as value.
    import_strategy : str, optional
        Import strategy. One of "CREATE", "UPDATE", "CREATE_AND_UPDATE", "DELETE". Default is "CREATE".
    dry_run : bool, optional
        Perform a dry run. Default is True.

    Returns
    -------
    dict
        Import report.

    Raises
    ------
    MissingColumn
        If a required column is missing.
    InvalidDataType
        If a column has an invalid data type.
    """
    data = _map_uids(
        df=data,
        organisation_unit_id=org_units_mapping,
        data_element_id=data_elements_mapping,
        category_option_combo_id=category_option_combos_mapping,
        attribute_option_combo_id=attribute_option_combos_mapping,
    )

    # ignore rows with null values
    nrows = len(data)
    data = data.drop_nulls()
    nrows_dropped = nrows - len(data)
    logger.debug(f"Dropped {nrows_dropped} rows with null values")

    _validate_data_values(df=data)

    data_values = data.select(
        pl.col("data_element_id").alias("dataElement"),
        pl.col("period"),
        pl.col("organisation_unit_id").alias("orgUnit"),
        pl.col("category_option_combo_id").alias("categoryOptionCombo"),
        pl.col("attribute_option_combo_id").alias("attributeOptionCombo"),
        pl.col("value"),
    ).to_dicts()

    report = dhis2.data_value_sets.post(
        data_values=data_values,
        import_strategy=import_strategy,
        dry_run=dry_run,
        skip_validation=True,
    )

    return report


def _extract_attribute_values(attribute_values: list[dict], mapping: dict) -> list[dict]:
    """Reshape attributeValue struct returned by the DHIS2 API and join attribute names.

    Parameters
    ----------
    attribute_values : list[dict]
        List of attribute values.
    mapping : dict
        Mapping of attribute IDs to attribute names.

    Returns
    -------
    list[dict]
        List of attribute values with the following keys: attribute_id, attribute_name, value
    """
    if attribute_values is None:
        return None

    values = []

    for attribute in attribute_values:
        attribute_id = attribute["attribute"]["id"]
        attribute_name = mapping[attribute_id]

        if attribute_id == "coordinates":
            continue

        values.append(
            {
                "attribute_id": attribute_id,
                "attribute_name": attribute_name,
                "value": attribute["value"],
            }
        )

    return values


def extract_organisation_unit_attributes(dhis2: DHIS2) -> pl.DataFrame:
    """Extract organisation unit attributes.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.

    Returns
    -------
    pl.DataFrame
        Dataframe containing organisation unit attributes with the following columns:
        organisation_unit_id, organisation_unit_name, attribute_id, attribute_name, value.
    """
    attributes = get_attributes(dhis2)
    mapping = {attr["id"]: attr["name"] for attr in attributes.to_dicts()}
    org_units = dhis2.meta.organisation_units(fields="id,name,attributeValues")
    schema = {
        "id": str,
        "name": str,
        "attributeValues": pl.List(pl.Struct({"attribute": pl.Struct({"id": str}), "value": str})),
    }
    org_units = pl.DataFrame(org_units, schema=schema)

    rows = []
    for row in org_units.iter_rows(named=True):
        attrs = _extract_attribute_values(row["attributeValues"], mapping)
        for attr in attrs:
            rows.append(
                {
                    "organisation_unit_id": row["id"],
                    "organisation_unit_name": row["name"],
                    "attribute_id": attr["attribute_id"],
                    "attribute_name": attr["attribute_name"],
                    "value": attr["value"],
                }
            )

    schema = {
        "organisation_unit_id": str,
        "organisation_unit_name": str,
        "attribute_id": str,
        "attribute_name": str,
        "value": str,
    }

    return pl.DataFrame(rows, schema=schema)


def get_programs(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract programs metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing programs metadata with the following columns: id, name.
    """
    rows = []
    for page in dhis2.api.get_paged("programs", params={"fields": "id,name", "filter": filters}):
        rows.extend(page["programs"])

    schema = {"id": str, "name": str}
    return pl.DataFrame(rows, schema=schema)


def get_tracked_entity_types(dhis2: DHIS2, filters: list[str] | None = None) -> pl.DataFrame:
    """Extract tracked entity types metadata.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    filters : list[str], optional
        DHIS2 query filter expressions.

    Returns
    -------
    pl.DataFrame
        Dataframe containing tracked entity types metadata with the following columns: id, name.
    """
    rows = []
    for page in dhis2.api.get_paged("trackedEntityTypes", params={"fields": "id,name", "filter": filters}):
        rows.extend(page["trackedEntityTypes"])

    schema = {"id": str, "name": str}
    return pl.DataFrame(rows, schema=schema)


def extract_events(
    dhis2: DHIS2,
    program_id: str,
    org_unit_parents: list[str],
    occurred_after: str | None = None,
    occurred_before: str | None = None,
) -> pl.DataFrame:
    """Extract events data.

    Parameters
    ----------
    dhis2 : DHIS2
        DHIS2 instance.
    program_id : str
        Program UID.
    org_unit_parents : list[str]
        Organisation unit parents UIDs. Event data will be extracted for all descendants of these
        organisation units.
    occurred_after : str, optional
        Start date in the format "YYYY-MM-DD".
    occurred_before : str, optional
        End date in the format "YYYY-MM-DD".

    Returns
    -------
    pl.DataFrame
        Dataframe containing events data with the following columns: event_id, program_id,
        organisation_unit_id, tracked_entity_instance_id, event_date.
    """
    data = []
    for org_unit in org_unit_parents:
        params = {
            "orgUnit": org_unit,
            "program": program_id,
            "ouMode": "DESCENDANTS",
            "fields": "event,status,program,programStage,orgUnit,occurredAt,deleted,attributeOptionCombo,dataValues",
            "totalPages": True,
        }
        if occurred_after:
            params["occurredAfter"] = occurred_after
        if occurred_before:
            params["occurredBefore"] = occurred_before
        for page in dhis2.api.get_paged("tracker/events", params=params):
            data.extend(page["instances"])

    schema = {
        "event": str,
        "status": str,
        "program": str,
        "programStage": str,
        "orgUnit": str,
        "occurredAt": str,
        "deleted": bool,
        "attributeOptionCombo": str,
        "dataValues": pl.List(pl.Struct({"dataElement": str, "value": str})),
    }

    df = pl.DataFrame(data, schema=schema)
    df = df.select(
        [
            pl.col("event").alias("event_id"),
            pl.col("status"),
            pl.col("program").alias("program_id"),
            pl.col("programStage").alias("program_stage_id"),
            pl.col("orgUnit").alias("organisation_unit_id"),
            pl.col("occurredAt").str.to_datetime("%Y-%m-%dT%H:%M:%S.%3f").alias("occurred_at"),
            pl.col("deleted"),
            pl.col("attributeOptionCombo").alias("attribute_option_combo_id"),
            pl.col("dataValues"),
        ]
    )

    # build a new dataframe with one row per event data value, instead of storing all data values
    # in a column of type list[struct]

    new_rows = []

    for row in df.iter_rows(named=True):
        for data_value in row["dataValues"]:
            new_row = {col: value for col, value in row.items() if col != "dataValues"}
            new_row["data_element_id"] = data_value["dataElement"]
            new_row["value"] = data_value["value"]
            new_rows.append(new_row)

    schema = {
        "event_id": str,
        "status": str,
        "program_id": str,
        "program_stage_id": str,
        "organisation_unit_id": str,
        "occurred_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
        "deleted": bool,
        "attribute_option_combo_id": str,
        "data_element_id": str,
        "value": str,
    }

    return pl.DataFrame(new_rows, schema=schema)


def join_object_names(
    df: pl.DataFrame,
    data_elements: pl.DataFrame | None = None,
    indicators: pl.DataFrame | None = None,
    organisation_units: pl.DataFrame | None = None,
    category_option_combos: pl.DataFrame | None = None,
) -> pl.DataFrame:
    if (
        (data_elements is None)
        and (organisation_units is None)
        and (category_option_combos is None)
        and (indicators is None)
    ):
        msg = "At least one of data_elements, organisation_units or category_option_combos must be provided"
        logger.error(msg)
        raise ValueError(msg)

    if data_elements is not None and "data_element_name" not in df.columns:
        df = df.join(
            other=data_elements.select("id", pl.col("name").alias("data_element_name")),
            left_on="data_element_id",
            right_on="id",
            how="left",
        )

    if indicators is not None and "indicator_name" not in df.columns:
        df = df.join(
            other=indicators.select("id", pl.col("name").alias("indicator_name")),
            left_on="indicator_id",
            right_on="id",
            how="left",
        )

    if organisation_units is not None and "organisation_unit_name" not in df.columns:
        ou_ids = [col for col in organisation_units.columns if col.startswith("level_") and col.endswith("_id")]
        ou_names = [col for col in organisation_units.columns if col.startswith("level_") and col.endswith("_name")]
        df = df.join(
            other=organisation_units.select("id", *ou_ids, *ou_names),
            left_on="organisation_unit_id",
            right_on="id",
            how="left",
        )

    if category_option_combos is not None and "category_option_combo_name" not in df.columns:
        df = df.join(
            other=category_option_combos.select("id", pl.col("name").alias("category_option_combo_name")),
            left_on="category_option_combo_id",
            right_on="id",
            how="left",
        )

    COLUMNS = [
        "dataset_name",
        "dataset_id",
        "dataset_period_type",
        "data_element_id",
        "data_element_name",
        "indicator_id",
        "indicator_name",
        "organisation_unit_id",
        "organisation_unit_name",
        "category_option_combo_id",
        "category_option_combo_name",
        "attribute_option_combo_id",
        "period",
        "value",
        *[col for col in df.columns if col.startswith("level_")],
        "created",
        "last_updated",
    ]

    # if additional columns were present in the original dataframe, keep them at the end
    COLUMNS += [col for col in df.columns if col not in COLUMNS]

    return df.select([col for col in COLUMNS if col in df.columns])
