import itertools
import json
import logging
from pathlib import Path
from typing import Iterator, List, Tuple, Union, Optional, Any, Dict

import pandas as pd
import polars as pl

from .api import Api, DHIS2Connection, DHIS2Error
from .periods import Period

logger = logging.getLogger(__name__)


class DHIS2:
    def __init__(self, connection: DHIS2Connection = None, cache_dir: Union[str, Path] = None, **kwargs):
        """Initialize a new DHIS2 instance.

        Parameters
        ----------
        connection : openhexa DHIS2Connection, optional
            An initialized openhexa dhis2 connection
        kwargs:
            Additional arguments to pass to initialize openhexa dhis2 connection, such as `url`, `username`, `password`
        cache_dir : str, optional
            Cache directory. Actual cache data will be stored under a sub-directory
            named after the DHIS2 instance domain.
        """
        if isinstance(cache_dir, str):
            cache_dir = Path(cache_dir)
        self.api = Api(connection, cache_dir, **kwargs)
        self.meta = Metadata(self)
        self.version = self.meta.system_info().get("version")
        self.data_value_sets = DataValueSets(self)
        self.analytics = Analytics(self)


class Metadata:
    def __init__(self, client: DHIS2):
        """Methods for accessing metadata API endpoints."""
        self.client = client

    def system_info(self) -> dict:
        """Get information on the current system."""
        r = self.client.api.get("system/info")
        return r

    def identifiable_objects(self, uid: str) -> dict:
        """Get metadata from element UID"""
        r = self.client.api.get(f"identifiableObjects/{uid}")
        return r

    def organisation_unit_levels(
        self,
        fields: str = "id,name,level",
    ) -> List[dict]:
        """Get names of all organisation unit levels.

        Parameters
        ----------
        fields: str, optional
            DHIS2 fields to include in the response, where default value is "id,name,level"
        Return
        ------
        list of dict
            Id, number and name of each org unit level.
        """
        params = {"fields": fields}
        r = self.client.api.get("filledOrganisationUnitLevels", params=params)
        levels = []
        fields_list = fields.split(",")
        for level in r:
            levels.append({k: v for k, v in level.items() if k in fields_list})
        return levels

    def organisation_units(
        self,
        fields: str = "id,name,level,path,geometry",
        page: Optional[int] = None,
        pageSize: Optional[int] = None,
        filters: Optional[List[str]] = None,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Get organisation units metadata from DHIS2.

        Parameters
        ----------
        fields: str, optional
            Comma-separated DHIS2 fields to include in the response.
        page: int, optional
            Page number for paginated requests.
        pageSize: int, optional
            Number of results per page.
        filters: list of str, optional
            DHIS2 query filters.

        Returns
        -------
        Union[List[Dict[str, Any]], Dict[str, Any]]
            - If `page` and `pageSize` are **not** provided: Returns a **list** of organisation units.
            - If `page` and `pageSize` **are** provided: Returns a **dict** with `organisationUnits` and `pager` for pagination.
        """

        def format_unit(ou: Dict[str, Any], fields: str) -> Dict[str, Any]:
            return {
                key: json.dumps(ou[key]) if key == "geometry" and ou.get(key) else ou.get(key)
                for key in fields.split(",")
            }

        params = {"fields": fields}

        if filters:
            params["filter"] = filters

        if page and pageSize:
            print(f"Page and pageSize : {page} {pageSize}")
            params["page"] = page
            params["pageSize"] = pageSize
            response = self.client.api.get("organisationUnits", params=params)

            org_units = [format_unit(ou, fields) for ou in response.get("organisationUnits", [])]

            return {"organisationUnits": org_units, "pager": response.get("pager", {})}

        org_units = [
            format_unit(ou, fields)
            for page in self.client.api.get_paged("organisationUnits", params=params)
            for ou in page.get("organisationUnits", [])
        ]

        return org_units

    def organisation_unit_groups(
        self,
        fields: str = "id,name,organisationUnits",
        page: Optional[int] = None,
        pageSize: Optional[int] = None,
        filters: Optional[List[str]] = None,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Get organisation unit groups metadata from DHIS2.

        Parameters
        ----------
        fields: str, optional
            Comma-separated DHIS2 fields to include in the response (default: "id,name,organisationUnits").
        page: int, optional
            Page number for paginated requests.
        pageSize: int, optional
            Number of results per page.
        filters: list of str, optional
            DHIS2 query filters.

        Returns
        -------
        Union[List[Dict[str, Any]], Dict[str, Any]]
            - If `page` and `pageSize` are **not** provided: Returns a **list** of organisation unit groups.
            - If `page` and `pageSize` **are** provided: Returns a **dict** with `organisationUnitGroups` and `pager` for pagination.
        """

        def format_unit_group(group: Dict[str, Any], fields: str) -> Dict[str, Any]:
            return {
                key: group.get(key)
                if key != "organisationUnits"
                else [ou.get("id") for ou in group.get("organisationUnits", [])]
                for key in fields.split(",")
            }

        params = {"fields": fields}

        if filters:
            params["filter"] = filters

        if page and pageSize:
            params["page"] = page
            params["pageSize"] = pageSize
            response = self.client.api.get("organisationUnitGroups", params=params)

            org_unit_groups = [format_unit_group(group, fields) for group in response.get("organisationUnitGroups", [])]

            return {"organisationUnitGroups": org_unit_groups, "pager": response.get("pager", {})}

        org_unit_groups = [
            format_unit_group(group, fields)
            for page in self.client.api.get_paged("organisationUnitGroups", params=params)
            for group in page.get("organisationUnitGroups", [])
        ]

        return org_unit_groups

    def datasets(
        self,
        fields: str = "id,name,dataSetElements,indicators,organisationUnits",
        page: Optional[int] = None,
        pageSize: Optional[int] = None,
        filters: Optional[List[str]] = None,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Get datasets metadata from DHIS2.

        Parameters
        ----------
        fields: str, optional
            Comma-separated DHIS2 fields to include in the response.
        page: int, optional
            Page number for paginated requests.
        pageSize: int, optional
            Number of results per page.
        filters: list of str, optional
            DHIS2 query filters.

        Returns
        -------
        Union[List[Dict[str, Any]], Dict[str, Any]]
            - If `page` and `pageSize` are **not** provided: Returns a **list** of datasets.
            - If `page` and `pageSize` **are** provided: Returns a **dict** with `dataSets` and `pager` for pagination.
        """

        def format_dataset(ds: Dict[str, Any], fields: str) -> Dict[str, Any]:
            fields_list = fields.split(",")
            formatted_ds = {}

            if "dataSetElements" in fields_list:
                formatted_ds["data_elements"] = [dx["dataElement"]["id"] for dx in ds.get("dataSetElements", [])]
                fields_list.remove("dataSetElements")
            if "indicators" in fields_list:
                formatted_ds["indicators"] = [indicator["id"] for indicator in ds.get("indicators", [])]
                fields_list.remove("indicators")
            if "organisationUnits" in fields_list:
                formatted_ds["organisation_units"] = [ou["id"] for ou in ds.get("organisationUnits", [])]
                fields_list.remove("organisationUnits")

            formatted_ds.update({key: ds.get(key) for key in fields_list})
            return formatted_ds

        params = {"fields": fields}

        if filters:
            params["filter"] = filters

        if page and pageSize:
            params["page"] = page
            params["pageSize"] = pageSize
            response = self.client.api.get("dataSets", params=params)

            datasets = [format_dataset(ds, fields) for ds in response.get("dataSets", [])]

            return {"dataSets": datasets, "pager": response.get("pager", {})}

        datasets = [
            format_dataset(ds, fields)
            for page in self.client.api.get_paged("dataSets", params=params)
            for ds in page.get("dataSets", [])
        ]

        return datasets

    def data_elements(
        self,
        fields: str = "id,name,aggregationType,zeroIsSignificant",
        page: Optional[int] = None,
        pageSize: Optional[int] = None,
        filters: Optional[List[str]] = None,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Get data elements metadata from DHIS2.

        Parameters
        ----------
        fields: str, optional
            Comma-separated DHIS2 fields to include in the response.
        page: int, optional
            Page number for paginated requests.
        pageSize: int, optional
            Number of results per page.
        filters: list of str, optional
            DHIS2 query filters.

        Returns
        -------
        Union[List[Dict[str, Any]], Dict[str, Any]]
            - If `page` and `pageSize` are **not** provided: Returns a **list** of data elements.
            - If `page` and `pageSize` **are** provided: Returns a **dict** with `dataElements` and `pager` for pagination.
        """

        def format_element(element: Dict[str, Any], fields: str) -> Dict[str, Any]:
            return {key: element.get(key) for key in fields.split(",")}

        params = {"fields": fields}

        if filters:
            params["filter"] = filters

        if page and pageSize:
            params["page"] = page
            params["pageSize"] = pageSize
            response = self.client.api.get("dataElements", params=params)

            elements = [format_element(element, fields) for element in response.get("dataElements", [])]

            return {"dataElements": elements, "pager": response.get("pager", {})}

        elements = [
            format_element(element, fields)
            for page in self.client.api.get_paged("dataElements", params=params)
            for element in page.get("dataElements", [])
        ]

        return elements

    def data_element_groups(
        self,
        fields: str = "id,name,dataElements",
        page: Optional[int] = None,
        pageSize: Optional[int] = None,
        filters: Optional[List[str]] = None,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Get data element groups metadata from DHIS2.

        Parameters
        ----------
        fields: str, optional
            Comma-separated DHIS2 fields to include in the response.
        page: int, optional
            Page number for paginated requests.
        pageSize: int, optional
            Number of results per page.
        filters: list of str, optional
            DHIS2 query filters.

        Returns
        -------
        Union[List[Dict[str, Any]], Dict[str, Any]]
            - If `page` and `pageSize` are **not** provided: Returns a **list** of data element groups.
            - If `page` and `pageSize` **are** provided: Returns a **dict** with `dataElementGroups` and `pager` for pagination.
        """

        def format_group(group: Dict[str, Any], fields: str) -> Dict[str, Any]:
            return {
                key: group.get(key) if key != "dataElements" else [de.get("id") for de in group.get("dataElements", [])]
                for key in fields.split(",")
            }

        params = {"fields": fields}

        if filters:
            params["filter"] = filters

        if page and pageSize:
            params["page"] = page
            params["pageSize"] = pageSize
            response = self.client.api.get("dataElementGroups", params=params)

            de_groups = [format_group(group, fields) for group in response.get("dataElementGroups", [])]

            return {"dataElementGroups": de_groups, "pager": response.get("pager", {})}

        de_groups = [
            format_group(group, fields)
            for page in self.client.api.get_paged("dataElementGroups", params=params)
            for group in page.get("dataElementGroups", [])
        ]

        return de_groups

    def category_option_combos(self) -> List[dict]:
        """Get category option combos metadata.

        Return
        ------
        list of dict
            Id and name of all category option combos.
        """
        combos = []
        for page in self.client.api.get_paged("categoryOptionCombos", params={"fields": "id,name"}):
            combos += page.get("categoryOptionCombos")
        return combos

    def indicators(
        self,
        fields: str = "id,name,numerator,denominator",
        page: Optional[int] = None,
        pageSize: Optional[int] = None,
        filters: Optional[List[str]] = None,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Get indicators metadata from DHIS2.

        Parameters
        ----------
        fields: str, optional
            Comma-separated DHIS2 fields to include in the response.
        page: int, optional
            Page number for paginated requests.
        pageSize: int, optional
            Number of results per page.
        filters: list of str, optional
            DHIS2 query filters.

        Returns
        -------
        Union[List[Dict[str, Any]], Dict[str, Any]]
            - If `page` and `pageSize` are **not** provided: Returns a **list** of indicators.
            - If `page` and `pageSize` **are** provided: Returns a **dict** with `indicators` and `pager` for pagination.
        """

        def format_indicator(indicator: Dict[str, Any], fields: str) -> Dict[str, Any]:
            return {key: indicator.get(key) for key in fields.split(",")}

        params = {"fields": fields}

        if filters:
            params["filter"] = filters

        if page and pageSize:
            params["page"] = page
            params["pageSize"] = pageSize
            response = self.client.api.get("indicators", params=params)

            indicators = [format_indicator(indicator, fields) for indicator in response.get("indicators", [])]

            return {"indicators": indicators, "pager": response.get("pager", {})}

        indicators = [
            format_indicator(indicator, fields)
            for page in self.client.api.get_paged("indicators", params=params)
            for indicator in page.get("indicators", [])
        ]

        return indicators

    def indicator_groups(
        self, fields: str = "id,name,indicators", page: int = None, pageSize: int = None, filters: [str] = None
    ) -> List[dict]:
        """Get indicator groups metadata.

        Return
        ------
        list of dict
            Id, name and indicators of all indicator groups.
        """
        ind_groups = []
        for page in self.client.api.get_paged(
            "indicatorGroups",
            params={"fields": fields},
        ):
            groups = []
            for group in page.get("indicatorGroups"):
                groups.append(
                    {
                        key: group.get(key)
                        if key != "indicators"
                        else [indicator.get("id") for indicator in group["indicators"]]
                        for key in fields.split(",")
                    }
                )
            ind_groups += groups
        return ind_groups

    @staticmethod
    def _get_uid_from_level(path: str, level: int):
        """Extract org unit uid from a path string."""
        parents = path.split("/")[1:-1]
        if len(parents) >= level:
            return parents[level - 1]
        else:
            return None

    def add_dx_name_column(
        self,
        dataframe: Union[pl.DataFrame, pd.DataFrame],
        dx_id_column: str = "dx",
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """Add column with dx name to input dataframe.

        Parameters
        ----------
        dataframe : polars or pandas dataframe
            Input dataframe with a dx id column
        org_unit_id_column : str (default="dx")
            Name of dx id column in input dataframe

        Return
        ------
        polars or pandas dataframe
            Input dataframe with a new "dx_name" column
        """
        src_format = _get_dataframe_frmt(dataframe)
        if src_format == "pandas":
            df = pl.DataFrame._from_pandas(dataframe)
        else:
            df = dataframe

        data_elements = pl.DataFrame(self.data_elements())
        indicators = pl.DataFrame(self.indicators())
        dx = pl.concat(
            [
                data_elements.select([pl.col("id"), pl.col("name").alias("dx_name")]),
                indicators.select([pl.col("id"), pl.col("name").alias("dx_name")]),
            ]
        )

        df = df.join(
            other=dx.select([pl.col("id"), pl.col("dx_name")]),
            how="left",
            left_on=dx_id_column,
            right_on="id",
        )

        if src_format == "pandas":
            df = df.to_pandas()
        return df

    def add_coc_name_column(
        self,
        dataframe: Union[pl.DataFrame, pd.DataFrame],
        coc_column: str = "co",
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """Add column with coc name to input dataframe.

        Parameters
        ----------
        dataframe : polars or pandas dataframe
            Input dataframe with a coc id column
        coc_id_column : str (default="co")
            Name of coc id column in input dataframe

        Return
        ------
        polars or pandas dataframe
            Input dataframe with a new "co_name" column
        """
        src_format = _get_dataframe_frmt(dataframe)
        if src_format == "pandas":
            df = pl.DataFrame._from_pandas(dataframe)
        else:
            df = dataframe

        coc = pl.DataFrame(self.category_option_combos())

        df = df.join(
            other=coc.select([pl.col("id"), pl.col("name").alias("co_name")]),
            how="left",
            left_on=coc_column,
            right_on="id",
        )

        if src_format == "pandas":
            df = df.to_pandas()
        return df

    def add_org_unit_name_column(
        self,
        dataframe: Union[pl.DataFrame, pd.DataFrame],
        org_unit_id_column: str = "ou",
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """Add column with org unit name to input dataframe.

        Parameters
        ----------
        dataframe : polars or pandas dataframe
            Input dataframe with a org unit id column
        org_unit_id_column : str (default="ou")
            Name of org unit id column in input dataframe

        Return
        ------
        polars or pandas dataframe
            Input dataframe with a new "ou_name" column
        """
        src_format = _get_dataframe_frmt(dataframe)
        if src_format == "pandas":
            df = pl.DataFrame._from_pandas(dataframe)
        else:
            df = dataframe

        org_units = pl.DataFrame(self.organisation_units())

        df = df.join(
            other=org_units.select([pl.col("id"), pl.col("name").alias("ou_name")]),
            how="left",
            left_on=org_unit_id_column,
            right_on="id",
        )

        if src_format == "pandas":
            df = df.to_pandas()
        return df

    def add_org_unit_parent_columns(
        self,
        dataframe: Union[pl.DataFrame, pd.DataFrame],
        org_unit_id_column: str = "ou",
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """Add parent org units id and names to input dataframe.

        Parameters
        ----------
        dataframe : polars or pandas dataframe
            Input dataframe with a org unit id column
        org_unit_id_column : str (default="ou")
            Name of org unit id column in input dataframe

        Return
        ------
        polars or pandas dataframe
            Input dataframe with added columns
        """
        src_format = _get_dataframe_frmt(dataframe)
        if src_format == "pandas":
            df = pl.DataFrame._from_pandas(dataframe)
        else:
            df = dataframe

        levels = pl.DataFrame(self.organisation_unit_levels())
        org_units = pl.DataFrame(self.organisation_units())

        for lvl in range(1, len(levels)):
            org_units = org_units.with_columns(
                pl.col("path").apply(lambda path: self._get_uid_from_level(path, lvl)).alias(f"parent_level_{lvl}_id")
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

        df = df.join(
            other=org_units.select(["id"] + [col for col in org_units.columns if col.startswith("parent_")]),
            how="left",
            left_on=org_unit_id_column,
            right_on="id",
        )

        if src_format == "pandas":
            df = df.to_pandas()
        return df


def _split_list(src_list: list, length: int) -> Iterator[List]:
    """Split list into chunks."""
    for i in range(0, len(src_list), length):
        yield src_list[i : i + length]


class DataValueSets:
    def __init__(self, client: DHIS2):
        """Methods for the dataValueSets API endpoint."""
        self.client = client
        self.MAX_DATA_ELEMENTS = 50
        self.MAX_ORG_UNITS = 50
        self.MAX_PERIODS = 1
        self.MAX_POST_DATA_VALUES = 50

    def split_params(self, params: dict) -> List[dict]:
        """Split request parameters into chunks.

        We try to chunk on three distinct parameters: period, orgUnit and dataElement -
        as they are usually the largest ones. NB: all of these parameters are optional.
        """
        params_to_chunk = []
        for param, max_length in zip(
            ["period", "orgUnit", "dataElement"],
            [self.MAX_PERIODS, self.MAX_ORG_UNITS, self.MAX_DATA_ELEMENTS],
        ):
            if params.get(param):
                params_to_chunk.append((param, max_length))

        if not params_to_chunk:
            return [params]

        chunks = []
        for chunk in itertools.product(
            *[_split_list(params.get(param), max_length) for param, max_length in params_to_chunk]
        ):
            p = params.copy()
            for i, (param, _) in enumerate(params_to_chunk):
                p[param] = chunk[i]
            chunks.append(p)

        return chunks

    def get(
        self,
        data_elements: List[str] = None,
        datasets: List[str] = None,
        data_element_groups: List[str] = None,
        periods: List[Union[str, Period]] = None,
        start_date: str = None,
        end_date: str = None,
        org_units: List[str] = None,
        org_unit_groups: List[str] = None,
        children: bool = False,
        attribute_option_combos: List[str] = None,
        last_updated: str = None,
        last_updated_duration: str = None,
    ) -> List[dict]:
        """Retrieve data values through the dataValueSets API resource.

        Parameters
        ----------
        data_elements : list of str, optional
            Data element identifiers (requires DHIS2 >= 2.39)
        datasets : str, optional
            Dataset identifiers
        data_element_groups : str, optional
            Data element groups identifiers
        periods : list of str or Period, optional
            Period identifiers in DHIS2 format
        start_date : str, optional
            Start date for the time span of the values to export (example: "2020-01-01")
        end_date : str, optional
            End date for the time span of the values to export (example: "2020-06-01")
        org_units : list of str, optional
            Organisation units identifiers
        org_unit_groups : list of str, optional
            Organisation unit groups identifiers
        children : bool, optional (default=False)
            Whether to include the children in the hierarchy of the organisation units
        attribute_option_combos : list of str, optional
            Attribute option combos identifiers
        last_updated : str, optional
            Include only data values which are updated since the given time stamp
        last_updated_duration : str, optional
            Include only data values which are updated within the given duration. The
            format is <value><time-unit>, where the supported time units are "d" (days),
            "h" (hours), "m" (minutes) and "s" (seconds).

        Return
        ------
        list of dict
            Response as a list of data values
        """
        what = data_elements or datasets or data_element_groups
        where = org_units or org_unit_groups
        when = (start_date and end_date) or periods or last_updated or last_updated_duration
        if not what:
            raise DHIS2Error("No data dimension provided")
        if not where:
            raise DHIS2Error("No spatial dimension provided")
        if not when:
            raise DHIS2Error("No temporal dimension provided")

        if data_elements and not self.client.version >= "2.39":
            raise DHIS2Error("Data elements parameter not supported for DHIS2 versions < 2.39")

        if periods:
            if all([isinstance(pe, Period) for pe in periods]):
                # convert Period objects to ISO strings
                periods = [str(pe) for pe in periods]
            elif not all([isinstance(pe, str) for pe in periods]):
                raise ValueError("Mixed period types")

        params = {
            "dataElement": data_elements,
            "dataSet": datasets,
            "dataElementGroup": data_element_groups,
            "period": periods,
            "startDate": start_date,
            "endDate": end_date,
            "orgUnit": org_units,
            "orgUnitGroup": org_unit_groups,
            "children": children,
            "attributeOptionCombo": attribute_option_combos,
            "lastUpdated": last_updated,
            "lastUpdatedDuration": last_updated_duration,
        }

        chunks = self.split_params(params)
        response = []
        for chunk in chunks:
            r = self.client.api.get("dataValueSets", params=chunk)
            if "dataValues" in r:
                response += r["dataValues"]

        return response

    def _validate(self, data_values: List[dict]):
        """Validate data values based on data element value type.

        Supported: NUMBER, INTEGER, UNIT_INTERVAL, PERCENTAGE, INTEGER_POSITIVE,
        INTEGER_NEGATIVE, INTEGER_ZERO_OR_POSITIVE, TEXT, LONG_TEXT, LETTER, BOOLEAN.
        Not supported: FILE_RESOURCE, COORDINATE, PHONE_NUMBER, EMAIL, TRUE_ONLY, DATE,
        DATETIME.
        """
        value_types = {}

        for dv in data_values:
            value = dv.get("value")
            de_uid = dv.get("dataElement")

            # keep a cache with value type for each data element
            if de_uid in value_types:
                value_type = value_types[de_uid]
            else:
                value_type = self.client.meta.identifiable_objects(de_uid).get("valueType")
                value_types[de_uid] = value_type

            for key in [
                "dataElement",
                "orgUnit",
                "period",
                "value",
                "categoryOptionCombo",
                "attributeOptionCombo",
            ]:
                if not dv.get(key):
                    raise ValueError(f"Missing {key} key in data value")

            if value_type == "INTEGER":
                if not isinstance(value, int):
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "NUMBER":
                if not isinstance(value, int) and not isinstance(value, float):
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "UNIT_INTERVAL":
                if not isinstance(value, float) or not (value >= 0 and value <= 1):
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "PERCENTAGE":
                if (not isinstance(value, float) and not isinstance(value, int)) or not (value >= 0 and value <= 100):
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "INTEGER_POSITIVE":
                if not isinstance(value, int) and value <= 0:
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "INTEGER_NEGATIVE":
                if not isinstance(value, int) and value >= 0:
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "INTEGER_ZERO_OR_POSITIVE":
                if not isinstance(value, int) and value < 0:
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "TEXT":
                if not isinstance(value, str):
                    raise ValueError(f"Data value {value} is not a valid {value_type}")
                elif len(value) > 50000:
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "LONG_TEXT":
                if not isinstance(value, str):
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "LETTER":
                if not isinstance(value, str):
                    raise ValueError(f"Data value {value} is not a valid {value_type}")
                elif len(value) != 1:
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            elif value_type == "BOOLEAN":
                if not isinstance(value, bool):
                    raise ValueError(f"Data value {value} is not a valid {value_type}")

            else:
                raise ValueError(f"Data value type {value_type} not supported")

    def post(
        self,
        data_values: List[dict],
        import_strategy: str = "CREATE",
        dry_run: bool = True,
        skip_validation: bool = False,
    ) -> str:
        """Push data values to a DHIS2 instance using the dataValueSets API endpoint.

        Parameters
        ----------
        data_values : list of dict
            Data values as a list of dictionaries with the following keys: dataElement,
            period, orgUnit, categoryOptionCombo, attributeOptionCombo, and value.
        import_strategy : str, optional (default="CREATE")
            CREATE, UPDATE, CREATE_AND_UPDATE, or DELETE
        dry_run : bool, optional
            Whether to save changes on the server or just return the
            import summary
        skip_validation : bool, optional (default=False)
            Skip validation of data values.

        Return
        ------
        dict
            Import counts summary
        """
        if not skip_validation:
            self._validate(data_values)

        if import_strategy not in ("UPDATE", "CREATE", "CREATE_AND_UPDATE", "DELETE"):
            raise ValueError("Invalid import strategy")

        import_counts = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}

        for chunk in _split_list(data_values, self.MAX_POST_DATA_VALUES):
            r = self.client.api.post(
                endpoint="dataValueSets",
                json={"dataValues": chunk},
                params={"dryRun": dry_run, "importStrategy": import_strategy},
            )

            if "response" in r.json():
                summary = r.json()["response"]
            else:
                summary = r.json()

            if r.status_code != 200:
                raise DHIS2Error(summary.get("description"))

            for key in ["imported", "updated", "ignored", "deleted"]:
                import_counts[key] += summary["importCount"][key]

        return import_counts


class Analytics:
    def __init__(self, client: DHIS2):
        """Methods for the analytics API endpoint."""
        self.client = client
        self.MAX_DX = 50
        self.MAX_ORG_UNITS = 50
        self.MAX_PERIODS = 1

    @staticmethod
    def split_dimension_param(param: str) -> Tuple[str, List[str]]:
        """Split formatted dimension parameter.

        Formatted parameter (e.g. `dx:uid;uid;uid`) is splitted into two parts: id (e.g.
        `dx`) and items (e.g. `[uid, uid]`).

        Parameters
        ----------
        param : str
            Formatted dimension parameter

        Return
        ------
        str
            Dimension id
        list of str
            Dimension items
        """
        dim_id, dim_items = param.split(":")
        if dim_items:
            dim_items = dim_items.split(";")
        else:
            dim_items = []
        return dim_id, dim_items

    def split_params(self, params: dict) -> List[dict]:
        """Split JSON parameters expected by the Analytics API endpoint into chunks.

        Dimension parameter is splitted across 3 dimensions (data, time, space) with
        respect to the `MAX_DX`, `MAX_ORG_UNITS` and `MAX_PERIODS` attributes.

        Parameters
        ----------
        params : dict
            Request parameters

        Return
        ------
        list of dict
            List of chunked request parameters
        """
        MAX_DIM_ITEMS = {
            "dx": self.MAX_DX,
            "ou": self.MAX_ORG_UNITS,
            "pe": self.MAX_PERIODS,
        }

        dimension = params["dimension"]

        dim_chunks = []
        for dim in dimension:
            dim_id, dim_items = self.split_dimension_param(dim)
            if dim_id in MAX_DIM_ITEMS:
                dim_item_chunks = [item for item in _split_list(dim_items, MAX_DIM_ITEMS.get(dim_id, 50))]
                dim_item_chunks = [f"{dim_id}:{';'.join(dim_items)}" for dim_items in dim_item_chunks]
                dim_chunks.append(dim_item_chunks)
            else:
                dim_chunks.append([dim])

        param_chunks = []
        for chunk in itertools.product(*dim_chunks):
            param_chunk = params.copy()
            param_chunk["dimension"] = chunk
            param_chunks.append(param_chunk)

        return param_chunks

    @staticmethod
    def format_dimension_param(
        data_elements: List[str] = None,
        data_element_groups: List[str] = None,
        indicators: List[str] = None,
        indicator_groups: List[str] = None,
        periods: List[str] = None,
        org_units: List[str] = None,
        org_unit_groups: List[str] = None,
        org_unit_levels: List[int] = None,
        include_cocs: bool = True,
    ) -> List[str]:
        """Format dimension parameters as expected by the Analytics API endpoint.

        Data dimension parameters are formatted as `dx:<dx_uid>;<dx_uid>`. Periods are
        formatted as `pe:<period>;<period>`. Org units are formatted as
        `ou:<ou_uid>;<ou_uid>. Org unit groups and levels are also supported.

        Parameters
        ----------
        data_elements : list of str, optional
            Data element identifiers
        data_element_groups : str, optional
            Data element groups identifiers
        indicators : list of str, optional
            Indicator identifiers
        indicator_groups : list of str, optional
            Indicator groups indifiers
        periods : list of str, optional
            Period identifiers in DHIS2 format
        org_units : list of str, optional
            Organisation units identifiers
        org_unit_groups : list of str, optional
            Organisation unit groups identifiers
        org_unit_levels : list of int, optional
            Organisation unit levels
        include_cocs : bool, optional (default=True)
            Include category option combos in response

        Return
        ------
        list of str
            Formatted dimension parameters as expected by the Analytics API endpoint
        """
        dx = []
        if data_elements:
            dx += data_elements
        if data_element_groups:
            dx += [f"DE_GROUP-{group}" for group in data_element_groups]
        if indicators:
            dx += indicators
        if indicator_groups:
            dx += [f"IN_GROUP-{group}" for group in indicator_groups]

        pe = []
        if periods:
            pe += [str(p) for p in periods]

        ou = []
        if org_unit_groups:
            ou += [f"OU_GROUP-{group}" for group in org_unit_groups]
        if org_unit_levels:
            ou += [f"LEVEL-{level}" for level in org_unit_levels]
        if org_units:
            ou += org_units

        dimension = [f"dx:{';'.join(dx)}", f"ou:{';'.join(ou)}"]
        if pe:
            dimension.append(f"pe:{';'.join(pe)}")

        if include_cocs:
            dimension.append("co:")

        return dimension

    @staticmethod
    def merge_chunked_responses(responses: List[dict]) -> dict:
        """Merge responses from chunked requests."""
        headers = responses[0]["headers"]
        rows = []
        for response in responses:
            rows += response["rows"]
        return {"headers": headers, "rows": rows}

    @staticmethod
    def to_data_values(response: dict) -> List[dict]:
        """Transform JSON response into data values.

        JSON response from Analytics endpoint is a structure with two keys: headers and
        rows. This function transforms it into the data values format used by the
        dataValueSets endpoint, so that it can easily be converted into a pandas or
        polars dataframe.
        """
        data_values = []
        for row in response["rows"]:
            data_value = {}
            for i, header in enumerate(response["headers"]):
                data_value[header["name"]] = row[i]
            data_values.append(data_value)
        return data_values

    def get(
        self,
        data_elements: List[str] = None,
        data_element_groups: List[str] = None,
        indicators: List[str] = None,
        indicator_groups: List[str] = None,
        periods: List[Union[str, Period]] = None,
        org_units: List[str] = None,
        org_unit_groups: List[str] = None,
        org_unit_levels: List[int] = None,
        include_cocs: bool = True,
    ) -> List[dict]:
        """Get requested data values using the Analytics API endpoint.

        If a large number of data elements, indicators, org units or periods are
        requested by the user, the request will automatically be divided into multiple
        chunks and merged before being returned.

        Parameters
        ----------
        data_elements : list of str, optional
            Data element identifiers
        data_element_groups : str, optional
            Data element groups identifiers
        indicators : list of str, optional
            Indicator identifiers
        indicator_groups : list of str, optional
            Indicator groups indifiers
        periods : list of str or Period, optional
            Period identifiers in DHIS2 format
        org_units : list of str, optional
            Organisation units identifiers
        org_unit_groups : list of str, optional
            Organisation unit groups identifiers
        org_unit_levels : list of int, optional
            Organisation unit levels
        include_cocs : bool, optional (default=True)
            Include category option combos in response

        Return
        ------
        list of dict
            Data values
        """
        what = data_elements or data_element_groups or indicators or indicator_groups
        where = org_units or org_unit_groups or org_unit_levels
        when = bool(periods)
        if not what:
            raise DHIS2Error("No data dimension provided")
        if not where:
            raise DHIS2Error("No spatial dimension provided")
        if not when:
            raise DHIS2Error("No temporal dimension provided")

        if all([isinstance(pe, Period) for pe in periods]):
            # convert Period objects to ISO strings
            periods = [str(pe) for pe in periods]
        elif not all([isinstance(pe, str) for pe in periods]):
            raise ValueError("Mixed period types")

        dimension = self.format_dimension_param(
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

        params = {"dimension": dimension, "paging": True, "ignoreLimit": True}
        params = self.split_params(params)

        responses = []
        for chunk in params:
            pages = [p for p in self.client.api.get_paged("analytics", params=chunk)]
            response = self.client.api.merge_pages(pages)
            responses.append(response)

        merged_response = self.merge_chunked_responses(responses)
        data_values = self.to_data_values(merged_response)
        return data_values


class Tracker:
    def __init__(self, client: DHIS2):
        self.client = client

    def get(self):
        pass

    def post(self):
        pass


def _get_dataframe_frmt(dataframe: Union[pl.DataFrame, pd.DataFrame]):
    if isinstance(dataframe, pl.DataFrame):
        return "polars"
    elif isinstance(dataframe, pd.DataFrame):
        return "pandas"
    else:
        raise ValueError("Unrecognized dataframe format")
