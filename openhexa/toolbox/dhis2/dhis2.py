import json
import os
from functools import wraps
from typing import Callable, List
from urllib.parse import urlparse
import logging
import datetime
import itertools

from diskcache import Cache

from openhexa.sdk.workspaces.connection import DHIS2Connection

from .api import Api, DHIS2Error

logger = logging.getLogger(__name__)


def use_cache(key: str):
    """Use sqlite-based diskcache.

    Return json response as a dict if cache is hit. If not, store the response in cache.
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not args[0].client.cache_dir:
                return func(*args, **kwargs)
            else:
                with Cache(args[0].client.cache_dir) as cache:
                    if key in cache:
                        return json.loads(cache.get(key))
                    else:
                        value = func(*args, **kwargs)
                        cache.set(key, json.dumps(value))
                        return value

        return wrapper

    return decorator


class DHIS2:
    def __init__(self, connection: DHIS2Connection, cache_dir: str = None):
        """Initialize a new DHIS2 instance.

        Parameters
        ----------
        connection : openhexa DHIS2Connection
            An initialized openhexa dhis2 connection
        cache_dir : str, optional
            Cache directory. Actual cache data will be stored under a sub-directory
            named after the DHIS2 instance domain.
        """
        self.api = Api(connection)
        self.info = self.system_info()
        self.version = self.info.get("version")
        self.cache_dir = self.setup_cache(cache_dir)
        self.meta = Metadata(self)
        self.data_value_sets = DataValueSets(self)

    def setup_cache(self, cache_dir: str):
        """Initialize diskcache."""
        cache_dir = os.path.join(cache_dir, urlparse(self.api.url).netloc)
        os.makedirs(cache_dir, exist_ok=True)
        return cache_dir

    @use_cache("system-info")
    def system_info(self) -> dict:
        """Get information on the current system."""
        r = self.api.get("system/info")
        return r.json()


class Metadata:
    def __init__(self, client: DHIS2):
        """Methods for accessing metadata API endpoints."""
        self.client = client

    @use_cache("organisation_unit_levels")
    def organisation_unit_levels(self) -> List[dict]:
        """Get names of all organisation unit levels.

        Return
        ------
        list of dict
            Id, number and name of each org unit level.
        """
        r = self.client.api.get("filledOrganisationUnitLevels")
        levels = []
        for level in r.json():
            levels.append(
                {
                    "id": level.get("id"),
                    "level": level.get("level"),
                    "name": level.get("name"),
                }
            )
        return levels

    @use_cache("organisation_units")
    def organisation_units(self) -> List[dict]:
        """Get organisation units metadata.

        Return
        ------
        list of dict
            Id, name, level, path and geometry of all org units.
        """
        org_units = []
        for page in self.client.api.get_paged(
            "organisationUnits",
            params={"fields": "id,name,level,path,geometry"},
            page_size=1000,
        ):
            page = page.json()
            for ou in page["organisationUnits"]:
                org_units.append(
                    {
                        "id": ou.get("id"),
                        "name": ou.get("name"),
                        "level": ou.get("level"),
                        "path": ou.get("path"),
                        "geometry": json.dumps(ou.get("geometry"))
                        if ou.get("geometry")
                        else None,
                    }
                )
        return org_units

    @use_cache("organisation_unit_groups")
    def organisation_unit_groups(self) -> List[dict]:
        """Get organisation unit groups metadata.

        Return
        ------
        list of dict
            Id, name, and org units of all org unit groups.
        """
        org_unit_groups = []
        for page in self.client.api.get_paged(
            "organisationUnitGroups",
            params={"fields": "id,name,organisationUnits"},
            page_size=50,
        ):
            groups = []
            for group in page.json().get("organisationUnitGroups"):
                groups.append(
                    {
                        "id": group.get("id"),
                        "name": group.get("name"),
                        "organisation_units": [
                            ou.get("id") for ou in group["organisationUnits"]
                        ],
                    }
                )
            org_unit_groups += groups
        return groups

    @use_cache("datasets")
    def datasets(self) -> List[dict]:
        """Get datasets metadata.

        Return
        ------
        list of dict
            Id, name, data elements, indicators and org units of all datasets.
        """
        datasets = []
        for page in self.client.api.get_paged(
            "dataSets",
            params={
                "fields": "id,name,dataSetElements,indicators,organisationUnits",
                "pageSize": 10,
            },
        ):
            for ds in page.json()["dataSets"]:
                row = {"id": ds.get("id"), "name": ds.get("name")}
                row["data_elements"] = [
                    dx["dataElement"]["id"] for dx in ds["dataSetElements"]
                ]
                row["indicators"] = [indicator["id"] for indicator in ds["indicators"]]
                row["organisation_units"] = [ou["id"] for ou in ds["organisationUnits"]]
                datasets.append(row)
        return datasets

    @use_cache("data_elements")
    def data_elements(self) -> List[dict]:
        """Get data elements metadata.

        Return
        ------
        list of dict
            Id, name, and aggregation type of all data elements.
        """
        elements = []
        for page in self.client.api.get_paged(
            "dataElements",
            params={"fields": "id,name,aggregationType,zeroIsSignificant"},
            page_size=1000,
        ):
            elements += page.json()["dataElements"]
        return elements

    @use_cache("data_element_groups")
    def data_element_groups(self) -> List[dict]:
        """Get data element groups metadata.

        Return
        ------
        list of dict
            Id, name and data elements of all data element groups.
        """
        de_groups = []
        for page in self.client.api.get_paged(
            "dataElementGroups",
            params={"fields": "id,name,dataElements"},
            page_size=50,
        ):
            groups = []
            for group in page.json().get("dataElementGroups"):
                groups.append(
                    {
                        "id": group.get("id"),
                        "name": group.get("name"),
                        "data_elements": [ou.get("id") for ou in group["dataElements"]],
                    }
                )
            de_groups += groups
        return de_groups

    @use_cache("category_option_combos")
    def category_option_combos(self) -> List[dict]:
        """Get category option combos metadata.

        Return
        ------
        list of dict
            Id and name of all category option combos.
        """
        combos = []
        for page in self.client.api.get_paged(
            "categoryOptionCombos", params={"fields": "id,name"}, page_size=1000
        ):
            combos += page.json().get("categoryOptionCombos")
        return combos

    @use_cache("indicators")
    def indicators(self) -> List[dict]:
        """Get indicators metadata.

        Return
        ------
        list of dict
            Id, name, numerator and denominator of all indicators.
        """
        indicators = []
        for page in self.client.api.get_paged(
            "indicators",
            params={"fields": "id,name,numerator,denominator"},
            page_size=1000,
        ):
            indicators += page.json()["indicators"]
        return indicators

    @use_cache("indicatorGroups")
    def indicator_groups(self) -> List[dict]:
        """Get indicator groups metadata.

        Return
        ------
        list of dict
            Id, name and indicators of all indicator groups.
        """
        ind_groups = []
        for page in self.client.api.get_paged(
            "indicatorGroups",
            params={"fields": "id,name,indicators"},
            page_size=50,
        ):
            groups = []
            for group in page.json().get("indicatorGroups"):
                groups.append(
                    {
                        "id": group.get("id"),
                        "name": group.get("name"),
                        "indicators": [ou.get("id") for ou in group["indicators"]],
                    }
                )
            ind_groups += groups


class DataValueSets:
    def __init__(self, client: DHIS2):
        """Methods for the dataValueSets API endpoint."""
        self.client = client
        self.MAX_DATA_ELEMENTS = 50
        self.MAX_ORG_UNITS = 50
        self.MAX_PERIODS = 1

    @staticmethod
    def split_list(src_list: list, length: int) -> List[list]:
        """Split list into chunks."""
        for i in range(0, len(src_list), length):
            yield src_list[i : i + length]

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
            *[
                self.split_list(params.get(param), max_length)
                for param, max_length in params_to_chunk
            ]
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
        periods: List[str] = None,
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
        periods : list of str, optional
            Period identifiers in ISO format
        start_date : str, optional
            Start date for the time span of the values to export
        end_date : str, optional
            End date for the time span of the values to export
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
            Response as a list of dict with data values.
        """
        what = data_elements or datasets or data_element_groups
        where = org_units or org_unit_groups
        when = (start_date and end_date) or periods
        if not what:
            raise DHIS2Error("No data dimension provided")
        if not where:
            raise DHIS2Error("No spatial dimension provided")
        if not when:
            raise DHIS2Error("No temporal dimension provided")

        if data_elements and not self.client.version >= 2.39:
            raise DHIS2Error(
                "Data elements parameter not supported for DHIS2 versions < 2.39"
            )

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
            "last_updated": last_updated,
            "last_updated_duration": last_updated_duration,
        }

        chunks = self.split_params(params)
        response = []
        for chunk in chunks:
            r = self.client.api.get("dataValueSets", params=chunk)
            response += r.json()["dataValues"]

        return response

    def post(
        self,
        data_values: List[dict],
        import_strategy: str = "CREATE",
        dry_run: bool = True,
    ):
        pass


class Analytics:
    def __init__(self, client: DHIS2):
        """Methods for the analytics API endpoint."""
        self.client = client

    def get(self):
        pass


class Tracker:
    def __init__(self, client: DHIS2):
        self.client = client

    def get(self):
        pass

    def post(self):
        pass
