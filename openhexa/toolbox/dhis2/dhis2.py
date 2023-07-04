import json
import os
from functools import wraps
from typing import Callable, List
from urllib.parse import urlparse

from diskcache import Cache

from openhexa.sdk.workspaces.connection import DHIS2Connection

from .api import Api


def use_cache(key: str):
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not args[0].cache_dir:
                return func(*args, **kwargs)
            else:
                with Cache(args[0].cache_dir) as cache:
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
        self.api = Api(connection)
        self.cache_dir = self.setup_cache(cache_dir)

    def setup_cache(self, cache_dir: str):
        cache_dir = os.path.join(cache_dir, urlparse(self.api.url).netloc)
        os.makedirs(cache_dir, exist_ok=True)
        return cache_dir

    @use_cache("organisation_unit_levels")
    def organisation_unit_levels(self) -> List[dict]:
        r = self.api.get("filledOrganisationUnitLevels")
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
        org_units = []
        for page in self.api.get_paged(
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
        org_unit_groups = []
        for page in self.api.get_paged(
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
        datasets = []
        for page in self.api.get_paged(
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
        elements = []
        for page in self.api.get_paged(
            "dataElements",
            params={"fields": "id,name,aggregationType,zeroIsSignificant"},
            page_size=1000,
        ):
            elements += page.json()["dataElements"]
        return elements

    @use_cache("data_element_groups")
    def data_element_groups(self) -> List[dict]:
        de_groups = []
        for page in self.api.get_paged(
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
        combos = []
        for page in self.api.get_paged(
            "categoryOptionCombos", params={"fields": "id,name"}, page_size=1000
        ):
            combos += page.json().get("categoryOptionCombos")
        return combos

    @use_cache("indicators")
    def indicators(self) -> List[dict]:
        indicators = []
        for page in self.api.get_paged(
            "indicators",
            params={"fields": "id,name,numerator,denominator"},
            page_size=1000,
        ):
            indicators += page.json()["indicators"]
        return indicators

    @use_cache("indicatorGroups")
    def indicator_groups(self) -> List[dict]:
        ind_groups = []
        for page in self.api.get_paged(
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
                        "data_elements": [ou.get("id") for ou in group["indicators"]],
                    }
                )
            ind_groups += groups
