import logging
from typing import Iterable, Sequence

import requests

from openhexa.sdk.workspaces.connection import DHIS2Connection

logger = logging.getLogger(__name__)


class Api:
    def __init__(self, connection: DHIS2Connection):
        self.url = self.parse_api_url(connection.url)
        self.session = self.authenticate(connection.username, connection.password)

    @staticmethod
    def parse_api_url(url: str) -> str:
        """Ensure that API URL is correctly formatted."""
        url = url.rstrip("/")
        if "/api" not in url:
            url += "/api"
        return url

    def authenticate(self, username: str, password: str) -> requests.Session():
        """Authentify using Basic Authentication."""
        s = requests.Session()
        s.auth = requests.auth.HTTPBasicAuth(username, password)
        r = s.get(f"{self.url}/system/ping")
        r.raise_for_status()
        logger.info(f"Logged in to '{self.url}' as '{username}'")
        return s

    def get(self, endpoint: str, params: dict = None) -> requests.Response:
        r = self.session.get(f"{self.url}/{endpoint}", params=params)
        r.raise_for_status()
        return r

    def get_paged(
        self, endpoint: str, params: dict = None, page_size: 1000 = None
    ) -> Iterable[requests.Response]:
        """Iterate over all response pages."""
        if params is None:
            params = {}
        params["pageSize"] = page_size

        r = self.session.get(f"{self.url}/{endpoint}", params=params)
        r.raise_for_status()
        yield r

        if "pager" in r.json():
            while "nextPage" in r.json()["pager"]:
                r = self.session.get(r.json()["pager"]["nextPage"])
                yield r

    @staticmethod
    def merge_pages(pages: Sequence[requests.Response]) -> dict:
        """Merge lists from paged responses.

        The "pager" key in the response will be removed and all keys of type `list`
        (e.g. organisationUnits) will be merged into a single one.

        Parameters
        ----------
        pages : list of responses
            A list of paged responses, as returned by Api.get_paged()

        Return
        ------
        dict
            Merged response as a dict with merged lists
        """
        merged_response = {}
        first_page = pages[0].json()
        for key in first_page.keys():
            if isinstance(first_page[key], list):
                merged_response[key] = []
                for page in pages:
                    merged_response[key] += page.json()[key]
        return merged_response
