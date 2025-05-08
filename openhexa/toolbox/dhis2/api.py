import json
import logging
import zlib
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, Optional, Protocol, Sequence, Union
from urllib.parse import urlparse

import requests
from diskcache import DEFAULT_SETTINGS, Cache
from humanize import naturalsize
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger(__name__)


class DHIS2ToolboxError(Exception):
    """Base class for all exceptions raised by the DHIS2 Toolbox."""

    pass


class DHIS2ApiError(Exception):
    """Base class for all exceptions raised by the DHIS2 API."""

    pass


class DHIS2Connection(Protocol):
    username: str
    password: str
    url: str


class Api:
    def __init__(self, connection: DHIS2Connection = None, cache_dir: Optional[Union[Path, str]] = None, **kwargs):
        self.session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=5,
                allowed_methods=["HEAD", "GET", "POST"],
                status_forcelist=[409, 429, 500, 502, 503, 504],
            )
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        if connection is None and ("url" not in kwargs or "username" not in kwargs or "password" not in kwargs):
            raise DHIS2ToolboxError("Connection or url, username and password must be provided")

        if connection:
            self.url = self.parse_api_url(connection.url)
            self.session = self.authenticate(connection.username, connection.password)
        else:
            self.url = self.parse_api_url(kwargs["url"])
            self.session = self.authenticate(kwargs["username"], kwargs["password"])

        username = connection.username if connection else kwargs["username"]
        logger.info(f"Using API URL {self.url} with user {username}")

        self.cache = None
        if cache_dir:
            self.cache = ApiCache(cache_dir, self.url)

        self.PAGE_SIZE = 1000

        self.DEFAULT_EXPIRE_TIME = 86400
        self.EXPIRE_TIMES = {
            "dataValueSets": 604800,
            "analytics": 604800,
            "system": 60,
        }

    @staticmethod
    def parse_api_url(url: str) -> str:
        """Ensure that API URL is correctly formatted."""
        url = url.rstrip("/")
        if "/api" not in url:
            url += "/api"
        return url

    @staticmethod
    def raise_if_error(response: requests.Response):
        """Raise DHIS2Error with message provided by API."""
        # raise DHIS2 error if error message is provided
        if response.status_code != 200 and "json" in response.headers["content-type"]:
            msg = response.json()
            if msg.get("status") == "ERROR":
                full_error_msg = f"Error {msg.get('httpStatusCode')}: {msg.get('message')}"
                logger.error(full_error_msg)
                raise DHIS2ApiError(full_error_msg)

        # raise with requests if no error message provided
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.exception(f"HTTP error: {e}")
            raise

    def authenticate(self, username: str, password: str) -> requests.Session:
        """Authentify using Basic Authentication."""
        s = requests.Session()
        s.auth = requests.auth.HTTPBasicAuth(username, password)
        return s

    def get(self, endpoint: str, params: dict = None, use_cache: bool = True) -> dict:
        """Send GET request and return JSON response as a dict."""
        r = requests.Request(method="GET", url=f"{self.url}/{endpoint}", params=params)
        url = r.prepare().url
        logger.debug(f"GET {url}")

        use_cache = self.cache and use_cache

        if use_cache:
            r = self.cache.get(endpoint=endpoint, params=params)
            if r:
                logger.debug("Cache hit, returning cached response")
                return r

        r = self.session.get(f"{self.url}/{endpoint}", params=params)
        self.raise_if_error(r)

        if use_cache:
            logger.debug("Cache miss, caching response")
            self.cache.set(endpoint=endpoint, params=params, response=r.json())

        logger.debug(f"Successful request of size {naturalsize(len(r.content))}")

        return r.json()

    def get_paged(self, endpoint: str, params: dict = None, use_cache: bool = True) -> Iterable[requests.Response]:
        """Iterate over paged responses."""
        use_cache = self.cache and use_cache

        if not params:
            params = {}
        params["pageSize"] = self.PAGE_SIZE

        if "tracker" in endpoint:
            params["page"] = 1
            params["totalPages"] = True

        # 1st page
        r = self.get(endpoint=endpoint, params=params, use_cache=use_cache)
        yield r

        if "pager" in r:
            logger.debug(f"Pager found, using page size {params['pageSize']}")
            params["page"] = r["pager"]["page"]
            while "nextPage" in r["pager"]:
                params["page"] += 1
                r = self.get(endpoint=endpoint, params=params, use_cache=use_cache)
                yield r

        # Tracker API do not have any pager
        # instead, check for a pageCount key and use that to check if there are more pages
        elif "pageCount" in r:
            logger.debug(f"Page count found, using page size {params['pageSize']}")
            page_count = r["pageCount"]
            while params["page"] < page_count:
                params["page"] += 1
                r = self.get(endpoint=endpoint, params=params, use_cache=use_cache)
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
        n = 0
        merged_response = {}
        first_page = pages[0]
        for key in first_page.keys():
            if isinstance(first_page[key], list):
                merged_response[key] = []
                for i, page in enumerate(pages):
                    merged_response[key] += page[key]
                    n += 1
        logger.debug(f"Merged {n} pages")
        return merged_response

    def post(self, endpoint: str, json: dict = None, params: dict = None) -> requests.Response:
        r = requests.Request(method="POST", url=f"{self.url}/{endpoint}", json=json, params=params)
        url = r.prepare().url
        logger.debug(f"POST {url}")
        r = self.session.post(f"{self.url}/{endpoint}", json=json, params=params)
        self.raise_if_error(r)
        return r

    def put(self, endpoint: str, json: dict = None, params: dict = None) -> requests.Response:
        r = requests.Request(method="PUT", url=f"{self.url}/{endpoint}", json=json, params=params)
        url = r.prepare().url
        logger.debug(f"PUT {url}")
        r = self.session.put(f"{self.url}/{endpoint}", json=json, params=params)
        self.raise_if_error(r)
        return r


class ApiCache:
    def __init__(self, cache_dir: Path, api_url: str):
        """Cache API requests with diskcache."""
        self.dir = cache_dir / urlparse(api_url).netloc
        self.setup()
        self.api_url = api_url

        self.DEFAULT_EXPIRE_TIME = 86400
        self.EXPIRE_TIMES = {"dataValueSets": 604800, "analytics": 604800, "system": 60}
        self.SETTINGS = DEFAULT_SETTINGS
        self.SETTINGS["size_limit"] = 10000000000

    def setup(self):
        """Setup cache directory.

        Diskcache directory will be a subdir in the provided cache_dir,
        named after the DHIS2 instance domain name.
        """
        self.dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Using cache directory {self.dir.absolute().as_posix()}")

    def expire(self):
        """Remove expired items from cache."""
        with Cache(self.dir, **self.SETTINGS) as cache:
            n_items = cache.expire()
            logger.debug(f"Expired {n_items} items from cache")

    def clear(self):
        """Remove all items from cache."""
        with Cache(self.dir, **self.SETTINGS) as cache:
            n_items = cache.clear()
            logger.debug(f"Cleared {n_items} items from cache")

    def get_key(self, endpoint: str, params: Optional[dict]) -> str:
        """Generate cache key from API endpoint and query parameters."""
        if params:
            params = OrderedDict(sorted(params.items()))
            return f"{self.api_url}/{endpoint}/{json.dumps(params)}"
        else:
            return f"{self.api_url}/{endpoint}"

    def get(self, endpoint: str, params: Optional[dict]) -> Union[dict, None]:
        """Get JSON query response from cache as a dict."""
        key = self.get_key(endpoint=endpoint, params=params)
        with Cache(self.dir, **self.SETTINGS) as cache:
            content = cache.get(key)
            if content:
                logger.debug("Cache hit, returning decompressed response")
                return json.loads(zlib.decompress(content).decode())
        return None

    def set(self, endpoint: str, response: dict, params: Optional[dict]):
        """Cache JSON query response."""
        key = self.get_key(endpoint=endpoint, params=params)
        with Cache(self.dir, **self.SETTINGS) as cache:
            value = zlib.compress(json.dumps(response).encode())
            cache.set(key, value, expire=self.EXPIRE_TIMES.get("endpoint", self.DEFAULT_EXPIRE_TIME), retry=True)
