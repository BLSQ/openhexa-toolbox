import dataclasses
import logging
import os

import requests
import stringcase
from requests.adapters import HTTPAdapter
from urllib3 import Retry

class IASOError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
        self.log_error()

    def __str__(self):
        return self.message

    def log_error(self):
        logging.error(f"IASO Error : {self.message}")



class ConnectionDoesNotExist(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
        self.log_error()
    def __str__(self):
        return self.message

    def log_error(self):
        logging.error(f"Connection Error : {self.message}")



@dataclasses.dataclass
class IASOConnection:
    """IASO connection.

    See https://github.com/BLSQ/iaso for more information.
    """

    url: str
    username: str
    password: str

    def __repr__(self):
        """Safe representation of the IASO connection (no credentials)."""
        return f"IASOConnection(url='{self.url}', username='{self.username}')"

    def __init__(self, identifier: str = None):
        """Get a IASO connection by identifier.

        Parameters
        ----------
        identifier : str
            The identifier of the connection in the OpenHEXA backend
        """
        identifier = identifier
        try:
            env_variable_prefix = stringcase.constcase(identifier.lower())
            self.url = os.environ[f"{env_variable_prefix}_URL"]
            self.username = os.environ[f"{env_variable_prefix}_USERNAME"]
            self.password = os.environ[f"{env_variable_prefix}_PASSWORD"]
        except KeyError:
            raise ConnectionDoesNotExist(f'No IASO connection for "{identifier}"')



class Api:
    connection: IASOConnection
    session: requests.Session

    def __init__(self, connection: IASOConnection):
        self.connection = connection
        self.authenticated_session = self.authenticate()

    def get(self, endpoint: str, params: dict = None) -> requests.Response:
        parsed_url = self.parse_api_url(endpoint)
        return self.session.get(parsed_url, params)


    def authenticate(self):
        credentials = {"username": self.username, "password": self.password}
        response = requests.post(self.server + "/api/token/", json=credentials)
        self.raise_if_error(response)
        session = requests.Session()
        headers = {"Authorization": f"Bearer {self.response.json().get("access")}"}
        session.headers.update(headers)
        self.session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=5,
                allowed_methods=["HEAD", "GET"],
                status_forcelist=[429, 500, 502, 503, 504],
            )
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        return session

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
        if response.status_code != 200 and "json" in response.headers["content-type"]:
            msg = response.json()
            if msg.get("status") == "ERROR":
                raise IASOError(f"{msg.get('status')} {msg.get('httpStatusCode')}: {msg.get('message')}")

        # raise with requests if no error message provided
        response.raise_for_status()

    def request(self, method, url, *args, **kwargs):
        full_url = f"{self.server_url}/{url.lstrip('/').rstrip('/')}/"
        try:
            resp = super().request(method, full_url, *args, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logging.exception(e)
            raise