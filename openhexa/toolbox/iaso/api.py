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
    token: str
    refresh_token: str

    def __init__(self, connection: IASOConnection):
        self.connection = connection
        self.session = self.authenticate()

    def get(self, endpoint: str) -> requests.Response:
        parsed_url = self.parse_api_url(self.connection.url + endpoint)
        return self.session.get(parsed_url)

    def post(self, endpoint: str, data) -> requests.Response:
        parsed_url = self.parse_api_url(self.connection.url + endpoint)
        return self.session.post(parsed_url, data=data)

    def authenticate(self):
        credentials = {"username": self.connection.username, "password": self.connection.password}
        response = requests.post(self.connection.url + "/api/token/", json=credentials)
        self.raise_if_error(response)
        session = requests.Session()
        self.token = response.json()["access"]
        self.refresh_token = response.json()["refresh"]
        headers = {"Authorization": f"Bearer {self.token}", "User-Agent": "openhexa-toolbox"}
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
        """Raise IASOError with message provided by API."""
        if response.status_code != 200 and "json" in response.headers["content-type"]:
            msg = response.json()
            if msg.get("status") == "ERROR":
                raise IASOError(f"{msg.get('status')} {msg.get('httpStatusCode')}: {msg.get('message')}")

        # raise with requests if no error message provided
        response.raise_for_status()

