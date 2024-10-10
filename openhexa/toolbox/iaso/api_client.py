import logging
from datetime import datetime, timezone
from typing import Union

import requests
import jwt
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class IASOError(Exception):
    """
    Base exception for IASO API errors.
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
        self.log_error()

    def __str__(self):
        return self.message

    def log_error(self):
        logging.error(f"IASO Error : {self.message}")


class ApiClient(requests.Session):
    """
    Client to manage HTTP session with IASO API on behalf of OpenHexa toolbox

    """

    def __init__(self, server_url: str, username: str, password: str):
        """
        Initialize the IASO API client.

        :param server_url: IASO server URL
        :param username: IASO instance username
        :param password: IASO instance password

        Examples:
            >>> client = ApiClient(server_url="http://localhost:8080", username="admin", password="<PASSWORD>")
        """
        super().__init__()
        self.server_url = server_url.rstrip("/")
        self.username = username
        self.password = password
        self.headers.update(
            {
                "User-Agent": "Openhexa-Toolbox",
            }
        )
        self.token = None
        self.token_expiry = None
        self._refresh_token = None
        self.authenticate()

    def request(self, method: str, url: str, *args, **kwargs) -> requests.Response:
        """
        Sends HTTP request to IASO API, handles exceptions raised during request
        """
        full_url = f"{self.server_url}/{url.strip('/')}/"
        try:
            resp = super().request(method, full_url, *args, **kwargs)
            self.raise_if_error(resp)
            return resp
        except requests.RequestException as exc:
            logging.exception(exc)
            raise

    def authenticate(self) -> None:
        """
        Authenticates with IASO API with username and password.
        Calling the endpoints to fetch authorization and refresh token.
        Ensures that failures are handles with status management, both with or without SSL communication
        """
        credentials = {"username": self.username, "password": self.password}
        response = self.request("POST", "/api/token/", json=credentials)
        json_data = response.json()
        self.token = json_data["access"]
        self.token_expiry = self.decode_token_expiry(self.token)
        self._refresh_token = json_data["refresh"]
        self.headers.update({"Authorization": f"Bearer {self.token}"})
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=5,
                allowed_methods=["HEAD", "GET"],
                status_forcelist=[429, 500, 502, 503, 504],
            )
        )
        self.mount("https://", adapter)
        self.mount("http://", adapter)

    def refresh_session(self) -> None:
        """
        Refreshes the session token by calling the refresh endpoint and updates the authentication token
        """
        response = self.request("POST", "/api/token/refresh/", json={"refresh": self._refresh_token})
        self.token = response.json()["access"]
        self.headers.update({"Authorization": f"Bearer {self.token}"})

    def raise_if_error(self, response: requests.Response) -> None:
        """
        Method to raise an exception if an error occurs during the request
        We raise a custom error if a JSON message is provided with an error

        :param response: the response object returned by the request
        """
        if response.status_code == 401 and self._refresh_token:
            self.refresh_session()
            return
        if response.status_code >= 300 and "json" in response.headers.get("content-type", ""):
            raise IASOError(f"{response.json()}")
        response.raise_for_status()

    @staticmethod
    def decode_token_expiry(token: str) -> Union[datetime, None]:
        """
        Decodes base64 encoded JWT token and returns expiry time from 'exp' field of the JWT token

        :param token: JWT token

        :return: Expiry datetime or None

        Examples:
        >>> decode_token_expiry(token = "eyJhbGciOiJIUzI1NiJ9.eyJSb2xlIjoiQWRtaW4iLCJJc3N1ZXIiOiJJc3N1ZXIiLCJVc2VybmFt\\
        ZSI6IkphdmFJblVzZSIsImV4cCI6MTcxNzY5MDEwNCwiaWF0IjoxNzE3NzYwMTA0fQ._pXcqDw0QgvznvNuhVPwYyIms3H5imH-q6A7lIQJjYQ")
        """
        decoded_token = jwt.decode(token, options={"verify_signature": False})
        exp_timestamp = decoded_token.get("exp")
        if exp_timestamp:
            return datetime.fromtimestamp(exp_timestamp, timezone.utc)
        return None
