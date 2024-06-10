import logging
from datetime import datetime, timezone

import requests
import jwt
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


class ApiClient(requests.Session):
    def __init__(self, server_url: str, username: str, password: str):
        super().__init__()
        self.server_url = server_url.rstrip("/")
        self.username = username
        self.password = password
        self.headers.update(
            {
                "User-Agent": "Openhexa-Toolbox",
            }
        )
        self._refresh_token = None
        self.token = None
        self.session = self.authenticate()

    def request(self, method, url, *args, **kwargs):
        full_url = f"{self.server_url}/{url.lstrip('/').rstrip('/')}/"
        try:
            resp = super().request(method, full_url, *args, **kwargs)
            self.raise_if_error(resp)
            return resp
        except requests.RequestException as exc:
            logging.exception(exc)
            raise

    def authenticate(self):
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
        return self

    def refresh_session(self):
        response = self.request("POST", "/api/token/refresh/", json={"refresh": self._refresh_token})
        self.token = response.json()["access"]
        self.headers.update({"Authorization": f"Bearer {self.token}"})

    def raise_if_error(self, response: requests.Response):
        if response.status_code >= 300 and "json" in response.headers.get("content-type", ""):
            raise IASOError(f"{response.json()}")
        if response.status_code == 401 and self._refresh_token:
            self.refresh_session()
        response.raise_for_status()

    def decode_token_expiry(self, token):
        decoded_token = jwt.decode(token, options= {"verify_signature":False})
        exp_timestamp = decoded_token.get("exp")
        if exp_timestamp:
            return datetime.fromtimestamp(exp_timestamp, timezone.utc)
        return None

