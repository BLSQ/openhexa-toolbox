from requests import Session
import typing


class NotFound(Exception):
    """Errors related to an element not found."""

    pass


class OpenHEXAClient:
    def __init__(self, base_url):
        self.url = base_url.rstrip("/")
        self.session = Session()
        self.session.headers.update({"Content-Type": "application/json", "User-Agent": "OpenHEXA Python Client"})

    def authenticate(
        self,
        with_credentials: typing.Optional[tuple[str, str]] = None,
        with_token: typing.Optional[str] = None,
    ):
        """
        with_credentials: tuple of email and password
        with_token: JWT token
        """
        if with_credentials:
            resp = self._graphql_request(
                """
                mutation Login($input: LoginInput!) {
                    login(input: $input) {
                        success
                    }
                }
            """,
                {
                    "input": {
                        "email": with_credentials[0],
                        "password": with_credentials[1],
                    }
                },
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            if data["login"]["success"]:
                self.session.headers["Cookie"] = resp.headers["Set-Cookie"]
            else:
                raise Exception("Login failed : verify if two-factor authentication is not enabled.")
        elif with_token:
            self.session.headers.update({"Authorization": f"Bearer {with_token}"})
        try:
            self.query("""query{me {user {id}}}""")
            return True
        except Exception:
            raise Exception("Authentication failed.")

    def _graphql_request(self, operation, variables=None):
        return self.session.post(f"{self.url}/graphql", json={"query": operation, "variables": variables})

    def query(self, operation, variables=None):
        resp = self._graphql_request(operation, variables)
        if resp.status_code == 400:
            raise Exception(resp.json()["errors"][0]["message"])
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("errors"):
            raise Exception(payload["errors"])
        return payload["data"]
