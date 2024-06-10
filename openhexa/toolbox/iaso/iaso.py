import io

from openhexa.toolbox.iaso.api_client import ApiClient
import polars as pl


class IASO:
    def __init__(self, client: ApiClient) -> None:
        self.api_client = client

    def get_projects(self, **kwargs) -> dict:
        response = self.api_client.request("GET", "/api/projects", params=kwargs)
        return response.json().get("projects")

    def get_org_units(self, **kwargs) -> dict:
        response = self.api_client.request("GET", "/api/orgunits", params=kwargs)
        return response.json().get("orgUnits")

    def get_for_forms(self, as_dataframe: bool,  **kwargs) -> dict | pl.DataFrame:
        params = kwargs
        if as_dataframe:
                params.update({"csv": "true"})
                response = self.api_client.request("GET", "/api/forms", params=params)
                forms = pl.read_csv(io.StringIO(response.content.decode("utf-8")))
                return forms
        response = self.api_client.get(f"/api/forms/", params=kwargs)
        forms = response.json().get("forms")
        return forms

    def post_for_forms(self, org_units=None, projects=None) -> dict:
        response = self.api_client.request(
            "POST", "/api/forms", data={"org_units": org_units or [], "projects": projects or []}
        )
        return response.json().get("forms")

