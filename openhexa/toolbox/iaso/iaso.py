import io

from openhexa.toolbox.iaso.api import ApiClient
import polars as pl


class IASO:
    def __init__(
        self, client : ApiClient
    ) -> None:
        self.api_client = client

    def get_projects(self) -> dict:
        response = self.api_client.request("GET", "/api/projects")
        return response.json().get("projects")

    def get_org_units(self) -> dict:
        response = self.api_client.request("GET", "/api/orgunits")
        return response.json().get("orgUnits")

    def get_all_submissions_forms(self) -> dict:
        response = self.api_client.request("GET", "/api/forms")
        forms = response.json().get("forms")
        return forms

    def get_submissions_forms(self, org_units=None, projects=None, **kwargs) -> dict:
        response = self.api_client.request(
            "POST", "/api/forms", data={"org_units": org_units or [], "projects": projects or []}
        )
        return response.json().get("forms")

    def get_submission_form_as_dataframe(self, form_ids: str) -> pl.DataFrame:
        get_forms_url = f"/api/instances/?form_ids={form_ids}&csv=true"
        response = self.api_client.request("GET", get_forms_url)
        forms = pl.read_csv(io.StringIO(response.content.decode("utf-8")))
        return forms
