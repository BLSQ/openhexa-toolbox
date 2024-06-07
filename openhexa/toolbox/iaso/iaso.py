import io
import typing

from openhexa.toolbox.iaso.api import IASOConnection, Api
import pandas as pd
from pandas import DataFrame

class IASO:

    def __init__(self, connection: IASOConnection):
        self.connection = connection
        self.api = Api(self.connection)

    def get_projects(self) -> dict:
        response = self.api.get("/api/projects")
        self.api.raise_if_error(response)
        return response.json().get("projects")


    def get_org_units(self) -> dict:
        response = self.api.get("/api/orgunits")
        self.api.raise_if_error(response)
        return response.json().get("orgUnits")

    def get_all_submissions_forms(self) -> dict:
        response = self.api.get("/api/forms")
        self.api.raise_if_error(response)
        forms = response.json().get("forms")
        return forms

    def get_submissions_forms(self, org_units=None, projects=None) -> dict:
        response = self.api.post("/api/forms",
                                  data={"org_units": org_units or [],
                                        "projects": projects or []
                                        })
        self.api.raise_if_error(response)
        return response.json().get("forms")

    def get_submission_form_in_csv(self, form_id: str) -> DataFrame:
        get_forms_url = f"/api/instances/?form_ids={form_id}&csv=true"
        response = self.api.get(get_forms_url)
        self.api.raise_if_error(response)
        forms = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        return forms

    def get_new_submission_forms_as_dataf(self, form_id: str, treated_forms: DataFrame) -> DataFrame:
        get_forms_url = f"/api/instances/?form_ids={form_id}&csv=true"
        response = self.api.get(get_forms_url)
        self.api.raise_if_error(response)
        forms = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        new_forms = forms[~forms.instanceID.isin(treated_forms.instanceID.unique())]
        return new_forms
