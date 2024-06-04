import io

from openhexa.toolbox.iaso.api import IASOConnection, Api
import pandas as pd
from pandas import DataFrame

class IASO:

    def __init__(self, connection: IASOConnection):
        self.connection = connection
        self.api = Api(self.connection)

    def get_projects(self):
        response = self.api.get("/api/projects")
        return response

    def get_org_units(self):
        response = self.api.get("/api/orgunits")
        return response

    def get_submission_forms(self, form_id: str, treated_forms:DataFrame):
        get_forms_url = f"/api/instances/?form_ids={form_id}&csv=true"
        response = self.api.get(get_forms_url)
        forms = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        new_forms = forms[~forms.instanceID.isin(treated_forms.instanceID.unique())]
        return new_forms

