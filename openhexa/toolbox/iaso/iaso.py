import io
import typing

from openhexa.toolbox.iaso.api_client import ApiClient
import polars as pl


class IASO:
    """
    The IASO toolbox provides an interface to interact with the IASO.
    """

    def __init__(self, client: ApiClient) -> None:
        """
        Initializes the IASO toolbox.
        :param client: The IASO toolbox client.

        Examples:
            >>> from openhexa.toolbox.iaso import IASO
            >>> iaso = IASO(client=ApiClient(url="http://iaso-staging.bluesquare.org",
            >>>             username="user",
            >>>             password="pass"))
        """
        self.api_client = client

    def get_projects(self, page: int = 0, limit: int = 10, **kwargs) -> dict:
        """
        Fetches projects list from IASO. Method is paginated by default. Pagination can be modified and additional
        arguments can be passed as key value parameters.

        Examples:
            >>> from openhexa.toolbox.iaso import IASO
            >>> iaso = IASO(client=ApiClient(url="http://iaso-staging.bluesquare.org",
            >>>             username="user",
            >>>             password="pass"))
            >>> iaso.get_projects(page=1, limit=1, id=1)
        """

        params = kwargs
        params.update({"page": page, "limit": limit})
        response = self.api_client.request("GET", "/api/projects", params=params)
        return response.json().get("projects")

    def get_org_units(self, page: int = 0, limit: int = 10, **kwargs) -> dict:
        """
        Fetches org units from IASO. Method is paginated by default. Pagination can be modified and additional
        arguments can be passed as key value parameters.

        Examples:
            >>> from openhexa.toolbox.iaso import IASO
            >>> iaso = IASO(client=ApiClient(url="http://iaso-staging.bluesquare.org",
            >>>             username="user",
            >>>             password="pass"))
            >>> projects = iaso.get_org_units(page=1, limit=1, id=1)
        """
        params = kwargs
        params.update({"page": page, "limit": limit})
        response = self.api_client.request("GET", "/api/orgunits", params=kwargs)
        return response.json().get("orgUnits")

    def get_for_forms(
        self, page: int = 0, limit: int = 10, as_dataframe: bool = False, **kwargs
    ) -> dict | pl.DataFrame:
        """
        Fetches form submissions from IASO filtered only by URL paramters. Method is paginated by default.
        Pagination can be modified and additional arguments can be passed as key value parameters.
        There is a possiblity to fetch forms directly as DataFrames

        Params:
            :param page: The page number of the form submission.
            :param limit: The maximum number of form submissions.
            :param as_dataframe: If true, will return a DataFrame containing form submissions.
            :param kwargs: additonal arguments passed to the /forms endpoint as URL parameters.

        Examples:
            >>> from openhexa.toolbox.iaso import IASO
            >>> iaso = IASO(client=ApiClient(url="http://iaso-staging.bluesquare.org",
            >>>             username="user",
            >>>             password="pass"))
            >>> form_dataframes = iaso.get_for_forms(page=1, limit=1, as_dataframe=True, ids="1,2,3")
        """

        params = kwargs
        params.update({"page": page, "limit": limit})
        if as_dataframe:
            params.update({"csv": "true"})
            response = self.api_client.request("GET", "/api/forms", params=params)
            forms = pl.read_csv(io.StringIO(response.content.decode("utf-8")))
            return forms
        response = self.api_client.request("GET", "/api/forms/", params=kwargs)
        forms = response.json().get("forms")
        return forms

    def post_for_forms(
        self,
            org_units: typing.List[int],
            projects: typing.List[int],
            page: int = 0, limit: int = 10, **kwargs) -> dict:
        """
        Fetches form submissions from IASO filtered by POST form. Method is paginated by default.
        Pagination can be modified and additional arguments can be passed as key value parameters.

        Params:
            :param org_units: A required list of organization units IDs.
            :param projects: A required list of project IDs.
            :param page: The page number of the form submission.
            :param limit: The maximum number of form submissions.
            :param kwargs: additonal arguments passed to the /forms endpoint as URL parameters.

        Examples:
            >>> from openhexa.toolbox.iaso import IASO
            >>> iaso = IASO(client=ApiClient(url="http://iaso-staging.bluesquare.org",
            >>>             username="user",
            >>>             password="pass"))
            >>> forms_by_orgunits_and_projects = iaso.post_for_forms(page=1, limit=1, org_units=[300], projects=[23])
        """

        if org_units is [] or projects is []:
            raise ValueError("Values for org_units and projects cannot be empty lists")
        params = kwargs
        params.update({"page": page, "limit": limit, "org_units": org_units, "projects": projects})
        response = self.api_client.request("POST", "/api/forms", data=params)
        return response.json().get("forms")
