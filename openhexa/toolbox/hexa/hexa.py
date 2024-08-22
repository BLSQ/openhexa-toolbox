from .api import OpenHEXAClient, NotFound
import typing
import uuid


class OpenHEXA:
    def __init__(
        self,
        server_url: str,
        username: typing.Optional[str] = None,
        password: typing.Optional[str] = None,
        token: typing.Optional[str] = None,
    ):
        """
        Initializes the OpenHEXA client. If username and password are provided we will try to
        authenticate via credentials. It support also the authentication via pipeline token.
        Note that two-factor authentication needs to be disabled, otherwise the authentication will fail.

        Parameters
        ----------
        server_url: OpenHEXA server URL
        username: OpenHEXA instance username
        password: OpenHEXA instance password
        token: OpenHEXA pipeline token

        Raises
        ------
        Exception
            when a login or authentication error

        Examples:
            >>> from openhexa.toolbox.hexa import OpenHEXA
            >>> hexa = OpenHEXA(server_url="https://app.demo.openhexa.org",
            >>>             username="user",
            >>>             password="pass")

            or

            >>> from openhexa.toolbox.hexa import OpenHEXA
            >>> hexa = OpenHEXA(server_url="https://app.demo.openhexa.org",token="token")
        """

        self.client = OpenHEXAClient(server_url)
        if username and password:
            self.client.authenticate(with_credentials=(username, password))
        elif token:
            self.client.authenticate(with_token=token)
        else:
            raise Exception("Missing authentication credentials: username, password or token missing.")

    def query(self, operation, variables=None):
        """
        Executes a query operation using the client's API.

        This method sends a query request to the server using the specified operation and optional variables.
        The `operation` parameter defines the operation to be performed, typically in the form of a query or mutation.
        The `variables` parameter allows you to pass any required variables for the query.

        Parameters:
        ----------
        operation : str
            A GraphQL string representing the query or mutation operation to be executed.

        variables : dict | None, optional
            A dictionary of parameters to be used in the query / mutation operation.

        Returns:
        -------
        dict
            The query result from OpenHEXA server.

        Example Usage:
        --------------
        ```
        # Simple query without variables
        response = hexa.query(operation=\"""{ users { id name } }\""")

        # Query with variables
        operation = \"""
            mutation($input: RunPipelineInput!) {
            runPipeline(input: $input) {
                success
                errors
                run {
                    id
                }
            }
        }
        \"""
        variables = {"input": {"id": pipeline_id, "config": {}}}
        response = hexa.query(operation=operation, variables=variables)
        ```
        """
        return self.client.query(operation, variables)

    def get_workspaces(self, page: int = 1, per_page: int = 10) -> dict:
        """
        Fetches a paginated list of workspaces.


        Parameters:
        ----------
        page : int, optional
            The page number to retrieve. Defaults to 1.
        per_page : int, optional
            The number of items to retrieve per page. Defaults to 10.

        Returns:
        -------
        dict
            A dictionary containing the list of workspaces (`items`), the total number of items (`totalItems`),
            and the total number of pages (`totalPages`).
        """
        return self.query(
            """
           query($page: Int!, $perPage: Int!) {
              workspaces(page: $page, perPage: $perPage) {
                items {
                  slug
                  name
                }
                totalItems
                totalPages
              }
            }
        """,
            {"page": page, "perPage": per_page},
        )

    def get_pipelines(self, workspace_slug: str, page: int = 1, per_page: int = 10) -> dict:
        """
        Retrieves a paginated list of pipelines within a specified workspace.

        Parameters:
        ----------
        workspace_slug : str
            The slug identifier of the workspace to retrieve pipelines from.
        page : int, optional
            The page number to retrieve. Defaults to 1.
        per_page : int, optional
            The number of items to retrieve per page. Defaults to 10.

        Returns:
        -------
        dict
            A dictionary containing the list of pipelines (`items`), the total number of items (`totalItems`),
            and the total number of pages (`totalPages`).
        """
        return self.query(
            """
            query workspacePipeline($workspaceSlug: String!,$page:Int, $perPage:Int) {
                pipelines(workspaceSlug: $workspaceSlug, page:$page, perPage:$perPage) {
                    items {
                        id
                        name
                        code
                        type
                    }
                    totalItems
                    totalPages
                }
            }
        """,
            {"workspaceSlug": workspace_slug, "page": page, "perPage": per_page},
        )

    def get_pipeline(self, workspace_slug: str, code: str) -> dict:
        """
        Retrieves details of a specific pipeline by its code within a given workspace.

        Parameters:
        ----------
        workspace_slug : str
            The slug identifier of the workspace containing the pipeline.
        code : str
            The unique code of the pipeline to retrieve.

        Returns:
        -------
        dict
            A dictionary containing the pipeline details.

        Raises:
        ------
        NotFound
            If the pipeline with the specified code is not found in the given workspace.
        """
        pipeline = self.query(
            """
            query getPipeline($workspaceSlug:String!, $pipelineCode:String!){
                pipelineByCode(workspaceSlug: $workspaceSlug, code: $pipelineCode) {
                    id
                    code
                    name
                    currentVersion {
                        id
                    }
                    createdAt
                    updatedAt
                    type
                }
            }
        """,
            {"workspaceSlug": workspace_slug, "pipelineCode": code},
        )
        if not pipeline:
            raise NotFound(f"Pipeline {code} not found in workspace {workspace_slug}.")

        return pipeline

    def run_pipeline(
        self,
        id: uuid.UUID,
        config: typing.Optional[dict] = {},
        version_id: typing.Optional[uuid.UUID] = None,
        send_notification: typing.Optional[bool] = False,
    ) -> dict:
        """
        Executes a specified pipeline with optional configuration, version, and notification settings.

        Parameters:
        ----------
        id : str
            The unique identifier of the pipeline to run.
        config : dict, optional
            A dictionary of configuration settings to apply to the pipeline
        version_id : str, optional
            The specific version ID of the pipeline to run.
        send_notification : bool, optional
            Whether to send notification emails upon pipeline execution, false by default.

        Returns:
        -------
        dict
            A dictionary containing details of the pipeline run, including its ID.

        Raises:
        ------
        NotFound
            If the pipeline with the specified ID is not found.
        Exception
            If the pipeline run fails for any other reason, the error message is raised.
        """
        result = self.query(
            """
            mutation RunWorkspacePipeline($input: RunPipelineInput!) {
                runPipeline(input: $input) {
                    success
                    errors
                    run {
                        id
                    }
                }
            }
        """,
            {
                "input": {
                    "id": id,
                    "config": config,
                    "versionId": version_id,
                    "sendMailNotifications": send_notification,
                }
            },
        )

        if not result["runPipeline"]["success"]:
            if "PIPELINE_NOT_FOUND" in result["runPipeline"]["errors"]:
                raise NotFound(f"Pipeline {id} not found.")

            raise Exception(result["runPipeline"]["errors"])

        return result["runPipeline"]["run"]

    def get_pipeline_run(self, run_id: uuid.UUID):
        """
        Retrieves details of a specific pipeline run by its ID.

        Parameters:
        ----------
        run_id : str
            The unique identifier of the pipeline run to retrieve.

        Returns:
        -------
        dict
            A dictionary containing the pipeline run details.

        Raises:
        ------
        NotFound
            If the pipeline run with the specified ID is not found.
        """
        result = self.query(
            """
            query pipelineRun($runId: UUID!) {
                pipelineRun(id: $runId) {
                    id
                    version {
                        name
                    }
                    status
                    config
                    executionDate
                    duration
                    triggerMode
                    pipeline {
                        id
                    }
                }
            }
        """,
            {"runId": run_id},
        )
        if not result["pipelineRun"]:
            raise NotFound(f"Run {run_id}  not found.")

        return result["pipelineRun"]
