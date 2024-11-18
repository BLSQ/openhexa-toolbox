
# OpenHEXA Toolbox OpenHEXAClient

The OpenHEXAClient module enables users to interact with the OpenHEXA backend using GraphQL syntax. 
Its primary goal is to simplify communication with OpenHEXA and streamline integration with third-party applications.

* [Installation](#installation)
* [Example](#example)


## [Installation](#)

``` sh
pip install openhexa.toolbox
```

Important : make sure two-factor authentication is disabled as the SDK currently does not support 
it during the authentication process.

## [Example](#)

Import OpenHEXA module:

```python
import json
from openhexa.toolbox.hexa import OpenHEXA
# We can authenticate using username / password
hexa = OpenHEXA("https://app.demo.openhexa.org", username="username",  password="password")

# You can also use the token provided by OpenHEXA on the pipelines page.
hexa = OpenHEXA("https://app.demo.openhexa.org", token="token")

# getting the list of workspaces
result = hexa.query("""
    query {
        workspaces(page: $page, perPage: $perPage) {
            items {
                slug
                name
            }
        }
    }
""", {"page": page, "perPage": per_page})

workspaces = result["workspaces"]["items"]

print("Workspaces:")
print(json.dumps(workspaces, indent=2))

# get a workspace connection 
connection = hexa.query(
    """
    query getConnection($workspaceSlug:String!, $connectionSlug: String!) {
        connectionBySlug(workspaceSlug:$workspaceSlug, connectionSlug: $connectionSlug) {
            type
            fields {
                code
                value
            }
        }
    }
    """,
    {"workspaceSlug": workspace_slug, "connectionSlug": connection_identifier},
)["connectionBySlug"]

print("Connection:")
print(json.dumps(connection, indent=2))

# get list of datasets
result = hexa.query(
    """
    query getWorkspaceDatasets($slug: String!, $page:Int, $perPage:Int) {
        workspace(slug: $slug) {
            datasets(page:$page, perPage:$perPage) {
                items { 
                    id
                    dataset {
                        id
                        slug
                        name
                        description
                    }
                }        
            }
        }
    }
    """,
    {"slug": workspace_slug, "page": page, "perPage": per_page})

datasets = result["workspace"]["datasets"]

print("Datasets:")
print(json.dumps(datasets, indent=2))
```



