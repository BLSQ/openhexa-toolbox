
# OpenHEXA Toolbox OpenHEXAClient

The OpenHEXAClient module enables users to interact with the OpenHEXA backend using GraphQL syntax. 
Its primary goal is to simplify communication with OpenHEXA and streamline integration with third-party applications.

* [Installation](#installation)
* [Example](#example)


## [Installation](#)

``` sh
pip install openhexa.toolbox
```

## [Example](#)

Import OpenHEXA module:
```python
import json
from openhexa.toolbox.hexa import OpenHEXA
# We can authenticate using username / password
hexa_client = OpenHEXA("https://app.demo.openhexa.org", username="username",  password="password")

# You can also use the token provided by OpenHEXA on the pipelines 
hexa_client = OpenHEXA("https://app.demo.openhexa.org", token="token")
page=1,per_page=10

workspaces = hexa.query("""
    query {
        workspaces (page: $page, perPage: $perPage) {
            items {
                slug
                name
            }
        }
    }
""", {"page":page,"perPage":per_page})["workspaces"]["items"]
print("Workspaces:")
print(json.dumps(workspaces, indent=2))
```



