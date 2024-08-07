
# OpenHEXA Toolbox IASO

Module to fetch data from IASO 

* [Installation](#installation)
* [Usage](#usage)
	* [Connect to an instance](#connect-to-an-instance)
	* [Read data](#read-data)

## [Installation](#)

``` sh
pip install openhexa.toolbox
```

## [Usage](#)

### [Connect to an instance](#)
Credentials are required to initialize a connection to IASO instance. Credentials should contain the username and 
password to connect to an instance of IASO. You have as well to provide the host name to for the api to connect to:
* Staging environment https://iaso-staging.bluesquare.org/api
* Production environment https://iaso.bluesquare.org/api

Import IASO module as:
```
from openhexa.toolbox.iaso import IASO

iaso = IASO("https://iaso-staging.bluesquare.org","username", "password")
```

### [Read data](#)
After importing IASO module, you can use provided method to fetch Projects, Organisation Units and Forms that you have 
permissions for.  
```
# Fetch projects 
iaso.get_projects()
# Fetch organisation units 
iaso.get_org_units()
# Fetch submitted forms filtered by form_ids passed in url parameters and with choice to fetch them as dataframe
iaso.get_form_instances(page=1, limit=1, as_dataframe=True, 
	dataframe_columns=["Date de création","Date de modification","Org unit"], ids=276)
# Fetch forms filtered by organisaiton units and projects that you have permissions to
iaso.get_forms(org_units=[781], projects=[149])
```

You can as well provide additional parameters to the method to filter on desired values as key value arguments. 
You can have an overview on the arguments you can filter on API documentation of IASO. 



