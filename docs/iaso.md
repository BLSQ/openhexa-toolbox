
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

### [Dataframe API](#)

The `dataframe` module provides a set of opinionated functions to extract data and metadata from an IASO account into Polars DataFrames.

#### [Get Organisation Units](#)

```python
from openhexa.toolbox.iaso import IASO
from openhexa.toolbox.iaso import dataframe

# Initialize IASO connection
iaso = IASO(connection)

# Get organisation units
df = dataframe.get_organisation_units(iaso)
```

Returns a DataFrame with columns:
- `id`: Organisation unit identifier
- `name`: Organisation unit name
- `short_name`: Organisation unit short name
- `level`: Organisation unit level in hierarchy
- `level_{n}_id`: ID of parent org unit at level n
- `level_{n}_name`: Name of parent org unit at level n
- `source`: Source system identifier
- `source_id`: Source system org unit ID
- `source_ref`: Source system reference
- `org_unit_type_id`: Organisation unit type ID
- `org_unit_type_name`: Organisation unit type name
- `created_at`: Creation timestamp
- `updated_at`: Last update timestamp
- `validation_status`: Validation status
- `opening_date`: Opening date
- `closed_date`: Closing date
- `geometry`: GeoJSON geometry string

## Get Form Metadata

```python
# Get form metadata
questions, choices = dataframe.get_form_metadata(iaso, form_id=123)
```

Parameters:
- `iaso`: IASO client instance
- `form_id`: Form identifier

Returns a tuple with:
1. `questions`: Dict with metadata for each question (name as key)
   - `name`: Question name
   - `type`: Question type
   - `label`: Question label
   - `list_name`: Choice list name (for select questions)
   - `calculate`: Calculate expression (for calculate questions)
2. `choices`: Dict with metadata for each choice list (list name as key)
   - Contains choice name and label for each option

## Extract Submissions

```python
# Extract form submissions
df = dataframe.extract_submissions(
    iaso,
    form_id=123,
    last_updated="2024-01-01"
)
```

Parameters:
- `iaso`: IASO client instance
- `form_id`: Form identifier
- `last_updated`: Optional ISO date string to filter by last update

Returns a DataFrame with:
- Standard columns:
  - `uuid`: Submission UUID
  - `id`: Submission ID
  - `form_id`: Form ID
  - `created_at`: Creation timestamp
  - `updated_at`: Last update timestamp
  - `org_unit_id`: Organisation unit ID
  - `org_unit_name`: Organisation unit name
  - `latitude`: Submission latitude
  - `longitude`: Submission longitude
- Dynamic columns:
  - One column per form question
  - Column types based on question types

## Replace Labels

```python
# Replace choice values with labels
df = dataframe.replace_labels(
    submissions=df,
    questions=questions,
    choices=choices,
    language="en"
)
```

Parameters:
- `submissions`: Submissions DataFrame
- `questions`: Questions metadata dict
- `choices`: Choices metadata dict  
- `language`: Optional language code for multi-language forms

Returns the submissions DataFrame with choice values replaced by their labels for:
- Single select questions
- Multiple select questions  
- Ranking questions

