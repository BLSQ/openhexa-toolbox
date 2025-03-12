
# OpenHEXA Toolbox IASO

Extract data and metadata from a IASO account.

* [Installation](#installation)
* [Usage](#usage)
    * [Connect to an instance](#connect-to-an-instance)
    * [JSON API](#json-api)
        * [Projects](#projects)
        * [Organisation units](#organisation-units)
        * [Forms](#forms)
        * [Form instances](#form-instances)
    * [Dataframe API](#dataframe-api)
        * [Get organisation units](#get-organisation-units)
        * [Get form metadata](#get-form-metadata)
        * [Extract submissions](#extract-submissions)
        * [Replace labels](#replace-labels)

## [Installation](#)

``` sh
pip install openhexa.toolbox
```

## [Usage](#)

### [Connect to an instance](#)

Credentials are required to initialize a connection to a IASO instance. Credentials include the server URL, the username and the password.

Import IASO module as:

```
from openhexa.toolbox.iaso import IASO

iaso = IASO("https://iaso-staging.bluesquare.org","username", "password")
```

### [JSON API](#)

Once the connection is initialized, you can fetch projects, organisation units,
forms and form instances from the account as JSON-like dicts:

```
# Fetch projects 
iaso.get_projects()

# Fetch organisation units 
iaso.get_org_units()

# Fetch submitted forms filtered by form_ids passed in url parameters and with choice to fetch them as dataframe
iaso.get_form_instances(
  page=1,
  limit=1,
  ids=276
)

# Fetch forms filtered by organisation units and projects that you have permissions to
iaso.get_forms(
  org_units=[781],
  projects=[149]
)
```

Additional parameters can be passed as kwargs if needed. Please refer to the API
documentation of IASO for more details.

### [Dataframe API](#)

The `dataframe` module provides a set of opinionated functions to extract data
and metadata from an IASO account into Polars DataFrames.

#### [Get Organisation Units](#)

Extract the organisation unit hierarchy for a given IASO account. Returns a DataFrame with columns:
- `id`: Organisation unit identifier
- `name`: Organisation unit name
- `short_name`: Organisation unit short name
- `level`: Organisation unit level in hierarchy
- `level_{n}_id`: ID of parent org unit at level n
- `level_{n}_name`: Name of parent org unit at level n
- `source`: Data source
- `source_id`: ID in source
- `source_ref`: Org unit ID in source
- `org_unit_type_id`: Organisation unit type ID
- `org_unit_type_name`: Organisation unit type name
- `created_at`: Creation date
- `updated_at`: Last updated date
- `validation_status`: Validation status
- `opening_date`: Opening date
- `closed_date`: Closing date
- `geometry`: GeoJSON geometry string

```python
from openhexa.toolbox.iaso import IASO
from openhexa.toolbox.iaso import dataframe

# Initialize IASO connection
iaso = IASO(connection)

# Get organisation units
df = dataframe.get_organisation_units(iaso)
```

```
shape: (136, 23)
┌─────────┬────────────┬────────────┬───────┬────────────┬──────────────┬────────────┬───────────────────┬────────────┬──────────────┬────────────┬──────────────┬─────────┬───────────┬─────────────┬──────────────────┬─────────────────────────────────┬────────────────────────────┬────────────────────────────┬───────────────────┬──────────────┬─────────────────────┬─────────────────────────────────┐
│ id      ┆ name       ┆ short_name ┆ level ┆ level_1_id ┆ level_1_name ┆ level_2_id ┆ level_2_name      ┆ level_3_id ┆ level_3_name ┆ level_4_id ┆ level_4_name ┆ source  ┆ source_id ┆ source_ref  ┆ org_unit_type_id ┆ org_unit_type_name              ┆ created_at                 ┆ updated_at                 ┆ validation_status ┆ opening_date ┆ closed_date         ┆ geometry                        │
│ ---     ┆ ---        ┆ ---        ┆ ---   ┆ ---        ┆ ---          ┆ ---        ┆ ---               ┆ ---        ┆ ---          ┆ ---        ┆ ---          ┆ ---     ┆ ---       ┆ ---         ┆ ---              ┆ ---                             ┆ ---                        ┆ ---                        ┆ ---               ┆ ---          ┆ ---                 ┆ ---                             │
│ i64     ┆ str        ┆ str        ┆ u32   ┆ i64        ┆ str          ┆ i64        ┆ str               ┆ i64        ┆ str          ┆ i64        ┆ str          ┆ str     ┆ i64       ┆ str         ┆ i64              ┆ str                             ┆ datetime[μs]               ┆ datetime[μs]               ┆ str               ┆ datetime[μs] ┆ datetime[μs]        ┆ str                             │
╞═════════╪════════════╪════════════╪═══════╪════════════╪══════════════╪════════════╪═══════════════════╪════════════╪══════════════╪════════════╪══════════════╪═════════╪═══════════╪═════════════╪══════════════════╪═════════════════════════════════╪════════════════════════════╪════════════════════════════╪═══════════════════╪══════════════╪═════════════════════╪═════════════════════════════════╡
│ 2049004 ┆ Balave     ┆ Balave     ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048980    ┆ DS Solenzo   ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ UMJIrkSBNbU ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:40.949980 ┆ 2024-12-12 10:52:00.283874 ┆ VALID             ┆ null         ┆ 2024-12-01 00:00:00 ┆ {"type": "MultiPolygon", "coor… │
│ 2048991 ┆ Bana       ┆ Bana       ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048978    ┆ DS Boromo    ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ uQB7JiOOrjv ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:40.718413 ┆ 2024-09-26 12:21:49.526091 ┆ VALID             ┆ null         ┆ null                ┆ {"type": "MultiPolygon", "coor… │
│ 2049022 ┆ Barani     ┆ Barani     ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048983    ┆ DS Nouna     ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ NrG13X1XYqQ ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:41.182463 ┆ 2024-09-26 12:21:50.372129 ┆ VALID             ┆ null         ┆ null                ┆ {"type": "MultiPolygon", "coor… │
│ 2049020 ┆ Bomborokuy ┆ Bomborokuy ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048983    ┆ DS Nouna     ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ DHl6M4QkLwf ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:41.153741 ┆ 2024-09-26 12:21:50.309763 ┆ VALID             ┆ null         ┆ null                ┆ {"type": "MultiPolygon", "coor… │
│ 2049013 ┆ Bondokuy   ┆ Bondokuy   ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048981    ┆ DS Dedougou  ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ mj68YizDVF5 ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:41.079592 ┆ 2024-09-26 12:21:50.012641 ┆ VALID             ┆ null         ┆ null                ┆ {"type": "MultiPolygon", "coor… │
│ …       ┆ …          ┆ …          ┆ …     ┆ …          ┆ …            ┆ …          ┆ …                 ┆ …          ┆ …            ┆ …          ┆ …            ┆ …       ┆ …         ┆ …           ┆ …                ┆ …                               ┆ …                          ┆ …                          ┆ …                 ┆ …            ┆ …                   ┆ …                               │
│ 2049016 ┆ Toma       ┆ Toma       ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048982    ┆ DS Toma      ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ Uhsw8vIvsoC ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:41.108300 ┆ 2024-09-26 12:21:50.146649 ┆ VALID             ┆ null         ┆ null                ┆ {"type": "MultiPolygon", "coor… │
│ 2048999 ┆ Tougan     ┆ Tougan     ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048979    ┆ DS Tougan    ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ qX6MMu2XR7K ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:40.847298 ┆ 2024-09-26 12:21:49.695558 ┆ VALID             ┆ null         ┆ null                ┆ {"type": "MultiPolygon", "coor… │
│ 2049018 ┆ Yaba       ┆ Yaba       ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048982    ┆ DS Toma      ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ lHf8j547iaL ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:41.130610 ┆ 2024-09-26 12:21:50.213055 ┆ VALID             ┆ null         ┆ null                ┆ {"type": "MultiPolygon", "coor… │
│ 2048987 ┆ Yaho       ┆ Yaho       ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048978    ┆ DS Boromo    ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ Pzf416puQrn ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:40.668022 ┆ 2024-09-26 12:21:49.424601 ┆ VALID             ┆ null         ┆ null                ┆ {"type": "MultiPolygon", "coor… │
│ 2049017 ┆ Ye         ┆ Ye         ┆ 4     ┆ 2048976    ┆ Burkina Faso ┆ 2048977    ┆ Boucle du Mouhoun ┆ 2048982    ┆ DS Toma      ┆ null       ┆ null         ┆ wfdmqgv ┆ 251       ┆ caFiJxeH5k7 ┆ 1054             ┆ Health area/Aire de santé - AR… ┆ 2024-09-26 12:21:41.119577 ┆ 2024-09-26 12:21:50.179429 ┆ VALID             ┆ null         ┆ null                ┆ {"type": "MultiPolygon", "coor… │
└─────────┴────────────┴────────────┴───────┴────────────┴──────────────┴────────────┴───────────────────┴────────────┴──────────────┴────────────┴──────────────┴─────────┴───────────┴─────────────┴──────────────────┴─────────────────────────────────┴────────────────────────────┴────────────────────────────┴───────────────────┴──────────────┴─────────────────────┴─────────────────────────────────┘
```

## Get Form Metadata

Get metadata for a given form, including questions and choices.

Parameters:
- `iaso`: IASO client
- `form_id`: Form ID

Returns a tuple with:
1. `questions`: Dict with metadata for each question (name as key)
   - `name`: Question name
   - `type`: Question type
   - `label`: Question label
   - `list_name`: Choice list name (for select questions)
   - `calculate`: Calculate expression (for calculate questions)
2. `choices`: Dict with metadata for each choice list (list name as key)
   - Contains choice name and label for each option

```python
# Get form metadata
questions, choices = dataframe.get_form_metadata(iaso, form_id=123)
```

## Extract Submissions

Extract submissions for a given form.

Parameters:
- `iaso`: IASO client instance
- `form_id`: Form identifier
- `last_updated`: Optional ISO date string to filter by last update

Returns a DataFrame with:
- Standard columns:
  - `uuid`: Submission UUID
  - `id`: Submission ID
  - `form_id`: Form ID
  - `created_at`: Creation date
  - `updated_at`: Last updated date
  - `org_unit_id`: Organisation unit ID
  - `org_unit_name`: Organisation unit name
  - `latitude`: Submission latitude
  - `longitude`: Submission longitude
- Dynamic columns:
  - One column per form question (column type based on question type)

```python
# Extract form submissions
df = dataframe.extract_submissions(
    iaso,
    form_id=123,
    last_updated="2024-01-01"
)
```

```
shape: (18, 43)
┌─────────────────────────────────┬───────┬─────────┬────────────────────────────┬────────────────────────────┬─────────────┬───────────────────┬──────────┬───────────┬──────────────┬─────────────────┬────────────────┬────────────┬───┬────────────┬─────────────────────────────────┬─────────────────────────────────┬─────────────────────────────────┬────────────────┬────────────┬────────┬───────────────────────────────┬──────────────────┬─────────────────┬───────────────────┬─────────────────────────────────┐
│ uuid                            ┆ id    ┆ form_id ┆ created_at                 ┆ updated_at                 ┆ org_unit_id ┆ org_unit_name     ┆ latitude ┆ longitude ┆ presence_cdf ┆ type_equipement ┆ fabriquant     ┆ modele     ┆ … ┆ nbre_homme ┆ engagements_sensibilisation     ┆ engagements_rh                  ┆ engagements_materiel            ┆ lieu_rdv       ┆ date_rdv   ┆ imgUrl ┆ Population_estimee_du_village ┆ Nombre_de_menage ┆ Nombre_de_MILDA ┆ strategie         ┆ instanceID                      │
│ ---                             ┆ ---   ┆ ---     ┆ ---                        ┆ ---                        ┆ ---         ┆ ---               ┆ ---      ┆ ---       ┆ ---          ┆ ---             ┆ ---            ┆ ---        ┆   ┆ ---        ┆ ---                             ┆ ---                             ┆ ---                             ┆ ---            ┆ ---        ┆ ---    ┆ ---                           ┆ ---              ┆ ---             ┆ ---               ┆ ---                             │
│ str                             ┆ i64   ┆ i64     ┆ datetime[μs]               ┆ datetime[μs]               ┆ i64         ┆ str               ┆ f64      ┆ f64       ┆ str          ┆ str             ┆ str            ┆ str        ┆   ┆ i64        ┆ list[str]                       ┆ list[str]                       ┆ list[str]                       ┆ str            ┆ date       ┆ str    ┆ i64                           ┆ str              ┆ str             ┆ list[str]         ┆ str                             │
╞═════════════════════════════════╪═══════╪═════════╪════════════════════════════╪════════════════════════════╪═════════════╪═══════════════════╪══════════╪═══════════╪══════════════╪═════════════════╪════════════════╪════════════╪═══╪════════════╪═════════════════════════════════╪═════════════════════════════════╪═════════════════════════════════╪════════════════╪════════════╪════════╪═══════════════════════════════╪══════════════════╪═════════════════╪═══════════════════╪═════════════════════════════════╡
│ 9217bd69-89bf-452b-bff4-bf84fb… ┆ 43136 ┆ 505     ┆ 2024-09-26 12:22:49.938381 ┆ 2024-09-26 12:22:50.386852 ┆ 2048976     ┆ Burkina Faso      ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant 2   ┆ Modele 1   ┆ … ┆ 3          ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["rafra_chissement__eau__nourr… ┆ ecoles         ┆ 2024-09-01 ┆ null   ┆ 99                            ┆ null             ┆ null            ┆ ["1", "4"]        ┆ uuid:9217bd69-89bf-452b-bff4-b… │
│ 3b18c141-3119-4049-b1d1-aad2e9… ┆ 43137 ┆ 505     ┆ 2024-09-26 12:22:50.861463 ┆ 2024-09-26 12:22:51.256730 ┆ 2048978     ┆ DS Boromo         ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 11         ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["rafra_chissement__eau__nourr… ┆ ecoles         ┆ 2024-08-07 ┆ null   ┆ 58                            ┆ null             ┆ null            ┆ ["2", "3"]        ┆ uuid:3b18c141-3119-4049-b1d1-a… │
│ 2c77336a-7000-4747-aa6c-5daa12… ┆ 43138 ┆ 505     ┆ 2024-09-26 12:22:51.542564 ┆ 2024-09-26 12:22:52.053329 ┆ 2048981     ┆ DS Dedougou       ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant 3   ┆ Modele ... ┆ … ┆ 11         ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ lieux_de_culte ┆ 2024-08-22 ┆ null   ┆ 99                            ┆ null             ┆ null            ┆ ["1", "2", … "4"] ┆ uuid:2c77336a-7000-4747-aa6c-5… │
│ 127e80a4-6a1a-484c-badf-eb3520… ┆ 43139 ┆ 505     ┆ 2024-09-26 12:22:52.350867 ┆ 2024-09-26 12:22:52.739952 ┆ 2048983     ┆ DS Nouna          ┆ null     ┆ null      ┆ yes          ┆ congelateur     ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 7          ┆ ["mobilisation_de_la_communaut… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ chefferies     ┆ 2024-08-10 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["2", "3"]        ┆ uuid:127e80a4-6a1a-484c-badf-e… │
│ 8a8127b0-c2d7-4b38-b29e-3b1d00… ┆ 43140 ┆ 505     ┆ 2024-09-26 12:22:53.028916 ┆ 2024-09-26 12:22:53.378534 ┆ 2048980     ┆ DS Solenzo        ┆ null     ┆ null      ┆ yes          ┆ congelateur     ┆ Fabriquant 2   ┆ Modele 1   ┆ … ┆ 8          ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "autre_engagem… ┆ ["logistique_de_poste_de_vacci… ┆ ecoles         ┆ 2024-07-04 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["1", "3"]        ┆ uuid:8a8127b0-c2d7-4b38-b29e-3… │
│ …                               ┆ …     ┆ …       ┆ …                          ┆ …                          ┆ …           ┆ …                 ┆ …        ┆ …         ┆ …            ┆ …               ┆ …              ┆ …          ┆ … ┆ …          ┆ …                               ┆ …                               ┆ …                               ┆ …              ┆ …          ┆ …      ┆ …                             ┆ …                ┆ …               ┆ …                 ┆ …                               │
│ 99aea198-3239-4cb0-a342-fa6abc… ┆ 43149 ┆ 505     ┆ 2024-09-26 12:22:59.650837 ┆ 2024-09-26 12:23:00.192410 ┆ 2049021     ┆ Bourasso          ┆ null     ┆ null      ┆ yes          ┆ refrigerateur   ┆ Fabriquant ... ┆ Modele 2   ┆ … ┆ 7          ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ ecoles         ┆ 2024-08-07 ┆ null   ┆ 83                            ┆ null             ┆ null            ┆ ["1", "4"]        ┆ uuid:99aea198-3239-4cb0-a342-f… │
│ e8a2a327-0392-4a42-a6b8-3df475… ┆ 43150 ┆ 505     ┆ 2024-09-26 12:23:00.514687 ┆ 2024-09-26 12:23:00.902340 ┆ 2049011     ┆ Dedougou          ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant ... ┆ Modele ... ┆ … ┆ 11         ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ autres         ┆ 2024-08-13 ┆ null   ┆ 98                            ┆ null             ┆ null            ┆ ["1", "2", … "4"] ┆ uuid:e8a2a327-0392-4a42-a6b8-3… │
│ ea4e7b45-66d9-4c8f-b490-fc7017… ┆ 43151 ┆ 505     ┆ 2024-09-26 12:23:01.246092 ┆ 2024-09-26 12:23:01.783395 ┆ 2048996     ┆ Di                ┆ null     ┆ null      ┆ yes          ┆ congelateur     ┆ Fabriquant 2   ┆ Modele 3   ┆ … ┆ 3          ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ march_s        ┆ 2024-08-26 ┆ null   ┆ 70                            ┆ null             ┆ null            ┆ ["1", "2"]        ┆ uuid:ea4e7b45-66d9-4c8f-b490-f… │
│ 1285afda-3e1d-45e5-b4c4-5c0423… ┆ 43152 ┆ 505     ┆ 2024-09-26 12:23:02.205442 ┆ 2024-09-26 12:23:02.677701 ┆ 2049025     ┆ Djibasso          ┆ null     ┆ null      ┆ yes          ┆ refrigerateur   ┆ Fabriquant 1   ┆ Modele ... ┆ … ┆ 6          ┆ ["mobilisation_des_autres_lead… ┆ ["securisation_des_mobilisateu… ┆ ["rafra_chissement__eau__nourr… ┆ chefferies     ┆ 2024-06-18 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["1", "4"]        ┆ uuid:1285afda-3e1d-45e5-b4c4-5… │
│ 6024e8d9-50e9-495a-a80b-05eced… ┆ 43153 ┆ 505     ┆ 2024-09-26 12:23:03.161387 ┆ 2024-09-26 12:23:03.579861 ┆ 2048977     ┆ Boucle du Mouhoun ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 3          ┆ ["mobilisation_de_la_communaut… ┆ ["volontaires", "autre_engagem… ┆ ["logistique_de_poste_de_vacci… ┆ lieux_de_culte ┆ 2024-08-11 ┆ null   ┆ 73                            ┆ null             ┆ null            ┆ ["2", "3"]        ┆ uuid:6024e8d9-50e9-495a-a80b-0… │
└─────────────────────────────────┴───────┴─────────┴────────────────────────────┴────────────────────────────┴─────────────┴───────────────────┴──────────┴───────────┴──────────────┴─────────────────┴────────────────┴────────────┴───┴────────────┴─────────────────────────────────┴─────────────────────────────────┴─────────────────────────────────┴────────────────┴────────────┴────────┴───────────────────────────────┴──────────────────┴─────────────────┴───────────────────┴─────────────────────────────────┘
```

## Replace Labels

Replace choice values in submissions dataframe by choice labels in the selected language.

Parameters:
- `submissions`: Submissions dataframe (as returned by `extract_submissions()`)
- `questions`: Questions metadata dict (as returned by `get_form_metadata()`)
- `choices`: Choices metadata dict (as returned by `get_form_metadata()`)
- `language`: Optional language code for multi-language forms

Returns the submissions DataFrame with choice values replaced by their labels for:
- Single select questions
- Multiple select questions  
- Ranking questions

```python
# Get submissions
df = extract_submissions(iaso, form_id=505)

# Get form metadata
questions, choices = get_form_metadata(iaso, form_id=505)

# Replace choice values with labels
df = replace_labels(
    submissions=df,
    questions=questions,
    choices=choices,
    language="French"
)
```

```
shape: (18, 43)
┌─────────────────────────────────┬───────┬─────────┬────────────────────────────┬────────────────────────────┬─────────────┬───────────────────┬──────────┬───────────┬──────────────┬─────────────────┬────────────────┬────────────┬───┬────────────┬─────────────────────────────────┬─────────────────────────────────┬─────────────────────────────────┬────────────────┬────────────┬────────┬───────────────────────────────┬──────────────────┬─────────────────┬─────────────────────────────────┬─────────────────────────────────┐
│ uuid                            ┆ id    ┆ form_id ┆ created_at                 ┆ updated_at                 ┆ org_unit_id ┆ org_unit_name     ┆ latitude ┆ longitude ┆ presence_cdf ┆ type_equipement ┆ fabriquant     ┆ modele     ┆ … ┆ nbre_homme ┆ engagements_sensibilisation     ┆ engagements_rh                  ┆ engagements_materiel            ┆ lieu_rdv       ┆ date_rdv   ┆ imgUrl ┆ Population_estimee_du_village ┆ Nombre_de_menage ┆ Nombre_de_MILDA ┆ strategie                       ┆ instanceID                      │
│ ---                             ┆ ---   ┆ ---     ┆ ---                        ┆ ---                        ┆ ---         ┆ ---               ┆ ---      ┆ ---       ┆ ---          ┆ ---             ┆ ---            ┆ ---        ┆   ┆ ---        ┆ ---                             ┆ ---                             ┆ ---                             ┆ ---            ┆ ---        ┆ ---    ┆ ---                           ┆ ---              ┆ ---             ┆ ---                             ┆ ---                             │
│ str                             ┆ i64   ┆ i64     ┆ datetime[μs]               ┆ datetime[μs]               ┆ i64         ┆ str               ┆ f64      ┆ f64       ┆ str          ┆ str             ┆ str            ┆ str        ┆   ┆ i64        ┆ list[str]                       ┆ list[str]                       ┆ list[str]                       ┆ str            ┆ date       ┆ str    ┆ i64                           ┆ str              ┆ str             ┆ list[str]                       ┆ str                             │
╞═════════════════════════════════╪═══════╪═════════╪════════════════════════════╪════════════════════════════╪═════════════╪═══════════════════╪══════════╪═══════════╪══════════════╪═════════════════╪════════════════╪════════════╪═══╪════════════╪═════════════════════════════════╪═════════════════════════════════╪═════════════════════════════════╪════════════════╪════════════╪════════╪═══════════════════════════════╪══════════════════╪═════════════════╪═════════════════════════════════╪═════════════════════════════════╡
│ 9217bd69-89bf-452b-bff4-bf84fb… ┆ 43136 ┆ 505     ┆ 2024-09-26 12:22:49.938381 ┆ 2024-09-26 12:22:50.386852 ┆ 2048976     ┆ Burkina Faso      ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant 2   ┆ Modele 1   ┆ … ┆ 3          ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Rafraîchissement (eau, nourr… ┆ Ecoles         ┆ 2024-09-01 ┆ null   ┆ 99                            ┆ null             ┆ null            ┆ ["Communautaire", "Fixe Mobile… ┆ uuid:9217bd69-89bf-452b-bff4-b… │
│ 3b18c141-3119-4049-b1d1-aad2e9… ┆ 43137 ┆ 505     ┆ 2024-09-26 12:22:50.861463 ┆ 2024-09-26 12:22:51.256730 ┆ 2048978     ┆ DS Boromo         ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 11         ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Rafraîchissement (eau, nourr… ┆ Ecoles         ┆ 2024-08-07 ┆ null   ┆ 58                            ┆ null             ┆ null            ┆ ["Porte-à-porte", "Fixe"]       ┆ uuid:3b18c141-3119-4049-b1d1-a… │
│ 2c77336a-7000-4747-aa6c-5daa12… ┆ 43138 ┆ 505     ┆ 2024-09-26 12:22:51.542564 ┆ 2024-09-26 12:22:52.053329 ┆ 2048981     ┆ DS Dedougou       ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant 3   ┆ Modele ... ┆ … ┆ 11         ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Lieux de culte ┆ 2024-08-22 ┆ null   ┆ 99                            ┆ null             ┆ null            ┆ ["Communautaire", "Porte-à-por… ┆ uuid:2c77336a-7000-4747-aa6c-5… │
│ 127e80a4-6a1a-484c-badf-eb3520… ┆ 43139 ┆ 505     ┆ 2024-09-26 12:22:52.350867 ┆ 2024-09-26 12:22:52.739952 ┆ 2048983     ┆ DS Nouna          ┆ null     ┆ null      ┆ Oui          ┆ Congélateur     ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 7          ┆ ["Mobilisation de la communaut… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Chefferies     ┆ 2024-08-10 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["Porte-à-porte", "Fixe"]       ┆ uuid:127e80a4-6a1a-484c-badf-e… │
│ 8a8127b0-c2d7-4b38-b29e-3b1d00… ┆ 43140 ┆ 505     ┆ 2024-09-26 12:22:53.028916 ┆ 2024-09-26 12:22:53.378534 ┆ 2048980     ┆ DS Solenzo        ┆ null     ┆ null      ┆ Oui          ┆ Congélateur     ┆ Fabriquant 2   ┆ Modele 1   ┆ … ┆ 8          ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Ecoles         ┆ 2024-07-04 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["Communautaire", "Fixe"]       ┆ uuid:8a8127b0-c2d7-4b38-b29e-3… │
│ …                               ┆ …     ┆ …       ┆ …                          ┆ …                          ┆ …           ┆ …                 ┆ …        ┆ …         ┆ …            ┆ …               ┆ …              ┆ …          ┆ … ┆ …          ┆ …                               ┆ …                               ┆ …                               ┆ …              ┆ …          ┆ …      ┆ …                             ┆ …                ┆ …               ┆ …                               ┆ …                               │
│ 99aea198-3239-4cb0-a342-fa6abc… ┆ 43149 ┆ 505     ┆ 2024-09-26 12:22:59.650837 ┆ 2024-09-26 12:23:00.192410 ┆ 2049021     ┆ Bourasso          ┆ null     ┆ null      ┆ Oui          ┆ Réfrigérateur   ┆ Fabriquant ... ┆ Modele 2   ┆ … ┆ 7          ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Ecoles         ┆ 2024-08-07 ┆ null   ┆ 83                            ┆ null             ┆ null            ┆ ["Communautaire", "Fixe Mobile… ┆ uuid:99aea198-3239-4cb0-a342-f… │
│ e8a2a327-0392-4a42-a6b8-3df475… ┆ 43150 ┆ 505     ┆ 2024-09-26 12:23:00.514687 ┆ 2024-09-26 12:23:00.902340 ┆ 2049011     ┆ Dedougou          ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant ... ┆ Modele ... ┆ … ┆ 11         ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Autres         ┆ 2024-08-13 ┆ null   ┆ 98                            ┆ null             ┆ null            ┆ ["Communautaire", "Porte-à-por… ┆ uuid:e8a2a327-0392-4a42-a6b8-3… │
│ ea4e7b45-66d9-4c8f-b490-fc7017… ┆ 43151 ┆ 505     ┆ 2024-09-26 12:23:01.246092 ┆ 2024-09-26 12:23:01.783395 ┆ 2048996     ┆ Di                ┆ null     ┆ null      ┆ Oui          ┆ Congélateur     ┆ Fabriquant 2   ┆ Modele 3   ┆ … ┆ 3          ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Marchés        ┆ 2024-08-26 ┆ null   ┆ 70                            ┆ null             ┆ null            ┆ ["Communautaire", "Porte-à-por… ┆ uuid:ea4e7b45-66d9-4c8f-b490-f… │
│ 1285afda-3e1d-45e5-b4c4-5c0423… ┆ 43152 ┆ 505     ┆ 2024-09-26 12:23:02.205442 ┆ 2024-09-26 12:23:02.677701 ┆ 2049025     ┆ Djibasso          ┆ null     ┆ null      ┆ Oui          ┆ Réfrigérateur   ┆ Fabriquant 1   ┆ Modele ... ┆ … ┆ 6          ┆ ["Mobilisation des autres lead… ┆ ["Sécurisation des mobilisateu… ┆ ["Rafraîchissement (eau, nourr… ┆ Chefferies     ┆ 2024-06-18 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["Communautaire", "Fixe Mobile… ┆ uuid:1285afda-3e1d-45e5-b4c4-5… │
│ 6024e8d9-50e9-495a-a80b-05eced… ┆ 43153 ┆ 505     ┆ 2024-09-26 12:23:03.161387 ┆ 2024-09-26 12:23:03.579861 ┆ 2048977     ┆ Boucle du Mouhoun ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 3          ┆ ["Mobilisation de la communaut… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Lieux de culte ┆ 2024-08-11 ┆ null   ┆ 73                            ┆ null             ┆ null            ┆ ["Porte-à-porte", "Fixe"]       ┆ uuid:6024e8d9-50e9-495a-a80b-0… │
└─────────────────────────────────┴───────┴─────────┴────────────────────────────┴────────────────────────────┴─────────────┴───────────────────┴──────────┴───────────┴──────────────┴─────────────────┴────────────────┴────────────┴───┴────────────┴─────────────────────────────────┴─────────────────────────────────┴─────────────────────────────────┴────────────────┴────────────┴────────┴───────────────────────────────┴──────────────────┴─────────────────┴─────────────────────────────────┴─────────────────────────────────┘
```