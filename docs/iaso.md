
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
- `org_unit_type`: Organisation unit type
- `latitude`: Latitude
- `longitude`: Longitude
- `opening_date`: Org unit opening date
- `closing_date`: Org unit closing date
- `created_at`: Org unit creation date
- `updated_at`: Org unit last updated date
- `source`: Data source
- `source_ref`: Org unit ID in source
- `validation_status`: Validation status
- `level_{n}_id`: ID of parent org unit at level n
- `level_{n}_name`: Name of parent org unit at level n
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
shape: (136, 21)
┌─────────┬────────────┬─────────────────────────────────┬──────────┬───────────┬──────────────┬──────────────┬─────────────────────┬─────────────────────┬─────────┬───────────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬──────────────┬───────────────────┬──────────────┬──────────────┬─────────────────────────────────┐
│ id      ┆ name       ┆ org_unit_type                   ┆ latitude ┆ longitude ┆ opening_date ┆ closing_date ┆ created_at          ┆ updated_at          ┆ source  ┆ validation_status ┆ source_ref  ┆ level_1_ref ┆ level_2_ref ┆ level_3_ref ┆ level_4_ref ┆ level_1_name ┆ level_2_name      ┆ level_3_name ┆ level_4_name ┆ geometry                        │
│ ---     ┆ ---        ┆ ---                             ┆ ---      ┆ ---       ┆ ---          ┆ ---          ┆ ---                 ┆ ---                 ┆ ---     ┆ ---               ┆ ---         ┆ ---         ┆ ---         ┆ ---         ┆ ---         ┆ ---          ┆ ---               ┆ ---          ┆ ---          ┆ ---                             │
│ i64     ┆ str        ┆ str                             ┆ f64      ┆ f64       ┆ date         ┆ date         ┆ datetime[μs]        ┆ datetime[μs]        ┆ str     ┆ str               ┆ str         ┆ str         ┆ str         ┆ str         ┆ str         ┆ str          ┆ str               ┆ str          ┆ str          ┆ str                             │
╞═════════╪════════════╪═════════════════════════════════╪══════════╪═══════════╪══════════════╪══════════════╪═════════════════════╪═════════════════════╪═════════╪═══════════════════╪═════════════╪═════════════╪═════════════╪═════════════╪═════════════╪══════════════╪═══════════════════╪══════════════╪══════════════╪═════════════════════════════════╡
│ 2049004 ┆ Balave     ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ 2024-12-01   ┆ 2024-09-26 12:21:00 ┆ 2024-12-12 10:52:00 ┆ wfdmqgv ┆ VALID             ┆ UMJIrkSBNbU ┆ XfC8RKeUvO4 ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Solenzo   ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-4.0141167… │
│ 2048991 ┆ Bana       ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ null         ┆ 2024-09-26 12:21:00 ┆ 2024-09-26 12:21:00 ┆ wfdmqgv ┆ VALID             ┆ uQB7JiOOrjv ┆ CTtB0TPRvWc ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Boromo    ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-3.4937746… │
│ 2049022 ┆ Barani     ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ null         ┆ 2024-09-26 12:21:00 ┆ 2024-09-26 12:21:00 ┆ wfdmqgv ┆ VALID             ┆ NrG13X1XYqQ ┆ B4Ra7K6HuCE ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Nouna     ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-3.4389384… │
│ 2049020 ┆ Bomborokuy ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ null         ┆ 2024-09-26 12:21:00 ┆ 2024-09-26 12:21:00 ┆ wfdmqgv ┆ VALID             ┆ DHl6M4QkLwf ┆ B4Ra7K6HuCE ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Nouna     ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-4.0204591… │
│ 2049013 ┆ Bondokuy   ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ null         ┆ 2024-09-26 12:21:00 ┆ 2024-09-26 12:21:00 ┆ wfdmqgv ┆ VALID             ┆ mj68YizDVF5 ┆ tiEY3MitYl2 ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Dedougou  ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-3.4937746… │
│ …       ┆ …          ┆ …                               ┆ …        ┆ …         ┆ …            ┆ …            ┆ …                   ┆ …                   ┆ …       ┆ …                 ┆ …           ┆ …           ┆ …           ┆ …           ┆ …           ┆ …            ┆ …                 ┆ …            ┆ …            ┆ …                               │
│ 2049016 ┆ Toma       ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ null         ┆ 2024-09-26 12:21:00 ┆ 2024-09-26 12:21:00 ┆ wfdmqgv ┆ VALID             ┆ Uhsw8vIvsoC ┆ hEu36sUTBzU ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Toma      ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-2.7440644… │
│ 2048999 ┆ Tougan     ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ null         ┆ 2024-09-26 12:21:00 ┆ 2024-09-26 12:21:00 ┆ wfdmqgv ┆ VALID             ┆ qX6MMu2XR7K ┆ LatuEy3yR38 ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Tougan    ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-3.0308257… │
│ 2049018 ┆ Yaba       ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ null         ┆ 2024-09-26 12:21:00 ┆ 2024-09-26 12:21:00 ┆ wfdmqgv ┆ VALID             ┆ lHf8j547iaL ┆ hEu36sUTBzU ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Toma      ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-3.0316312… │
│ 2048987 ┆ Yaho       ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ null         ┆ 2024-09-26 12:21:00 ┆ 2024-09-26 12:21:00 ┆ wfdmqgv ┆ VALID             ┆ Pzf416puQrn ┆ CTtB0TPRvWc ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Boromo    ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-3.4668757… │
│ 2049017 ┆ Ye         ┆ Health area/Aire de santé - AR… ┆ null     ┆ null      ┆ null         ┆ null         ┆ 2024-09-26 12:21:00 ┆ 2024-09-26 12:21:00 ┆ wfdmqgv ┆ VALID             ┆ caFiJxeH5k7 ┆ hEu36sUTBzU ┆ awG7snlrjVy ┆ zmSNCYjqQGj ┆ null        ┆ DS Toma      ┆ Boucle du Mouhoun ┆ Burkina Faso ┆ null         ┆ {"coordinates": [[[[-3.0823771… │
└─────────┴────────────┴─────────────────────────────────┴──────────┴───────────┴──────────────┴──────────────┴─────────────────────┴─────────────────────┴─────────┴───────────────────┴─────────────┴─────────────┴─────────────┴─────────────┴─────────────┴──────────────┴───────────────────┴──────────────┴──────────────┴─────────────────────────────────┘
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
  - `id`: Submission ID
  - `form_version`: IASO form version
  - `created_at`: Creation date
  - `updated_at`: Last updated date
  - `org_unit_id`: Organisation unit ID
  - `org_unit_name`: Organisation unit name
  - `org_unit_ref`: Organisation unit reference
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
┌───────┬──────────────┬─────────────────────┬─────────────────────┬─────────────┬───────────────────┬──────────────┬──────────┬───────────┬──────────────┬─────────────────┬────────────────┬────────────┬───┬────────────┬─────────────────────────────────┬─────────────────────────────────┬─────────────────────────────────┬────────────────┬────────────┬────────┬───────────────────────────────┬──────────────────┬─────────────────┬───────────────────┬─────────────────────────────────┐
│ id    ┆ form_version ┆ created_at          ┆ updated_at          ┆ org_unit_id ┆ org_unit_name     ┆ org_unit_ref ┆ latitude ┆ longitude ┆ presence_cdf ┆ type_equipement ┆ fabriquant     ┆ modele     ┆ … ┆ nbre_homme ┆ engagements_sensibilisation     ┆ engagements_rh                  ┆ engagements_materiel            ┆ lieu_rdv       ┆ date_rdv   ┆ imgUrl ┆ Population_estimee_du_village ┆ Nombre_de_menage ┆ Nombre_de_MILDA ┆ strategie         ┆ instanceID                      │
│ ---   ┆ ---          ┆ ---                 ┆ ---                 ┆ ---         ┆ ---               ┆ ---          ┆ ---      ┆ ---       ┆ ---          ┆ ---             ┆ ---            ┆ ---        ┆   ┆ ---        ┆ ---                             ┆ ---                             ┆ ---                             ┆ ---            ┆ ---        ┆ ---    ┆ ---                           ┆ ---              ┆ ---             ┆ ---               ┆ ---                             │
│ str   ┆ str          ┆ datetime[μs]        ┆ datetime[μs]        ┆ i64         ┆ str               ┆ str          ┆ f64      ┆ f64       ┆ str          ┆ str             ┆ str            ┆ str        ┆   ┆ i64        ┆ list[str]                       ┆ list[str]                       ┆ list[str]                       ┆ str            ┆ date       ┆ str    ┆ i64                           ┆ str              ┆ str             ┆ list[str]         ┆ str                             │
╞═══════╪══════════════╪═════════════════════╪═════════════════════╪═════════════╪═══════════════════╪══════════════╪══════════╪═══════════╪══════════════╪═════════════════╪════════════════╪════════════╪═══╪════════════╪═════════════════════════════════╪═════════════════════════════════╪═════════════════════════════════╪════════════════╪════════════╪════════╪═══════════════════════════════╪══════════════════╪═════════════════╪═══════════════════╪═════════════════════════════════╡
│ 43136 ┆ 2024092601   ┆ 2024-09-26 12:22:49 ┆ 2024-09-26 12:22:49 ┆ 2048976     ┆ Burkina Faso      ┆ zmSNCYjqQGj  ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant 2   ┆ Modele 1   ┆ … ┆ 3          ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["rafra_chissement__eau__nourr… ┆ ecoles         ┆ 2024-09-01 ┆ null   ┆ 99                            ┆ null             ┆ null            ┆ ["1", "4"]        ┆ uuid:9217bd69-89bf-452b-bff4-b… │
│ 43137 ┆ 2024092601   ┆ 2024-09-26 12:22:50 ┆ 2024-09-26 12:22:50 ┆ 2048978     ┆ DS Boromo         ┆ CTtB0TPRvWc  ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 11         ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["rafra_chissement__eau__nourr… ┆ ecoles         ┆ 2024-08-07 ┆ null   ┆ 58                            ┆ null             ┆ null            ┆ ["2", "3"]        ┆ uuid:3b18c141-3119-4049-b1d1-a… │
│ 43138 ┆ 2024092601   ┆ 2024-09-26 12:22:51 ┆ 2024-09-26 12:22:51 ┆ 2048981     ┆ DS Dedougou       ┆ tiEY3MitYl2  ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant 3   ┆ Modele ... ┆ … ┆ 11         ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ lieux_de_culte ┆ 2024-08-22 ┆ null   ┆ 99                            ┆ null             ┆ null            ┆ ["1", "2", … "4"] ┆ uuid:2c77336a-7000-4747-aa6c-5… │
│ 43139 ┆ 2024092601   ┆ 2024-09-26 12:22:52 ┆ 2024-09-26 12:22:52 ┆ 2048983     ┆ DS Nouna          ┆ B4Ra7K6HuCE  ┆ null     ┆ null      ┆ yes          ┆ congelateur     ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 7          ┆ ["mobilisation_de_la_communaut… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ chefferies     ┆ 2024-08-10 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["2", "3"]        ┆ uuid:127e80a4-6a1a-484c-badf-e… │
│ 43140 ┆ 2024092601   ┆ 2024-09-26 12:22:52 ┆ 2024-09-26 12:22:52 ┆ 2048980     ┆ DS Solenzo        ┆ XfC8RKeUvO4  ┆ null     ┆ null      ┆ yes          ┆ congelateur     ┆ Fabriquant 2   ┆ Modele 1   ┆ … ┆ 8          ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "autre_engagem… ┆ ["logistique_de_poste_de_vacci… ┆ ecoles         ┆ 2024-07-04 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["1", "3"]        ┆ uuid:8a8127b0-c2d7-4b38-b29e-3… │
│ …     ┆ …            ┆ …                   ┆ …                   ┆ …           ┆ …                 ┆ …            ┆ …        ┆ …         ┆ …            ┆ …               ┆ …              ┆ …          ┆ … ┆ …          ┆ …                               ┆ …                               ┆ …                               ┆ …              ┆ …          ┆ …      ┆ …                             ┆ …                ┆ …               ┆ …                 ┆ …                               │
│ 43149 ┆ 2024092601   ┆ 2024-09-26 12:22:59 ┆ 2024-09-26 12:22:59 ┆ 2049021     ┆ Bourasso          ┆ HurgLopZRl5  ┆ null     ┆ null      ┆ yes          ┆ refrigerateur   ┆ Fabriquant ... ┆ Modele 2   ┆ … ┆ 7          ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ ecoles         ┆ 2024-08-07 ┆ null   ┆ 83                            ┆ null             ┆ null            ┆ ["1", "4"]        ┆ uuid:99aea198-3239-4cb0-a342-f… │
│ 43150 ┆ 2024092601   ┆ 2024-09-26 12:23:00 ┆ 2024-09-26 12:23:00 ┆ 2049011     ┆ Dedougou          ┆ cwNqtZtnTgU  ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant ... ┆ Modele ... ┆ … ┆ 11         ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ autres         ┆ 2024-08-13 ┆ null   ┆ 98                            ┆ null             ┆ null            ┆ ["1", "2", … "4"] ┆ uuid:e8a2a327-0392-4a42-a6b8-3… │
│ 43151 ┆ 2024092601   ┆ 2024-09-26 12:23:01 ┆ 2024-09-26 12:23:01 ┆ 2048996     ┆ Di                ┆ VfuI1E54hNc  ┆ null     ┆ null      ┆ yes          ┆ congelateur     ┆ Fabriquant 2   ┆ Modele 3   ┆ … ┆ 3          ┆ ["mobilisation_des_autres_lead… ┆ ["volontaires", "securisation_… ┆ ["logistique_de_poste_de_vacci… ┆ march_s        ┆ 2024-08-26 ┆ null   ┆ 70                            ┆ null             ┆ null            ┆ ["1", "2"]        ┆ uuid:ea4e7b45-66d9-4c8f-b490-f… │
│ 43152 ┆ 2024092601   ┆ 2024-09-26 12:23:01 ┆ 2024-09-26 12:23:01 ┆ 2049025     ┆ Djibasso          ┆ bEKAF9scFJC  ┆ null     ┆ null      ┆ yes          ┆ refrigerateur   ┆ Fabriquant 1   ┆ Modele ... ┆ … ┆ 6          ┆ ["mobilisation_des_autres_lead… ┆ ["securisation_des_mobilisateu… ┆ ["rafra_chissement__eau__nourr… ┆ chefferies     ┆ 2024-06-18 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["1", "4"]        ┆ uuid:1285afda-3e1d-45e5-b4c4-5… │
│ 43153 ┆ 2024092601   ┆ 2024-09-26 12:23:03 ┆ 2024-09-26 12:23:03 ┆ 2048977     ┆ Boucle du Mouhoun ┆ awG7snlrjVy  ┆ null     ┆ null      ┆ no           ┆ None            ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 3          ┆ ["mobilisation_de_la_communaut… ┆ ["volontaires", "autre_engagem… ┆ ["logistique_de_poste_de_vacci… ┆ lieux_de_culte ┆ 2024-08-11 ┆ null   ┆ 73                            ┆ null             ┆ null            ┆ ["2", "3"]        ┆ uuid:6024e8d9-50e9-495a-a80b-0… │
└───────┴──────────────┴─────────────────────┴─────────────────────┴─────────────┴───────────────────┴──────────────┴──────────┴───────────┴──────────────┴─────────────────┴────────────────┴────────────┴───┴────────────┴─────────────────────────────────┴─────────────────────────────────┴─────────────────────────────────┴────────────────┴────────────┴────────┴───────────────────────────────┴──────────────────┴─────────────────┴───────────────────┴─────────────────────────────────┘
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
┌───────┬──────────────┬─────────────────────┬─────────────────────┬─────────────┬───────────────────┬──────────────┬──────────┬───────────┬──────────────┬─────────────────┬────────────────┬────────────┬───┬────────────┬─────────────────────────────────┬─────────────────────────────────┬─────────────────────────────────┬────────────────┬────────────┬────────┬───────────────────────────────┬──────────────────┬─────────────────┬─────────────────────────────────┬─────────────────────────────────┐
│ id    ┆ form_version ┆ created_at          ┆ updated_at          ┆ org_unit_id ┆ org_unit_name     ┆ org_unit_ref ┆ latitude ┆ longitude ┆ presence_cdf ┆ type_equipement ┆ fabriquant     ┆ modele     ┆ … ┆ nbre_homme ┆ engagements_sensibilisation     ┆ engagements_rh                  ┆ engagements_materiel            ┆ lieu_rdv       ┆ date_rdv   ┆ imgUrl ┆ Population_estimee_du_village ┆ Nombre_de_menage ┆ Nombre_de_MILDA ┆ strategie                       ┆ instanceID                      │
│ ---   ┆ ---          ┆ ---                 ┆ ---                 ┆ ---         ┆ ---               ┆ ---          ┆ ---      ┆ ---       ┆ ---          ┆ ---             ┆ ---            ┆ ---        ┆   ┆ ---        ┆ ---                             ┆ ---                             ┆ ---                             ┆ ---            ┆ ---        ┆ ---    ┆ ---                           ┆ ---              ┆ ---             ┆ ---                             ┆ ---                             │
│ str   ┆ str          ┆ datetime[μs]        ┆ datetime[μs]        ┆ i64         ┆ str               ┆ str          ┆ f64      ┆ f64       ┆ str          ┆ str             ┆ str            ┆ str        ┆   ┆ i64        ┆ list[str]                       ┆ list[str]                       ┆ list[str]                       ┆ str            ┆ date       ┆ str    ┆ i64                           ┆ str              ┆ str             ┆ list[str]                       ┆ str                             │
╞═══════╪══════════════╪═════════════════════╪═════════════════════╪═════════════╪═══════════════════╪══════════════╪══════════╪═══════════╪══════════════╪═════════════════╪════════════════╪════════════╪═══╪════════════╪═════════════════════════════════╪═════════════════════════════════╪═════════════════════════════════╪════════════════╪════════════╪════════╪═══════════════════════════════╪══════════════════╪═════════════════╪═════════════════════════════════╪═════════════════════════════════╡
│ 43136 ┆ 2024092601   ┆ 2024-09-26 12:22:49 ┆ 2024-09-26 12:22:49 ┆ 2048976     ┆ Burkina Faso      ┆ zmSNCYjqQGj  ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant 2   ┆ Modele 1   ┆ … ┆ 3          ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Rafraîchissement (eau, nourr… ┆ Ecoles         ┆ 2024-09-01 ┆ null   ┆ 99                            ┆ null             ┆ null            ┆ ["Communautaire", "Fixe Mobile… ┆ uuid:9217bd69-89bf-452b-bff4-b… │
│ 43137 ┆ 2024092601   ┆ 2024-09-26 12:22:50 ┆ 2024-09-26 12:22:50 ┆ 2048978     ┆ DS Boromo         ┆ CTtB0TPRvWc  ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 11         ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Rafraîchissement (eau, nourr… ┆ Ecoles         ┆ 2024-08-07 ┆ null   ┆ 58                            ┆ null             ┆ null            ┆ ["Porte-à-porte", "Fixe"]       ┆ uuid:3b18c141-3119-4049-b1d1-a… │
│ 43138 ┆ 2024092601   ┆ 2024-09-26 12:22:51 ┆ 2024-09-26 12:22:51 ┆ 2048981     ┆ DS Dedougou       ┆ tiEY3MitYl2  ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant 3   ┆ Modele ... ┆ … ┆ 11         ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Lieux de culte ┆ 2024-08-22 ┆ null   ┆ 99                            ┆ null             ┆ null            ┆ ["Communautaire", "Porte-à-por… ┆ uuid:2c77336a-7000-4747-aa6c-5… │
│ 43139 ┆ 2024092601   ┆ 2024-09-26 12:22:52 ┆ 2024-09-26 12:22:52 ┆ 2048983     ┆ DS Nouna          ┆ B4Ra7K6HuCE  ┆ null     ┆ null      ┆ Oui          ┆ Congélateur     ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 7          ┆ ["Mobilisation de la communaut… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Chefferies     ┆ 2024-08-10 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["Porte-à-porte", "Fixe"]       ┆ uuid:127e80a4-6a1a-484c-badf-e… │
│ 43140 ┆ 2024092601   ┆ 2024-09-26 12:22:52 ┆ 2024-09-26 12:22:52 ┆ 2048980     ┆ DS Solenzo        ┆ XfC8RKeUvO4  ┆ null     ┆ null      ┆ Oui          ┆ Congélateur     ┆ Fabriquant 2   ┆ Modele 1   ┆ … ┆ 8          ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Ecoles         ┆ 2024-07-04 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["Communautaire", "Fixe"]       ┆ uuid:8a8127b0-c2d7-4b38-b29e-3… │
│ …     ┆ …            ┆ …                   ┆ …                   ┆ …           ┆ …                 ┆ …            ┆ …        ┆ …         ┆ …            ┆ …               ┆ …              ┆ …          ┆ … ┆ …          ┆ …                               ┆ …                               ┆ …                               ┆ …              ┆ …          ┆ …      ┆ …                             ┆ …                ┆ …               ┆ …                               ┆ …                               │
│ 43149 ┆ 2024092601   ┆ 2024-09-26 12:22:59 ┆ 2024-09-26 12:22:59 ┆ 2049021     ┆ Bourasso          ┆ HurgLopZRl5  ┆ null     ┆ null      ┆ Oui          ┆ Réfrigérateur   ┆ Fabriquant ... ┆ Modele 2   ┆ … ┆ 7          ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Ecoles         ┆ 2024-08-07 ┆ null   ┆ 83                            ┆ null             ┆ null            ┆ ["Communautaire", "Fixe Mobile… ┆ uuid:99aea198-3239-4cb0-a342-f… │
│ 43150 ┆ 2024092601   ┆ 2024-09-26 12:23:00 ┆ 2024-09-26 12:23:00 ┆ 2049011     ┆ Dedougou          ┆ cwNqtZtnTgU  ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant ... ┆ Modele ... ┆ … ┆ 11         ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Autres         ┆ 2024-08-13 ┆ null   ┆ 98                            ┆ null             ┆ null            ┆ ["Communautaire", "Porte-à-por… ┆ uuid:e8a2a327-0392-4a42-a6b8-3… │
│ 43151 ┆ 2024092601   ┆ 2024-09-26 12:23:01 ┆ 2024-09-26 12:23:01 ┆ 2048996     ┆ Di                ┆ VfuI1E54hNc  ┆ null     ┆ null      ┆ Oui          ┆ Congélateur     ┆ Fabriquant 2   ┆ Modele 3   ┆ … ┆ 3          ┆ ["Mobilisation des autres lead… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Marchés        ┆ 2024-08-26 ┆ null   ┆ 70                            ┆ null             ┆ null            ┆ ["Communautaire", "Porte-à-por… ┆ uuid:ea4e7b45-66d9-4c8f-b490-f… │
│ 43152 ┆ 2024092601   ┆ 2024-09-26 12:23:01 ┆ 2024-09-26 12:23:01 ┆ 2049025     ┆ Djibasso          ┆ bEKAF9scFJC  ┆ null     ┆ null      ┆ Oui          ┆ Réfrigérateur   ┆ Fabriquant 1   ┆ Modele ... ┆ … ┆ 6          ┆ ["Mobilisation des autres lead… ┆ ["Sécurisation des mobilisateu… ┆ ["Rafraîchissement (eau, nourr… ┆ Chefferies     ┆ 2024-06-18 ┆ null   ┆ 95                            ┆ null             ┆ null            ┆ ["Communautaire", "Fixe Mobile… ┆ uuid:1285afda-3e1d-45e5-b4c4-5… │
│ 43153 ┆ 2024092601   ┆ 2024-09-26 12:23:03 ┆ 2024-09-26 12:23:03 ┆ 2048977     ┆ Boucle du Mouhoun ┆ awG7snlrjVy  ┆ null     ┆ null      ┆ Non          ┆ None            ┆ Fabriquant ... ┆ Modele 1   ┆ … ┆ 3          ┆ ["Mobilisation de la communaut… ┆ ["Volontaires (guide, griot, c… ┆ ["Logistique de poste de vacci… ┆ Lieux de culte ┆ 2024-08-11 ┆ null   ┆ 73                            ┆ null             ┆ null            ┆ ["Porte-à-porte", "Fixe"]       ┆ uuid:6024e8d9-50e9-495a-a80b-0… │
└───────┴──────────────┴─────────────────────┴─────────────────────┴─────────────┴───────────────────┴──────────────┴──────────┴───────────┴──────────────┴─────────────────┴────────────────┴────────────┴───┴────────────┴─────────────────────────────────┴─────────────────────────────────┴─────────────────────────────────┴────────────────┴────────────┴────────┴───────────────────────────────┴──────────────────┴─────────────────┴─────────────────────────────────┴─────────────────────────────────┘
```