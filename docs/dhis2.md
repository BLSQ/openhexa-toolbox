<!-- vscode-markdown-toc-config
	numbering=false
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->

# OpenHEXA Toolbox DHIS2

An utility library to acquire and process data from a DHIS2 instance.

<!-- /vscode-markdown-toc -->

# OpenHEXA Toolbox DHIS2

An utility library to acquire and process data from a DHIS2 instance.

<!-- vscode-markdown-toc -->
* [Installation](#installation)
* [Usage](#usage)
    * [Connect to an instance](#connect-to-an-instance)
    * [Caching](#caching)
    * [Dataframe API](#dataframe-api)
        * [Extract metadata](#extract-metadata)
            * [Datasets](#datasets)
            * [Data elements](#data-elements)
            * [Data element groups](#data-element-groups)
            * [Organisation units](#organisation-units)
            * [Organisation unit groups](#organisation-unit-groups)
            * [Organisation unit levels](#organisation-unit-levels)
            * [Category option combos](#category-option-combos)
        * [Extract data](#extract-data)
            * [Extract dataset values](#extract-dataset-values)
            * [Extract data element group values](#extract-data-element-group-values) 
            * [Extract data elements values](#extract-data-elements-values)
            * [Extract Analytics query](#extract-analytics-query)
            * [Extract event data values](#extract-event-data-values)
            * [Extract organisation unit attributes](#extract-organisation-unit-attributes)
            * [Import tabular data values](#import-tabular-data-values)
        * [Join dataframe extracts](#join-dataframe-extracts)
    * [JSON API](#json-api)
        * [Read metadata](#read-metadata)
        * [Read data](#read-data)
            * [Data value sets](#data-value-sets)
            * [Analytics](#analytics)
        * [Write data](#write-data)
    * [Periods](#periods)

## [Installation](#)

``` sh
pip install openhexa.toolbox
```

## [Usage](#)

### [Connect to an instance](#)

Credentials are required to initialize a connection to a DHIS2 instance, and must be provided through a `Connection` object.

In an OpenHEXA workspace (e.g. in an OpenHEXA pipeline or in an OpenHEXA notebook), a `Connection` object can be created using the OpenHEXA SDK by providing the identifier of the workspace connection.

![OpenHEXA workspace connection](images/connection_id.png)

``` python
>>> from openhexa.sdk import workspace
>>> from openhexa.toolbox.dhis2 import DHIS2

>>> # initialize a new connection in an OpenHEXA workspace
>>> con = workspace.dhis2_connection("DHIS2_PLAY")
>>> dhis = DHIS2(con)
```

Outside an OpenHEXA workspace, a connection can be manually created using the SDK by providing the instance URL, an username and a password.


``` python
>>> from openhexa.sdk.workspaces.connections import DHIS2Connection
>>> from openhexa.toolbox.dhis2 import DHIS2

>>> # initialize a new connection outside an OpenHEXA workspace
>>> con = DHIS2Connection(url="https://play.dhis2.org/40.0.1", username="admin", password="district")
>>> dhis = DHIS2(con)
```

If needed, the OpenHEXA SDK dependency can be bypassed by providing a `namedtuple` instead of a `Connection` object.

``` python
>>> from collections import namedtuple
>>> from openhexa.toolbox.dhis2 import DHIS2

>>> # initialize a new connection outside an OpenHEXA workspace
>>> Connection = namedtuple("Connection", ["url", "username", "password"])
>>> con = Connection(url="https://play.dhis2.org/40.0.1", username="admin", password="district")
>>> dhis = DHIS2(con)
```

### [Caching](#)

Caching can be activated by providing a cache directory when initializing a new connection.

``` python
>>> from openhexa.sdk import workspace
>>> from openhexa.toolbox.dhis2 import DHIS2

>>> # initialize a new connection in an OpenHEXA workspace
>>> con = workspace.dhis2_connection("DHIS2_PLAY")
>>> dhis = DHIS2(con, cache_dir=".cache")
```

As of now, the library only caches instance metadata and does not handle data queries.

### [Dataframe API](#)

The `dataframe` module provides opinionated functions to extract DHIS2 metadata and data values into analysis-ready dataframes.

#### [Extract metadata](#)

##### [Datasets](#)

```python
from openhexa.sdk import workspace

from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import get_datasets

# Initialize DHIS2 connection
con = workspace.dhis2_connection("my_dhis2_connection")
dhis2 = DHIS2(con)

# Get datasets metadata
df = dataframe.get_datasets(dhis2)
```

Returns a DataFrame with columns:
- `id`: Dataset identifier
- `name`: Dataset name 
- `organisation_units`: List of organisation unit IDs
- `data_elements`: List of data element IDs
- `indicators`: List of indicator IDs
- `period_type`: Period type of the dataset

```
┌─────────────┬─────────────────┬─────────────────┬─────────────────┬────────────────┬─────────────┐
│ id          ┆ name            ┆ organisation_un ┆ data_elements   ┆ indicators     ┆ period_type │
│ ---         ┆ ---             ┆ its             ┆ ---             ┆ ---            ┆ ---         │
│ str         ┆ str             ┆ ---             ┆ list[str]       ┆ list[str]      ┆ str         │
│             ┆                 ┆ list[str]       ┆                 ┆                ┆             │
╞═════════════╪═════════════════╪═════════════════╪═════════════════╪════════════════╪═════════════╡
│ lyLU2wR22tC ┆ ART monthly     ┆ ["y77LiPqLMoq", ┆ ["AzwEuYfWAtN", ┆ []             ┆ Monthly     │
│             ┆ summary         ┆ "rwfuVQHnZJ5",… ┆ "TyQ1vOHM6JO",… ┆                ┆             │
│ BfMAe6Itzgt ┆ Child Health    ┆ ["y77LiPqLMoq", ┆ ["Y53Jcc9LBYh", ┆ []             ┆ Monthly     │
│             ┆                 ┆ "rwfuVQHnZJ5",… ┆ "pikOziyCXbM",… ┆                ┆             │
│ VTdjfLXXmoi ┆ Clinical        ┆ ["y77LiPqLMoq", ┆ ["tY33H1Xmbiq", ┆ []             ┆ SixMonthly  │
│             ┆ Monitoring      ┆ "rwfuVQHnZJ5",… ┆ "X4SRfUAnrHD",… ┆                ┆             │
│             ┆ Checklist       ┆                 ┆                 ┆                ┆             │
│ TuL8IOPzpHh ┆ EPI Stock       ┆ ["y77LiPqLMoq", ┆ ["t99PL3gUxIl", ┆ ["OEWO2PpiUKx" ┆ Monthly     │
│             ┆                 ┆ "rwfuVQHnZJ5",… ┆ "XNrjXqZrHD8",… ┆ , "bASXd9ukRGD ┆             │
│             ┆                 ┆                 ┆                 ┆ ",…            ┆             │
│ Lpw6GcnTrmS ┆ Emergency       ┆ ["y77LiPqLMoq", ┆ ["EX2jDbKe4Yq", ┆ []             ┆ Monthly     │
│             ┆ Response        ┆ "rwfuVQHnZJ5",… ┆ "qiaHMoI3bjA",… ┆                ┆             │
│ …           ┆ …               ┆ …               ┆ …               ┆ …              ┆ …           │
│ Y8gAn9DfAGU ┆ Project         ┆ ["y77LiPqLMoq", ┆ ["qF555AXehEn", ┆ []             ┆ Quarterly   │
│             ┆ Management      ┆ "rwfuVQHnZJ5",… ┆ "seNDI6rguib",… ┆                ┆             │
│ QX4ZTUbOt3a ┆ Reproductive    ┆ ["y77LiPqLMoq", ┆ ["V37YqbqpEhV", ┆ ["gNAXtpqAqW2" ┆ Monthly     │
│             ┆ Health          ┆ "rwfuVQHnZJ5",… ┆ "bqK6eSIwo3h",… ┆ , "n3fzCxYk3k3 ┆             │
│             ┆                 ┆                 ┆                 ┆ ",…            ┆             │
...
│             ┆ Reporting Form  ┆ "rwfuVQHnZJ5",… ┆ "iwKQb6AzcyA",… ┆                ┆             │
│ OsPTWNqq26W ┆ TB/HIV (VCCT)   ┆ ["y77LiPqLMoq", ┆ ["IpwsH1GUjCs", ┆ []             ┆ Monthly     │
│             ┆ monthly summary ┆ "rwfuVQHnZJ5",… ┆ "EASG4IZChNr",… ┆                ┆             │
└─────────────┴─────────────────┴─────────────────┴─────────────────┴────────────────┴─────────────┘
```

##### [Data elements](#)

```python
df = dataframe.get_data_elements(dhis2)
```

Returns a DataFrame with columns:
- `id`: Data element identifier
- `name`: Data element name
- `value_type`: Data element value type

```
┌─────────────┬─────────────────────────────────┬────────────┐
│ id          ┆ name                            ┆ value_type │
│ ---         ┆ ---                             ┆ ---        │
│ str         ┆ str                             ┆ str        │
╞═════════════╪═════════════════════════════════╪════════════╡
│ FTRrcoaog83 ┆ Accute Flaccid Paralysis (Deat… ┆ NUMBER     │
│ P3jJH5Tu5VC ┆ Acute Flaccid Paralysis (AFP) … ┆ NUMBER     │
│ FQ2o8UBlcrS ┆ Acute Flaccid Paralysis (AFP) … ┆ NUMBER     │
│ M62VHgYT2n0 ┆ Acute Flaccid Paralysis (AFP) … ┆ NUMBER     │
│ WO8yRIZb7nb ┆ Additional medication           ┆ TEXT       │
│ …           ┆ …                               ┆ …          │
│ l6byfWFUGaP ┆ Yellow Fever doses given        ┆ NUMBER     │
│ hvdCBRWUk80 ┆ Yellow fever follow-up          ┆ NUMBER     │
│ XWU1Huh0Luy ┆ Yellow fever new                ┆ NUMBER     │
│ zSJF2b48kOg ┆ Yellow fever referrals          ┆ NUMBER     │
│ QN8WyI8KgpU ┆ YesOnly                         ┆ TRUE_ONLY  │
└─────────────┴─────────────────────────────────┴────────────┘
```

##### [Data element groups](#)

```python
# Get data element groups
df = dataframe.get_data_element_groups(dhis2)
```

Returns a DataFrame with columns:
- `id`: Data element group identifier
- `name`: Data element group name

```
┌─────────────┬─────────────────────────────────┬────────────┐
│ id          ┆ name                            ┆ value_type │
│ ---         ┆ ---                             ┆ ---        │
│ str         ┆ str                             ┆ str        │
╞═════════════╪═════════════════════════════════╪════════════╡
│ FTRrcoaog83 ┆ Accute Flaccid Paralysis (Deat… ┆ NUMBER     │
│ P3jJH5Tu5VC ┆ Acute Flaccid Paralysis (AFP) … ┆ NUMBER     │
│ FQ2o8UBlcrS ┆ Acute Flaccid Paralysis (AFP) … ┆ NUMBER     │
│ M62VHgYT2n0 ┆ Acute Flaccid Paralysis (AFP) … ┆ NUMBER     │
│ WO8yRIZb7nb ┆ Additional medication           ┆ TEXT       │
│ …           ┆ …                               ┆ …          │
│ l6byfWFUGaP ┆ Yellow Fever doses given        ┆ NUMBER     │
│ hvdCBRWUk80 ┆ Yellow fever follow-up          ┆ NUMBER     │
│ XWU1Huh0Luy ┆ Yellow fever new                ┆ NUMBER     │
│ zSJF2b48kOg ┆ Yellow fever referrals          ┆ NUMBER     │
│ QN8WyI8KgpU ┆ YesOnly                         ┆ TRUE_ONLY  │
└─────────────┴─────────────────────────────────┴────────────┘
```

##### [Organisation units](#)

```python
# Get org units up to level 2
df = dataframe.get_organisation_units(dhis2, max_level=2)
```

Returns a DataFrame with columns:
- `id`: Organisation unit identifier 
- `name`: Organisation unit name
- `level`: Organisation unit level
- `opening_date`: Organisation unit opening date
- `closed_date`: Organisation unit closing date
- `level_{n}_id`: ID of parent org unit at level n
- `level_{n}_name`: Name of parent org unit at level n
- `geometry`: GeoJSON geometry string

```
┌─────────────┬──────────────┬───────┬─────────────┬──────────────┬─────────────┬──────────────┬─────────────────────────────────┐
│ id          ┆ name         ┆ level ┆ level_1_id  ┆ level_1_name ┆ level_2_id  ┆ level_2_name ┆ geometry                        │
│ ---         ┆ ---          ┆ ---   ┆ ---         ┆ ---          ┆ ---         ┆ ---          ┆ ---                             │
│ str         ┆ str          ┆ i64   ┆ str         ┆ str          ┆ str         ┆ str          ┆ str                             │
╞═════════════╪══════════════╪═══════╪═════════════╪══════════════╪═════════════╪══════════════╪═════════════════════════════════╡
│ ImspTQPwCqd ┆ Sierra Leone ┆ 1     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ null        ┆ null         ┆ null                            │
│ O6uvpzGd5pu ┆ Bo           ┆ 2     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ O6uvpzGd5pu ┆ Bo           ┆ {"type": "Polygon", "coordinat… │
│ fdc6uOvgoji ┆ Bombali      ┆ 2     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ fdc6uOvgoji ┆ Bombali      ┆ {"type": "Polygon", "coordinat… │
│ lc3eMKXaEfw ┆ Bonthe       ┆ 2     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ lc3eMKXaEfw ┆ Bonthe       ┆ {"type": "MultiPolygon", "coor… │
│ jUb8gELQApl ┆ Kailahun     ┆ 2     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ jUb8gELQApl ┆ Kailahun     ┆ {"type": "Polygon", "coordinat… │
│ …           ┆ …            ┆ …     ┆ …           ┆ …            ┆ …           ┆ …            ┆ …                               │
│ jmIPBj66vD6 ┆ Moyamba      ┆ 2     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ jmIPBj66vD6 ┆ Moyamba      ┆ {"type": "MultiPolygon", "coor… │
│ TEQlaapDQoK ┆ Port Loko    ┆ 2     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ TEQlaapDQoK ┆ Port Loko    ┆ {"type": "MultiPolygon", "coor… │
│ bL4ooGhyHRQ ┆ Pujehun      ┆ 2     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ bL4ooGhyHRQ ┆ Pujehun      ┆ {"type": "MultiPolygon", "coor… │
│ eIQbndfxQMb ┆ Tonkolili    ┆ 2     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ eIQbndfxQMb ┆ Tonkolili    ┆ {"type": "Polygon", "coordinat… │
│ at6UHUQatSo ┆ Western Area ┆ 2     ┆ ImspTQPwCqd ┆ Sierra Leone ┆ at6UHUQatSo ┆ Western Area ┆ {"type": "MultiPolygon", "coor… │
└─────────────┴──────────────┴───────┴─────────────┴──────────────┴─────────────┴──────────────┴─────────────────────────────────┘
```

##### [Organisation unit groups](#)

```python
df = dataframe.get_organisation_unit_groups(dhis2)
```

Returns a DataFrame with columns:
- `id`: Group identifier
- `name`: Group name
- `organisation_units`: List of organisation unit IDs

```
┌─────────────┬───────────────────┬─────────────────────────────────┐
│ id          ┆ name              ┆ organisation_units              │
│ ---         ┆ ---               ┆ ---                             │
│ str         ┆ str               ┆ list[str]                       │
╞═════════════╪═══════════════════╪═════════════════════════════════╡
│ CXw2yu5fodb ┆ CHC               ┆ ["Mi4dWRtfIOC", "EUUkKEDoNsf",… │
│ uYxK4wmcPqA ┆ CHP               ┆ ["EQnfnY03sRp", "ZpE2POxvl9P",… │
│ gzcv65VyaGq ┆ Chiefdom          ┆ ["r06ohri9wA9", "Z9QaI6sxTwW",… │
│ RXL3lPSK8oG ┆ Clinic            ┆ ["LaxJ6CD2DHq", "ctfiYW0ePJ8",… │
│ RpbiCJpIYEj ┆ Country           ┆ ["ImspTQPwCqd"]                 │
│ …           ┆ …                 ┆ …                               │
│ oRVt7g429ZO ┆ Public facilities ┆ ["y77LiPqLMoq", "rwfuVQHnZJ5",… │
│ GGghZsfu7qV ┆ Rural             ┆ ["EQnfnY03sRp", "r06ohri9wA9",… │
│ jqBqIXoXpfy ┆ Southern Area     ┆ ["O6uvpzGd5pu", "jmIPBj66vD6",… │
│ f25dqv3Y7Z0 ┆ Urban             ┆ ["y77LiPqLMoq", "Z9QaI6sxTwW",… │
│ b0EsAxm8Nge ┆ Western Area      ┆ ["at6UHUQatSo", "TEQlaapDQoK",… │
└─────────────┴───────────────────┴─────────────────────────────────┘
```

##### [Organisation unit levels](#)

```python
df = dataframe.get_organisation_unit_levels(dhis2)
```

Returns a DataFrame with columns:
- `id`: Organisation unit level identifier
- `name`: Organisation unit level name
- `level`: Organisation unit level

```
┌─────────────┬──────────┬───────┐
│ id          ┆ name     ┆ level │
│ ---         ┆ ---      ┆ ---   │
│ str         ┆ str      ┆ i64   │
╞═════════════╪══════════╪═══════╡
│ H1KlN4QIauv ┆ National ┆ 1     │
│ wjP19dkFeIk ┆ District ┆ 2     │
│ tTUf91fCytl ┆ Chiefdom ┆ 3     │
│ m9lBJogzE95 ┆ Facility ┆ 4     │
└─────────────┴──────────┴───────┘
```

##### [Category option combos](#)

```python
df = dataframe.get_category_option_combos(dhis2)
```

Returns a DataFrame with columns:
- `id`: Category option combo identifier
- `name`: Category option combo name

```
┌─────────────┬─────────────────────────────────┐
│ id          ┆ name                            │
│ ---         ┆ ---                             │
│ str         ┆ str                             │
╞═════════════╪═════════════════════════════════╡
│ S34ULMcHMca ┆ 0-11m                           │
│ sqGRzCziswD ┆ 0-11m                           │
│ o2gxEt6Ek2C ┆ 0-4y                            │
│ LEDQQXEpWUl ┆ 12-59m                          │
│ wHBMVthqIX4 ┆ 12-59m                          │
│ …           ┆ …                               │
│ QjyqqJMm0X7 ┆ World Vision, Improve access t… │
│ zBTkMNxXEvq ┆ World Vision, Improve access t… │
│ E5M2FZ1hReY ┆ World Vision, Provide access t… │
│ pN2UVP29cKQ ┆ World Vision, Provide access t… │
│ HllvX50cXC0 ┆ default                         │
└─────────────┴─────────────────────────────────┘
```

#### [Extract data](#)

##### [Extract dataset values](#)

```python
from datetime import datetime

df = dataframe.extract_dataset(
    dhis2,
    dataset="BfMAe6Itzgt",
    start_date=datetime(2022, 7, 1),
    end_date=datetime(2022, 8, 1),
    org_units=["DiszpKrYNg8"],
    include_children=False
)
```

Returns a DataFrame with columns:
- `data_element_id`: Data element identifier
- `period`: Period identifier
- `organisation_unit_id`: Organisation unit identifier
- `category_option_combo_id`: Category option combo identifier
- `attribute_option_combo_id`: Attribute option combo identifier
- `value`: Data value
- `created`: Creation timestamp
- `last_updated`: Last update timestamp

```
┌─────────────────┬────────┬──────────────────────┬──────────────────────────┬───────────────────────────┬───────┬─────────────────────────┬─────────────────────────┐
│ data_element_id ┆ period ┆ organisation_unit_id ┆ category_option_combo_id ┆ attribute_option_combo_id ┆ value ┆ created                 ┆ last_updated            │
│ ---             ┆ ---    ┆ ---                  ┆ ---                      ┆ ---                       ┆ ---   ┆ ---                     ┆ ---                     │
│ str             ┆ str    ┆ str                  ┆ str                      ┆ str                       ┆ str   ┆ datetime[ms, UTC]       ┆ datetime[ms, UTC]       │
╞═════════════════╪════════╪══════════════════════╪══════════════════════════╪═══════════════════════════╪═══════╪═════════════════════════╪═════════════════════════╡
│ pikOziyCXbM     ┆ 202207 ┆ DiszpKrYNg8          ┆ hEFKSsPV5et              ┆ HllvX50cXC0               ┆ 22    ┆ 2022-09-05 13:06:21 UTC ┆ 2013-09-27 00:00:00 UTC │
│ pikOziyCXbM     ┆ 202207 ┆ DiszpKrYNg8          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 10    ┆ 2022-09-05 13:06:21 UTC ┆ 2010-08-06 00:00:00 UTC │
│ pikOziyCXbM     ┆ 202207 ┆ DiszpKrYNg8          ┆ psbwp3CQEhs              ┆ HllvX50cXC0               ┆ 33    ┆ 2022-09-05 13:06:21 UTC ┆ 2013-11-05 00:00:00 UTC │
│ pikOziyCXbM     ┆ 202207 ┆ DiszpKrYNg8          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 13    ┆ 2022-09-05 13:06:21 UTC ┆ 2010-08-06 00:00:00 UTC │
│ O05mAByOgAv     ┆ 202207 ┆ DiszpKrYNg8          ┆ hEFKSsPV5et              ┆ HllvX50cXC0               ┆ 25    ┆ 2022-09-05 13:06:21 UTC ┆ 2013-09-27 00:00:00 UTC │
│ …               ┆ …      ┆ …                    ┆ …                        ┆ …                         ┆ …     ┆ …                       ┆ …                       │
│ ldGXl6SEdqf     ┆ 202207 ┆ DiszpKrYNg8          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 18    ┆ 2022-09-05 13:06:21 UTC ┆ 2010-08-06 00:00:00 UTC │
│ tU7GixyHhsv     ┆ 202207 ┆ DiszpKrYNg8          ┆ hEFKSsPV5et              ┆ HllvX50cXC0               ┆ 3     ┆ 2022-09-05 13:06:21 UTC ┆ 2013-09-27 00:00:00 UTC │
│ tU7GixyHhsv     ┆ 202207 ┆ DiszpKrYNg8          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 3     ┆ 2022-09-05 13:06:21 UTC ┆ 2013-09-27 00:00:00 UTC │
│ tU7GixyHhsv     ┆ 202207 ┆ DiszpKrYNg8          ┆ psbwp3CQEhs              ┆ HllvX50cXC0               ┆ 3     ┆ 2022-09-05 13:06:21 UTC ┆ 2013-09-27 00:00:00 UTC │
│ tU7GixyHhsv     ┆ 202207 ┆ DiszpKrYNg8          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 3     ┆ 2022-09-05 13:06:21 UTC ┆ 2013-09-27 00:00:00 UTC │
└─────────────────┴────────┴──────────────────────┴──────────────────────────┴───────────────────────────┴───────┴─────────────────────────┴─────────────────────────┘
```

##### [Extract data element group values](#)

```python
df = dataframe.extract_data_element_group(
    dhis2,
    data_element_group="h9cuJOkOwY2",
    start_date=datetime(2020, 11, 1), 
    end_date=datetime(2021, 2, 5),
    org_units="jPidqyo7cpF",
    include_children=True
)
```

Returns a DataFrame with columns:
- `data_element_id`: Data element identifier
- `period`: Period identifier
- `organisation_unit_id`: Organisation unit identifier
- `category_option_combo_id`: Category option combo identifier
- `attribute_option_combo_id`: Attribute option combo identifier
- `value`: Data value
- `created`: Creation timestamp
- `last_updated`: Last update timestamp

```
┌─────────────────┬────────┬──────────────────────┬──────────────────────────┬───────────────────────────┬───────┬─────────────────────────┬─────────────────────────┐
│ data_element_id ┆ period ┆ organisation_unit_id ┆ category_option_combo_id ┆ attribute_option_combo_id ┆ value ┆ created                 ┆ last_updated            │
│ ---             ┆ ---    ┆ ---                  ┆ ---                      ┆ ---                       ┆ ---   ┆ ---                     ┆ ---                     │
│ str             ┆ str    ┆ str                  ┆ str                      ┆ str                       ┆ str   ┆ datetime[ms, UTC]       ┆ datetime[ms, UTC]       │
╞═════════════════╪════════╪══════════════════════╪══════════════════════════╪═══════════════════════════╪═══════╪═════════════════════════╪═════════════════════════╡
│ fClA2Erf6IO     ┆ 202101 ┆ egjrZ1PHNtT          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 10    ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:06:10 UTC │
│ fClA2Erf6IO     ┆ 202101 ┆ egjrZ1PHNtT          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 20    ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:06:10 UTC │
│ fClA2Erf6IO     ┆ 202011 ┆ egjrZ1PHNtT          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 33    ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:18:44 UTC │
│ fClA2Erf6IO     ┆ 202011 ┆ egjrZ1PHNtT          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 36    ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:18:44 UTC │
│ fClA2Erf6IO     ┆ 202012 ┆ egjrZ1PHNtT          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 12    ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:18:44 UTC │
│ …               ┆ …      ┆ …                    ┆ …                        ┆ …                         ┆ …     ┆ …                       ┆ …                       │
│ vI2csg55S9C     ┆ 202101 ┆ egjrZ1PHNtT          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 13    ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:06:10 UTC │
│ vI2csg55S9C     ┆ 202011 ┆ egjrZ1PHNtT          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 11    ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:18:44 UTC │
│ vI2csg55S9C     ┆ 202011 ┆ egjrZ1PHNtT          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 15    ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:18:44 UTC │
│ vI2csg55S9C     ┆ 202012 ┆ egjrZ1PHNtT          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 7     ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:18:44 UTC │
│ vI2csg55S9C     ┆ 202012 ┆ egjrZ1PHNtT          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 27    ┆ 2022-09-05 13:06:21 UTC ┆ 2022-05-29 22:18:44 UTC │
└─────────────────┴────────┴──────────────────────┴──────────────────────────┴───────────────────────────┴───────┴─────────────────────────┴─────────────────────────┘
```

##### [Extract data elements values](#)

```python
    df = dataframe.extract_data_elements(
        dhis2,
        ["pikOziyCXbM", "x3Do5e7g4Qo"],
        start_date=datetime(2020, 11, 1),
        end_date=datetime(2021, 2, 5),
        org_units=["vELbGdEphPd", "UugO8xDeLQD"],
    )
```

Returns a DataFrame with columns:
- `data_element_id`: Data element identifier
- `period`: Period identifier
- `organisation_unit_id`: Organisation unit identifier
- `category_option_combo_id`: Category option combo identifier
- `attribute_option_combo_id`: Attribute option combo identifier
- `value`: Data value
- `created`: Creation timestamp
- `last_updated`: Last update timestamp

```
┌─────────────────┬────────┬──────────────────────┬──────────────────────────┬───────────────────────────┬───────┬─────────────────────────┬─────────────────────────┐
│ data_element_id ┆ period ┆ organisation_unit_id ┆ category_option_combo_id ┆ attribute_option_combo_id ┆ value ┆ created                 ┆ last_updated            │
│ ---             ┆ ---    ┆ ---                  ┆ ---                      ┆ ---                       ┆ ---   ┆ ---                     ┆ ---                     │
│ str             ┆ str    ┆ str                  ┆ str                      ┆ str                       ┆ str   ┆ datetime[ms, UTC]       ┆ datetime[ms, UTC]       │
╞═════════════════╪════════╪══════════════════════╪══════════════════════════╪═══════════════════════════╪═══════╪═════════════════════════╪═════════════════════════╡
│ pikOziyCXbM     ┆ 202101 ┆ vELbGdEphPd          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 23    ┆ 2022-09-05 13:06:21 UTC ┆ 2010-02-27 00:00:00 UTC │
│ pikOziyCXbM     ┆ 202011 ┆ vELbGdEphPd          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 6     ┆ 2022-09-05 13:06:21 UTC ┆ 2010-12-09 00:00:00 UTC │
│ pikOziyCXbM     ┆ 202011 ┆ vELbGdEphPd          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 20    ┆ 2022-09-05 13:06:21 UTC ┆ 2010-12-09 00:00:00 UTC │
│ pikOziyCXbM     ┆ 202012 ┆ vELbGdEphPd          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 6     ┆ 2022-09-05 13:06:21 UTC ┆ 2011-01-11 00:00:00 UTC │
│ pikOziyCXbM     ┆ 202012 ┆ vELbGdEphPd          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 16    ┆ 2022-09-05 13:06:21 UTC ┆ 2011-01-08 00:00:00 UTC │
│ x3Do5e7g4Qo     ┆ 202101 ┆ vELbGdEphPd          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 23    ┆ 2022-09-05 13:06:21 UTC ┆ 2010-02-27 00:00:00 UTC │
│ x3Do5e7g4Qo     ┆ 202011 ┆ vELbGdEphPd          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 22    ┆ 2022-09-05 13:06:21 UTC ┆ 2010-12-09 00:00:00 UTC │
│ x3Do5e7g4Qo     ┆ 202012 ┆ vELbGdEphPd          ┆ V6L425pT3A0              ┆ HllvX50cXC0               ┆ 5     ┆ 2022-09-05 13:06:21 UTC ┆ 2011-01-08 00:00:00 UTC │
│ x3Do5e7g4Qo     ┆ 202012 ┆ vELbGdEphPd          ┆ Prlt0C1RF0s              ┆ HllvX50cXC0               ┆ 30    ┆ 2022-09-05 13:06:21 UTC ┆ 2011-01-08 00:00:00 UTC │
└─────────────────┴────────┴──────────────────────┴──────────────────────────┴───────────────────────────┴───────┴─────────────────────────┴─────────────────────────┘
```

##### [Extract Analytics query](#)

```python
df = dataframe.extract_analytics(
    dhis2,
    periods=["2021"],
    data_elements=["pikOziyCXbM", "x3Do5e7g4Qo"],
    org_unit_levels=[2]
)
```

Returns a DataFrame with columns:
- `data_element_id` or `indicator_id`: Element/indicator identifier
- `category_option_combo_id`: Category option combo identifier (if included)
- `organisation_unit_id`: Organisation unit identifier
- `period`: Period identifier
- `value`: Data value

```
┌─────────────────┬──────────────────────────┬──────────────────────┬────────┬───────┐
│ data_element_id ┆ category_option_combo_id ┆ organisation_unit_id ┆ period ┆ value │
│ ---             ┆ ---                      ┆ ---                  ┆ ---    ┆ ---   │
│ str             ┆ str                      ┆ str                  ┆ str    ┆ str   │
╞═════════════════╪══════════════════════════╪══════════════════════╪════════╪═══════╡
│ pikOziyCXbM     ┆ psbwp3CQEhs              ┆ O6uvpzGd5pu          ┆ 2021   ┆ 44    │
│ x3Do5e7g4Qo     ┆ Prlt0C1RF0s              ┆ jmIPBj66vD6          ┆ 2021   ┆ 3296  │
│ pikOziyCXbM     ┆ Prlt0C1RF0s              ┆ lc3eMKXaEfw          ┆ 2021   ┆ 1279  │
│ x3Do5e7g4Qo     ┆ Prlt0C1RF0s              ┆ fdc6uOvgoji          ┆ 2021   ┆ 3333  │
│ pikOziyCXbM     ┆ hEFKSsPV5et              ┆ kJq2mPyFEHo          ┆ 2021   ┆ 255   │
│ …               ┆ …                        ┆ …                    ┆ …      ┆ …     │
│ pikOziyCXbM     ┆ psbwp3CQEhs              ┆ PMa2VCrupOd          ┆ 2021   ┆ 3     │
│ pikOziyCXbM     ┆ V6L425pT3A0              ┆ eIQbndfxQMb          ┆ 2021   ┆ 953   │
│ pikOziyCXbM     ┆ hEFKSsPV5et              ┆ TEQlaapDQoK          ┆ 2021   ┆ 30    │
│ pikOziyCXbM     ┆ hEFKSsPV5et              ┆ fdc6uOvgoji          ┆ 2021   ┆ 47    │
│ pikOziyCXbM     ┆ Prlt0C1RF0s              ┆ jmIPBj66vD6          ┆ 2021   ┆ 3376  │
└─────────────────┴──────────────────────────┴──────────────────────┴────────┴───────┘
```

##### [Extract event data values](#)

```python
df = dataframe.extract_events(
    dhis2,
    program_id="lxAQ7Zs9VYR",
    org_unit_parents=["ImspTQPwCqd"],  # Country org unit ID
)
```

Or to filter by occurred date:

```python
df = dataframe.extract_events(
    dhis2,
    program_id="lxAQ7Zs9VYR",
    org_unit_parents=["ImspTQPwCqd"],  # Country org unit ID
    occurred_after="2024-01-01",
    occurred_before="2025-01-01"
)
```

Returns a DataFrame with columns:
- `event_id`: Event identifier
- `status`: Event status
- `program_id`: Program identifier
- `program_stage_id`: Program stage identifier
- `organisation_unit_id`: Organisation unit identifier
- `occurred_at`: Event occurrence date
- `deleted`: Event deletion status
- `attribute_option_combo_id`: Attribute option combo identifier
- `data_element_id`: Data element identifier
- `value`: Data element value

```
┌─────────────┬────────┬─────────────┬──────────────────┬───┬─────────┬───────────────────────────┬─────────────────┬───────┐
│ event_id    ┆ status ┆ program_id  ┆ program_stage_id ┆ … ┆ deleted ┆ attribute_option_combo_id ┆ data_element_id ┆ value │
│ ---         ┆ ---    ┆ ---         ┆ ---              ┆   ┆ ---     ┆ ---                       ┆ ---             ┆ ---   │
│ str         ┆ str    ┆ str         ┆ str              ┆   ┆ bool    ┆ str                       ┆ str             ┆ str   │
╞═════════════╪════════╪═════════════╪══════════════════╪═══╪═════════╪═══════════════════════════╪═════════════════╪═══════╡
│ ohAH6BXIMad ┆ ACTIVE ┆ lxAQ7Zs9VYR ┆ dBwrot7S420      ┆ … ┆ false   ┆ HllvX50cXC0               ┆ sWoqcoByYmD     ┆ false │
│ ohAH6BXIMad ┆ ACTIVE ┆ lxAQ7Zs9VYR ┆ dBwrot7S420      ┆ … ┆ false   ┆ HllvX50cXC0               ┆ sWoqcoByYmD     ┆ false │
│ onXW2DQHRGS ┆ ACTIVE ┆ lxAQ7Zs9VYR ┆ dBwrot7S420      ┆ … ┆ false   ┆ HllvX50cXC0               ┆ sWoqcoByYmD     ┆ false │
│ onXW2DQHRGS ┆ ACTIVE ┆ lxAQ7Zs9VYR ┆ dBwrot7S420      ┆ … ┆ false   ┆ HllvX50cXC0               ┆ sWoqcoByYmD     ┆ false │
│ A7vnB73x5Xw ┆ ACTIVE ┆ lxAQ7Zs9VYR ┆ dBwrot7S420      ┆ … ┆ false   ┆ HllvX50cXC0               ┆ sWoqcoByYmD     ┆ true  │
│ A7vnB73x5Xw ┆ ACTIVE ┆ lxAQ7Zs9VYR ┆ dBwrot7S420      ┆ … ┆ false   ┆ HllvX50cXC0               ┆ sWoqcoByYmD     ┆ true  │
└─────────────┴────────┴─────────────┴──────────────────┴───┴─────────┴───────────────────────────┴─────────────────┴───────┘
```

##### [Extract organisation unit attributes](#)

Extract values of custom attributes associated with organisation units as a
dataframe with one row per attribute value.

```python
df = dataframe.extract_organisation_unit_attributes(dhis2)
```

Returns a DataFrame with columns:
- `organisation_unit_id`: Organisation unit identifier
- `organisation_unit_name`: Organisation unit name
- `attribute_id`: Attribute identifier
- `attribute_name`: Attribute name
- `value`: Attribute value

```
┌──────────────────────┬───────────────────────────┬──────────────┬────────────────┬─────────────────────────────────┐
│ organisation_unit_id ┆ organisation_unit_name    ┆ attribute_id ┆ attribute_name ┆ value                           │
│ ---                  ┆ ---                       ┆ ---          ┆ ---            ┆ ---                             │
│ str                  ┆ str                       ┆ str          ┆ str            ┆ str                             │
╞══════════════════════╪═══════════════════════════╪══════════════╪════════════════╪═════════════════════════════════╡
│ plnHVbJR6p4          ┆ Ahamadyya Mission Cl      ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-12.961799,9… │
│ BV4IomHvri4          ┆ Ahmadiyya Muslim Hospital ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-12.285545,8… │
│ qjboFI0irVu          ┆ Air Port Centre, Lungi    ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-13.215772,8… │
│ kbGqmM6ZWWV          ┆ Allen Town Health Post    ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-13.206753,8… │
│ eoYV2p74eVz          ┆ Approved School CHP       ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-13.222615,8… │
│ …                    ┆ …                         ┆ …            ┆ …              ┆ …                               │
│ x5ZxMDvEQUb          ┆ Yonibana MCHP             ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-12.287716,8… │
│ TGRCfJEnXJr          ┆ Yorgbofore MCHP           ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-12.904236,7… │
│ roGdTjEqLZQ          ┆ Yormandu CHC              ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-11.173782,8… │
│ VdXuxcNkiad          ┆ Yoyema MCHP               ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-12.597718,8… │
│ BNFrspDBKel          ┆ Zimmi CHC                 ┆ ihn1wb9eho8  ┆ Catchment area ┆ {"coordinates":[[[-11.378116,7… │
└──────────────────────┴───────────────────────────┴──────────────┴────────────────┴─────────────────────────────────┘
```

##### [Import tabular data values](#)

```python
df = pl.DataFrame([
    {
        "data_element_id": "pikOziyCXbM",
        "period": "202401",
        "organisation_unit_id": "O6uvpzGd5pu", 
        "category_option_combo_id": "psbwp3CQEhs",
        "attribute_option_combo_id": "HllvX50cXC0",
        "value": "100"
    }
])

report = dataframe.import_data_values(
    dhis2,
    data=df,
    import_strategy="CREATE_AND_UPDATE",
    dry_run=True
)
```

Import data values into a DHIS2 from a dataframe and optional UIDs mappings. The input dataframe that contains data values to import must have the following columns:
- `data_element_id`: Data element identifier (str)
- `period`: Period identifier (str)
- `organisation_unit_id`: Organisation unit identifier (str)
- `category_option_combo_id`: Category option combo identifier (str)
- `attribute_option_combo_id`: Attribute option combo identifier (str)
- `value`: Data value (str)

The `import_strategy` parameter can be one of the following:
- `CREATE`: Only create new data values
- `UPDATE`: Only update existing data values
- `CREATE_AND_UPDATE`: Create new data values and update existing ones

By default, the `dry_run` parameter is set to `True` and no data is imported. Set it to `False` to actually import the data. Note that DHIS2 will still validate the import and return an import report for dry runs.

The function returns the DHIS2 import report as a dictionary:

```
{'imported': 2, 'updated': 0, 'ignored': 0, 'deleted': 0}
```

Mappings can be provided to replace the UIDs in the input dataframe before import, for example if the UIDs in the target DHIS2 instance differ from the source. The mappings must be provided as a dictionary with the old UID as key, and the new UID as value:

```python
org_units_mapping = {
    "O6uvpzGd5pu": "vELbGdEphPd",
    "EQnfnY03sRp": "egjrZ1PHNtT"
}

data_elements_mapping = {
    "pikOziyCXbM": "fClA2Erf6IO",
    "x3Do5e7g4Qo": "vI2csg55S9C"
}

report = dataframe.import_data_values(
    dhis2,
    data=df,
    org_units_mapping=org_units_mapping,
    data_elements_mapping=data_elements_mapping,
    import_strategy="CREATE_AND_UPDATE",
    dry_run=True
)
```

#### [Join dataframe extracts](#)

All dataframes extracted from DHIS2 can be joined together using the `join` method from Polars.

For example, to add item names to a dataset extract:

```python
values = dataframe.extract_dataset(
    dhis2,
    dataset="BfMAe6Itzgt",
    start_date=datetime(2022, 7, 1),
    end_date=datetime(2022, 8, 1),
    org_units=["DiszpKrYNg8"],
)

data_elements = dataframe.get_data_elements(dhis2)
combos = dataframe.get_category_option_combos(dhis2)

data_elements = data_elements.select(
    pl.col("id").alias("data_element_id"),
    pl.col("name").alias("data_element_name")
)

combos = combos.select(
    pl.col("id").alias("category_option_combo_id"),
    pl.col("name").alias("category_option_combo_name")
)

values = values.join(
    other=data_elements,
    on="data_element_id",
    how="left"
).join(
    other=combos,
    on="category_option_combo_id",
    how="left"
)
```

Or to join the hierarchy of organisation units:

```python
org_units = dataframe.get_organisation_units(dhis2)

org_units = org_units.select(
    pl.col("id").alias("organisation_unit_id"),
    pl.col("name").alias("organisation_unit_name")
)
```

### [JSON API](#)

#### [Read metadata](#)

Instance metadata can be accessed through a set of methods under the `DHIS2.meta` namespace. Metadata are always returned as JSON-like objects that can easily be converted into Pandas or Polars dataframes.

``` python
>>> import polars as pl
>>> from openhexa.sdk import workspace
>>> from openhexa.toolbox.dhis2 import DHIS2

>>> # initialize a new connection in an OpenHEXA workspace
>>> con = workspace.dhis2_connection("DHIS2_PLAY")
>>> dhis = DHIS2(con, cache_dir=".cache")

>>> # read organisation units metadata
>>> org_units = dhis.meta.organisation_units()
>>> df = pl.DataFrame(org_units)

>>> print(df)

shape: (1_332, 5)
┌─────────────┬──────────────────────┬───────┬─────────────────────────────────┬───────────────────┐
│ id          ┆ name                 ┆ level ┆ path                            ┆ geometry          │
│ ---         ┆ ---                  ┆ ---   ┆ ---                             ┆ ---               │
│ str         ┆ str                  ┆ i64   ┆ str                             ┆ str               │
╞═════════════╪══════════════════════╪═══════╪═════════════════════════════════╪═══════════════════╡
│ Rp268JB6Ne4 ┆ Adonkia CHP          ┆ 4     ┆ /ImspTQPwCqd/at6UHUQatSo/qtr8GG ┆ null              │
│             ┆                      ┆       ┆ l…                              ┆                   │
│ cDw53Ej8rju ┆ Afro Arab Clinic     ┆ 4     ┆ /ImspTQPwCqd/at6UHUQatSo/qtr8GG ┆ null              │
│             ┆                      ┆       ┆ l…                              ┆                   │
│ GvFqTavdpGE ┆ Agape CHP            ┆ 4     ┆ /ImspTQPwCqd/O6uvpzGd5pu/U6Kr7G ┆ null              │
│             ┆                      ┆       ┆ t…                              ┆                   │
│ plnHVbJR6p4 ┆ Ahamadyya Mission Cl ┆ 4     ┆ /ImspTQPwCqd/PMa2VCrupOd/QywkxF ┆ {"type": "Point", │
│             ┆                      ┆       ┆ u…                              ┆ "coordinates":…   │
│ …           ┆ …                    ┆ …     ┆ …                               ┆ …                 │
│ hDW65lFySeF ┆ Youndu CHP           ┆ 4     ┆ /ImspTQPwCqd/jmIPBj66vD6/Z9QaI6 ┆ null              │
│             ┆                      ┆       ┆ s…                              ┆                   │
│ Urk55T8KgpT ┆ Yoyah CHP            ┆ 4     ┆ /ImspTQPwCqd/jUb8gELQApl/yu4N82 ┆ null              │
│             ┆                      ┆       ┆ F…                              ┆                   │
│ VdXuxcNkiad ┆ Yoyema MCHP          ┆ 4     ┆ /ImspTQPwCqd/jmIPBj66vD6/USQdmv ┆ {"type": "Point", │
│             ┆                      ┆       ┆ r…                              ┆ "coordinates":…   │
│ BNFrspDBKel ┆ Zimmi CHC            ┆ 4     ┆ /ImspTQPwCqd/bL4ooGhyHRQ/BD9gU0 ┆ {"type": "Point", │
│             ┆                      ┆       ┆ G…                              ┆ "coordinates":…   │
└─────────────┴──────────────────────┴───────┴─────────────────────────────────┴───────────────────┘
```

The following metadata types are supported:
* `DHIS2.meta.system_info()`
* `DHIS2.meta.organisation_units()`
* `DHIS2.meta.organisation_unit_groups()`
* `DHIS2.meta.organisation_unit_levels()`
* `DHIS2.meta.datasets()`
* `DHIS2.meta.data_elements()`
* `DHIS2.meta.data_element_groups()`
* `DHIS2.meta.indicators()`
* `DHIS2.meta.indicator_groups()`
* `DHIS2.meta.category_option_combos()`

#### [Read data](#)

Data can be accessed through two distinct endpoints: [`dataValueSets`](https://docs.dhis2.org/en/develop/using-the-api/dhis-core-version-240/data.html#webapi_reading_data_values) and [`analytics`](https://docs.dhis2.org/en/develop/using-the-api/dhis-core-version-240/analytics.html). The `dataValueSets` endpoint allows to query raw data values stored in the DHIS2 database, while `analytics` can access aggregated data stored in the DHIS2 analytics tables.

##### [Data value sets](#)

Raw data values can be read using the `DHIS2.data_value_sets.get()` method. The method accepts the following arguments:

* **`data_elements`** : *list of str, optional*<br>
    Data element identifiers (requires DHIS2 >= 2.39)


* **`datasets`** : *list of str, optional*<br>
    Dataset identifiers

* **`data_element_groups`** : *str, optiona*l<br>
    Data element groups identifiers

* **`periods`** : *list of str, optional*<br>
    Period identifiers in ISO format

* **`start_date`** : *str, optional*<br>
    Start date for the time span of the values to export

* **`end_date`** : *str, optional*<br>
    End date for the time span of the values to export

* **`org_units`** : *list of str, optional*<br>
    Organisation units identifiers

* **`org_unit_groups`** : *list of str, optional*<br>
    Organisation unit groups identifiers

* **`children`** : *bool, optional (default=False)*<br>
    Whether to include the children in the hierarchy of the organisation units

* **`attribute_option_combos`** : *list of str, optional*<br>
    Attribute option combos identifiers

* **`last_updated`** : *str, optional*<br>
    Include only data values which are updated since the given time stamp

* **`last_updated_duration`** : *str, optional*<br>
    Include only data values which are updated within the given duration. The
    format is <value><time-unit>, where the supported time units are "d" (days),
    "h" (hours), "m" (minutes) and "s" (seconds).

At least 3 arguments must be provided:
* One in the data dimension (`data_elements`, `data_element_groups`, or `datasets`)
* One in the spatial dimension (`org_units` or `org_unit_groups`)
* One in the temporal dimension (`periods` or `start_date` and `end_date`)

Data values are returned in a JSON-like list of dictionaries that can be converted into a Pandas or Polars dataframe.

```python
>>> import polars as pl
>>> from openhexa.sdk import workspace
>>> from openhexa.toolbox.dhis2 import DHIS2

>>> # initialize a new connection in an OpenHEXA workspace
>>> con = workspace.dhis2_connection("DHIS2_PLAY")
>>> dhis = DHIS2(con, cache_dir=".cache")

>>> data_values = dhis.data_value_sets.get(
...     datasets=["QX4ZTUbOt3a"],
...     org_units=["JQr6TJx5KE3", "KbO0JnhiMwl", "f90eISKFm7P"],
...     start_date="2022-01-01",
...     end_date="2022-04-01"
... )

>>> print(len(data_values))
301

>>> print(data_values[0])
{
    'dataElement': 'zzHwXqxKYy1', 'period': '202201', 'orgUnit': 'JQr6TJx5KE3', 'categoryOptionCombo': 'r8xySVHExGT', 'attributeOptionCombo': 'HllvX50cXC0', 'value': '2', 'storedBy': 'kailahun1', 'created': '2010-03-07T00:00:00.000+0000', 'lastUpdated': '2010-03-07T00:00:00.000+0000', 'comment': '', 'followup': False
}

>>> df = pl.DataFrame(data_values)
>>> print(df)

shape: (301, 11)
┌────────────┬────────┬────────────┬────────────┬───┬────────────┬────────────┬─────────┬──────────┐
│ dataElemen ┆ period ┆ orgUnit    ┆ categoryOp ┆ … ┆ created    ┆ lastUpdate ┆ comment ┆ followup │
│ t          ┆ ---    ┆ ---        ┆ tionCombo  ┆   ┆ ---        ┆ d          ┆ ---     ┆ ---      │
│ ---        ┆ str    ┆ str        ┆ ---        ┆   ┆ str        ┆ ---        ┆ str     ┆ bool     │
│ str        ┆        ┆            ┆ str        ┆   ┆            ┆ str        ┆         ┆          │
╞════════════╪════════╪════════════╪════════════╪═══╪════════════╪════════════╪═════════╪══════════╡
│ zzHwXqxKYy ┆ 202201 ┆ JQr6TJx5KE ┆ r8xySVHExG ┆ … ┆ 2010-03-07 ┆ 2010-03-07 ┆         ┆ false    │
│ 1          ┆        ┆ 3          ┆ T          ┆   ┆ T00:00:00. ┆ T00:00:00. ┆         ┆          │
│            ┆        ┆            ┆            ┆   ┆ 000+0000   ┆ 000+0000   ┆         ┆          │
│ zzHwXqxKYy ┆ 202201 ┆ JQr6TJx5KE ┆ cBQmyRrEKo ┆ … ┆ 2010-03-07 ┆ 2010-03-07 ┆         ┆ false    │
│ 1          ┆        ┆ 3          ┆ 3          ┆   ┆ T00:00:00. ┆ T00:00:00. ┆         ┆          │
│            ┆        ┆            ┆            ┆   ┆ 000+0000   ┆ 000+0000   ┆         ┆          │
│ zzHwXqxKYy ┆ 202201 ┆ JQr6TJx5KE ┆ U1PHVSShuW ┆ … ┆ 2010-03-07 ┆ 2010-03-07 ┆         ┆ false    │
│ 1          ┆        ┆ 3          ┆ j          ┆   ┆ T00:00:00. ┆ T00:00:00. ┆         ┆          │
│            ┆        ┆            ┆            ┆   ┆ 000+0000   ┆ 000+0000   ┆         ┆          │
│ zzHwXqxKYy ┆ 202201 ┆ f90eISKFm7 ┆ dcguXUTwen ┆ … ┆ 2010-03-12 ┆ 2010-03-12 ┆         ┆ false    │
│ 1          ┆        ┆ P          ┆ I          ┆   ┆ T00:00:00. ┆ T00:00:00. ┆         ┆          │
│            ┆        ┆            ┆            ┆   ┆ 000+0000   ┆ 000+0000   ┆         ┆          │
│ …          ┆ …      ┆ …          ┆ …          ┆ … ┆ …          ┆ …          ┆ …       ┆ …        │
│ h8vtacmZL5 ┆ 202203 ┆ f90eISKFm7 ┆ bckzBoAurH ┆ … ┆ 2010-05-21 ┆ 2010-05-21 ┆         ┆ false    │
│ j          ┆        ┆ P          ┆ I          ┆   ┆ T00:00:00. ┆ T00:00:00. ┆         ┆          │
│            ┆        ┆            ┆            ┆   ┆ 000+0000   ┆ 000+0000   ┆         ┆          │
│ h8vtacmZL5 ┆ 202203 ┆ f90eISKFm7 ┆ TDb5JyDQqh ┆ … ┆ 2010-05-21 ┆ 2010-05-21 ┆         ┆ false    │
│ j          ┆        ┆ P          ┆ o          ┆   ┆ T00:00:00. ┆ T00:00:00. ┆         ┆          │
│            ┆        ┆            ┆            ┆   ┆ 000+0000   ┆ 000+0000   ┆         ┆          │
│ h8vtacmZL5 ┆ 202203 ┆ f90eISKFm7 ┆ y1jbXYIuub ┆ … ┆ 2010-05-21 ┆ 2010-05-21 ┆         ┆ false    │
│ j          ┆        ┆ P          ┆ N          ┆   ┆ T00:00:00. ┆ T00:00:00. ┆         ┆          │
│            ┆        ┆            ┆            ┆   ┆ 000+0000   ┆ 000+0000   ┆         ┆          │
│ h8vtacmZL5 ┆ 202203 ┆ f90eISKFm7 ┆ x1Ti1RoTKF ┆ … ┆ 2010-05-21 ┆ 2010-05-21 ┆         ┆ false    │
│ j          ┆        ┆ P          ┆ r          ┆   ┆ T00:00:00. ┆ T00:00:00. ┆         ┆          │
│            ┆        ┆            ┆            ┆   ┆ 000+0000   ┆ 000+0000   ┆         ┆          │
└────────────┴────────┴────────────┴────────────┴───┴────────────┴────────────┴─────────┴──────────┘
```

##### [Analytics](#)

Aggregated data from the Analytics tables can be read using the `DHIS2.analytics.get()` method. The method accepts the following arguments:

* **`data_elements`** : *list of str, optional*<br>
    Data element identifiers

* **`data_element_groups`** : *list of str, optional*<br>
    Data element groups identifiers

* **`indicators`**: *list of str, optional*<br>
    Indicator identifiers

* **`indicator_groups`**: *list of str, optional*<br>
    Indicator groups identifiers

* **`periods`** : *list of str, optional*<br>
    Period identifiers in ISO format

* **`org_units`** : *list of str, optional*<br>
    Organisation units identifiers

* **`org_unit_groups`** : *list of str, optional*<br>
    Organisation unit groups identifiers

* **`org_unit_levels`** : *list of int, optional*<br>
    Organisation unit levels

* **`include_cocs`** : *bool, optional (default=True)*<br>
    Include category option combos in response

At least 3 arguments must be provided:
* One in the data dimension (`data_elements`, `data_element_groups`, `indicators` or `indicator_groups`)
* One in the spatial dimension (`org_units`, `org_unit_groups` or `org_unit_levels`)
* One in the temporal dimension (`periods`)

Data values are returned in a JSON-like list of dictionaries that can be converted into a Pandas or Polars dataframe.

```python
>>> import polars as pl
>>> from openhexa.sdk import workspace
>>> from openhexa.toolbox.dhis2 import DHIS2

>>> # initialize a new connection in an OpenHEXA workspace
>>> con = workspace.dhis2_connection("DHIS2_PLAY")
>>> dhis = DHIS2(con, cache_dir=".cache")

>>> data_values = play.analytics.get(
...     data_elements=["V37YqbqpEhV", "tn3p7vIxoKY", "HZSdnO5fCUc"],
...     org_units=["JQr6TJx5KE3", "KbO0JnhiMwl", "f90eISKFm7P"],
...     periods=["202201", "202202", "202203"]
... )

>>> df = pl.DataFrame(data_values)
>>> print(df)

shape: (14, 5)
┌─────────────┬─────────────┬─────────────┬────────┬───────┐
│ dx          ┆ co          ┆ ou          ┆ pe     ┆ value │
│ ---         ┆ ---         ┆ ---         ┆ ---    ┆ ---   │
│ str         ┆ str         ┆ str         ┆ str    ┆ str   │
╞═════════════╪═════════════╪═════════════╪════════╪═══════╡
│ V37YqbqpEhV ┆ PT59n8BQbqM ┆ JQr6TJx5KE3 ┆ 202201 ┆ 5     │
│ V37YqbqpEhV ┆ pq2XI5kz2BY ┆ f90eISKFm7P ┆ 202201 ┆ 4     │
│ V37YqbqpEhV ┆ PT59n8BQbqM ┆ f90eISKFm7P ┆ 202201 ┆ 11    │
│ V37YqbqpEhV ┆ pq2XI5kz2BY ┆ JQr6TJx5KE3 ┆ 202201 ┆ 2     │
│ …           ┆ …           ┆ …           ┆ …      ┆ …     │
│ V37YqbqpEhV ┆ pq2XI5kz2BY ┆ KbO0JnhiMwl ┆ 202203 ┆ 12    │
│ V37YqbqpEhV ┆ pq2XI5kz2BY ┆ JQr6TJx5KE3 ┆ 202203 ┆ 5     │
│ V37YqbqpEhV ┆ PT59n8BQbqM ┆ JQr6TJx5KE3 ┆ 202203 ┆ 8     │
│ V37YqbqpEhV ┆ pq2XI5kz2BY ┆ f90eISKFm7P ┆ 202203 ┆ 13    │
└─────────────┴─────────────┴─────────────┴────────┴───────┘
```

#### [Write data](#)

In developement.

### [Periods](#)

Helper classes and methods to deal with DHIS2 periods are available in the `openhexa.toolbox.dhis2.periods` module.

```python
>>> from openhexa.toolbox.dhis2.periods import Month, Quarter, period_from_string

>>> m1 = Month("202211")
>>> m2 = Month("202302")
>>> m2 > m1
True

>>> m1.get_range(m2)
["202211", "202212", "202301", "202302"]

>>> q1 = Quarter("2022Q3")
>>> q2 = Quarter("2023Q2")
>>> q1.get_range(q2)
["2022Q3", "2022Q4", "2023Q1", "2023Q2"]

>>> period_from_string("2022Q3") == q1
True
```