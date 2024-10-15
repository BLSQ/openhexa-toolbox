# OpenHEXA Toolbox ERA5

The package contains ETL classes and functions to acquire and process ERA5-Land data. ERA5-Land
provides hourly information of surface variables from 1950 to 5 days before the current date, with
a ~9 km spatial resolution. See [ERA5-Land: data
documentation](https://confluence.ecmwf.int/display/CKB/ERA5-Land%3A+data+documentation) for more
information.

## Usage

The package contains 3 modules:
* `openhexa.toolbox.era5.cds`: download ERA5-land products from the Copernicus [Climate Data Store](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=overview)
* `openhexa.toolbox.era5.google`: download ERA5 products from Google Cloud [Public Datasets](https://cloud.google.com/storage/docs/public-datasets/era5)
* `openhexa.toolbox.era5.aggregate`: aggregate ERA5 data in space and time

### Download from CDS

To download products from the Climate Data Store, you will need to create an account and generate an API key in ECMWF (see [CDS](https://cds.climate.copernicus.eu/)).

```python
from openhexa.toolbox.era5.cds import Client

cds = Client(key="<cds_api_key>")

request = cds.build_request(
    variable="2m_temperature",
    year=2024,
    month=4
)

cds.download(
    request=request,
    dst_file="data/product.grib"
)
```

The module also contains helper functions to use bounds from a geoparquet file as an area of interest.

```python
bounds = bounds_from_file(fp=Path("data/districts.parquet"), buffer=0.5)

request = cds.build_request(
    variable="total_precipitation",
    year=2023,
    month=10,
    days=[1, 2, 3, 4, 5],
    area=bounds
)

cds.download(
    request=request,
    dst_file="data/product.grib"
)
```

To download multiple products for a given period, use `Client.download_between()`:

```python
cds.download_between(
    variable="2m_temperature",
    start=datetime(2020, 1, 1),
    end=datetime(2021, 6, 1),
    dst_dir="data/raw/2m_temperature",
    area=bounds
)
```

Checking latest available date in the ERA5-Land dataset:

```python
cds = Client("<api_key>")

cds.latest
```
```
>>> datetime(2024, 10, 8)
```

### Download from Google Cloud

```python
from openhexa.toolbox.era5.google import Client

google = Client()

google.download(
    variable="2m_temperature",
    date=datetime(2024, 6, 15),
    dst_file="data/product.nc"
)
```

Or to download all products for a given period:

```python
# if products are already presents in dst_dir, they will be skipped
google.sync(
    variable="2m_temperature",
    start_date=datetime(2022, 1, 1),
    end_date=datetime(2022, 6, 1),
    dst_dir="data"
)
```

### Aggregation

```python
from pathlib import Path

import geopandas as gpd
from openhexa.toolbox.era5.aggregate import build_masks, merge, aggregate, get_transform

boundaries = gpd.read_parquet("districts.parquet")
data_dir = Path("data/era5/total_precipitation")

ds = merge(data_dir)

ncols = len(ds.longitude)
nrows = len(ds.latitude)
transform = get_transform(ds)
masks = build_masks(boundaries, nrows, ncols, transform)

df = aggregate(
    ds=ds,
    var="tp",
    masks=masks,
    boundaries_id=[uid for uid in boundaries["district_id"]]
)

print(df)
```
```
shape: (18_410, 5)
┌─────────────┬────────────┬───────────┬──────────┬───────────┐
│ boundary_id ┆ date       ┆ mean      ┆ min      ┆ max       │
│ ---         ┆ ---        ┆ ---       ┆ ---      ┆ ---       │
│ str         ┆ date       ┆ f64       ┆ f64      ┆ f64       │
╞═════════════╪════════════╪═══════════╪══════════╪═══════════╡
│ mPenE8ZIBFC ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ TPgpGxUBU9y ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ AhST5ZpuCDJ ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ Lp2BjBVT63s ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ EdfRX9b9vEb ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ yhs1ecKsLOc ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ iHSJypSwlo5 ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ CTtB0TPRvWc ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ eVFAuZOzogt ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ WVEJjdJ2S15 ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ rbYGKFgupK9 ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ Nml6rVDElLh ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ E0hd8TD1M0q ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ PCg4pLGmKSM ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ C6EBhE8OnfW ┆ 2024-01-01 ┆ 0.000462  ┆ 0.0      ┆ 0.00086   │
│ …           ┆ …          ┆ …         ┆ …        ┆ …         │
│ CkpfOFkMyrd ┆ 2024-10-07 ┆ 1.883121  ┆ 0.001785 ┆ 2.700447  │
│ tMXsltjzzmR ┆ 2024-10-07 ┆ 3.579136  ┆ 0.105436 ┆ 4.702504  │
│ F0ytkh0RExg ┆ 2024-10-07 ┆ 8.415455  ┆ 0.838535 ┆ 17.08884  │
...
│ TTSmaRnHa82 ┆ 2024-10-07 ┆ 1.724243  ┆ 0.007809 ┆ 5.692989  │
│ jbmw2gdrrTV ┆ 2024-10-07 ┆ 1.176629  ┆ 0.110173 ┆ 1.582995  │
│ eKYyXbBdvmB ┆ 2024-10-07 ┆ 0.599976  ┆ 0.037771 ┆ 1.189411  │
└─────────────┴────────────┴───────────┴──────────┴───────────┘
```