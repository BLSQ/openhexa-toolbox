# OpenHEXA Toolbox ERA5

Download and process ERA5-Land climate reanalysis data from the [Copernicus Climate Data
Store](https://www.google.com/url?sa=t&source=web&rct=j&opi=89978449&url=https://cds.climate.copernicus.eu/&ved=2ahUKEwi0x-Pl4aqQAxUnRKQEHftaGdAQFnoECBEQAQ&usg=AOvVaw1BwvwpB-Kja5hnXP6DTcbl)
(CDS).

- [Overview](#overview)
- [Installation](#installation)
- [Supported variables](#supported-variables)
- [Usage](#usage)
  - [Prepare and retrieve data requests](#prepare-and-retrieve-data-requests)
  - [Move GRIB files into a Zarr store](#move-grib-files-into-a-zarr-store)
  - [Read climate data from a Zarr store](#read-climate-data-from-a-zarr-store)
  - [Aggregate climate data stored in a Zarr store](#aggregate-climate-data-stored-in-a-zarr-store)
- [Tests](#tests)

## Overview

The package provides tools to:
- Download ERA5-Land hourly data from ECMWF's Climate Data Store
- Convert GRIB files to analysis-ready Zarr format
- Perform spatial aggregation using geographic boundaries
- Aggregate data temporally across various periods (daily, weekly, monthly, yearly)
- Support DHIS2-compatible weekly periods (standard, Wednesday, Thursday, Saturday, Sunday weeks)

## Installation

With pip:

```bash
pip install openhexa.toolbox[all]
# Or
pip install openhexa.toolbox[era5]
```

With uv:

```bash
uv add openhexa.toolbox --extra all
# Or
uv add openhexa.toolbox --extra era5
```

## Supported variables

The module supports a subset of ERA5-Land variables commonly used in health:

- 10m u-component of wind (`u10`)
- 10m v-component of wind (`v10`)
- 2m dewpoint temperature (`d2m`)
- 2m temperature (`t2m`)
- Runoff (`ro`)
- Soil temperature level 1 (`stl1`)
- Volumetric soil water layer 1 (`swvl1`)
- Volumetric soil water layer 2 (`swvl2`)
- Total precipitation (`tp`)
- Total evaporation (`e`)

When fetching hourly data, we sample instantaneous variable at 4 daily steps: 01:00,
07:00, 13:00 and 19:00. For accumulated variables (e.g. total precipitation), we only
retrieve totals at the end of each day.

See [variables.toml](/openhexa/toolbox/era5/data/variables.toml) for more details on
supported variables.

## Usage

### Prepare and retrieve data requests

Download ERA5-Land data from the CDS API. You'll need to set up your CDS API credentials
first (see [CDS API setup](https://cds.climate.copernicus.eu/how-to-api)) and accept the
license of the dataset you want to download.

```python
from datetime import date
from pathlib import Path
from ecmwf.datastores import Client
from openhexa.toolbox.era5.extract import prepare_requests, retrieve_requests
import os

client = Client(url=os.getenv("CDS_API_URL"), key=os.getenv("CDS_API_KEY"))

# Prepare the data requests that need to be submitted to the CDS
# If data already exists in the destination zarr store, it will not be requested again
# NB: At this point, no data is moved to the Zarr store - it is used to avoid
# downloading data we already have
requests = prepare_requests(
    client=client,
    dataset_id="reanalysis-era5-land",
    start_date=date(2025, 3, 28),
    end_date=date(2025, 4, 5),
    variable="2m_temperature",
    area=[10, -1, 8, 1],  # [north, west, south, east] in degrees
    zarr_store=Path("data/2m_temperature.zarr"),
)

# Submit data requests and retrieve data in GRIB format as they are ready
# Depending on request size and server load, this may take a while
retrieve_requests(
    client=client,
    dataset_id="reanalysis-era5-land",
    requests=requests,
    dst_dir=Path("data/raw"),
    wait=30,  # Check every 30 seconds for completed requests
)
```

### Move GRIB files into a Zarr store

Convert downloaded GRIB files into an analysis-ready Zarr store for efficient access.

```python
from pathlib import Path
from openhexa.toolbox.era5.extract import grib_to_zarr

grib_to_zarr(
    src_dir=Path("data/raw"),
    zarr_store=Path("data/2m_temperature.zarr"),
    data_var="t2m",  # Short name for 2m temperature
)
```

### Read climate data from a Zarr store

Data is stored in [Zarr](https://zarr.dev/) stores for efficient storage and access of
climate variables as N-dimensional arrays. You can read data in Zarr stores using
[xarray](https://xarray.dev/).

When opening a Zarr store, no data is loaded into memory yet. You can check the dataset
structure without loading the data.

```python
import xarray as xr

ds = xr.open_zarr("data/2m_temperature.zarr", consolidated=True)
print(ds)
```
```
<xarray.Dataset> Size: 7MB
Dimensions:    (latitude: 71, longitude: 91, time: 284)
Coordinates:
  * latitude   (latitude) float64 568B 16.0 15.9 15.8 15.7 ... 9.3 9.2 9.1 9.0
  * longitude  (longitude) float64 728B -6.0 -5.9 -5.8 -5.7 ... 2.7 2.8 2.9 3.0
  * time       (time) datetime64[ns] 2kB 2024-10-01T01:00:00 ... 2024-12-10T1...
Data variables:
    t2m        (latitude, longitude, time) float32 7MB ...
Attributes:
    Conventions:             CF-1.7
    GRIB_centre:             ecmf
    GRIB_centreDescription:  European Centre for Medium-Range Weather Forecasts
    GRIB_edition:            1
    GRIB_subCentre:          0
    history:                 2025-10-14T09:02 GRIB to CDM+CF via cfgrib-0.9.1...
    institution:             European Centre for Medium-Range Weather Forecasts
```

You can use real dates and coordinates to index the data.

```python
import xarray as xr

t2m = xr.open_zarr("data/2m_temperature.zarr", consolidated=True)
t2m_daily_mean = t2m.resample(time="1D").mean()
t2m_daily_mean.mean(dim=["latitude", "longitude"]).t2m.plot.line()
```

![ERA5 2m Temperature Daily Mean](/docs/images/era5_t2m_lineplot.png)

### Aggregate climate data stored in a Zarr store

Aggregate hourly climate data by administrative boundaries and time periods.

```python
from pathlib import Path
import geopandas as gpd
import xarray as xr
from openhexa.toolbox.era5.transform import (
    create_masks,
    aggregate_in_space,
    aggregate_in_time,
    Period,
)

t2m = xr.open_zarr("./2m_temperature.zarr", consolidated=True, decode_timedelta=False)
```

For instantaneous variables (e.g. 2m temperature, soil moisture...), hourly data should
be aggregated to daily 1st. In ERA5-Land data, data is structured along 2 temporal
dimensions: `time` and `step`. To aggregate hourly data to daily, you need to average over
the `step` dimension:

```python
t2m_daily = t2m.mean(dim="step")

# or to compute daily extremes
t2m_daily_max = t2m.max(dim="step")
t2m_daily_min = t2m.min(dim="step")
```

```python
import matplotlib.pyplot as plt

plt.imshow(
    t2m_daily.sel(time="2024-10-04").t2m,
    cmap="coolwarm",
)
plt.colorbar(label="Temperature (°C)", shrink=0.8)
plt.axis("off")
```
![2m temperature raster](/docs/images/era5_t2m_raster.png)

The module provides helper functions to help you perform spatial aggregation on gridded
ERA5 data. Use the `create_masks()` function to create raster masks from vector
boundaries. Raster masks uses the same grid as the ERA5 dataset.

```python
import geopandas as gpd
from openhexa.toolbox.era5.transform import create_masks

# Boundaries geographic file should use EPSG:4326 coordinate reference system (lat/lon)
boundaries = gpd.read_file("boundaries.gpkg")

masks = create_masks(
    gdf=boundaries,
    id_column="district_id",  # Column in the GeoDataFrame with unique boundary IDs
    ds=t2m_daily,
)
```

Example of raster mask for 1 vector boundary:

![Boundary vector](/docs/images/era5_boundary_vector.png)
![Boundary raster mask](/docs/images/era5_boundary_raster.png)

You can now aggregate daily gridded ERA5 data in space and time:

```python
from openhexa.toolbox.era5.transform import aggregate_in_space, aggregate_in_time, Period

# convert from Kelvin to Celsius
t2m_daily = t2m_daily - 273.15

t2m_agg = aggregate_in_space(
    ds=t2m_daily,
    masks=masks,
    variable="t2m",
    agg="mean",
)
print(t2m_agg)
```

```
shape: (4_970, 3)
┌─────────────┬────────────┬───────────┐
│ boundary    ┆ time       ┆ value     │
│ ---         ┆ ---        ┆ ---       │
│ str         ┆ date       ┆ f64       │
╞═════════════╪════════════╪═══════════╡
│ mPenE8ZIBFC ┆ 2024-10-01 ┆ 26.534632 │
│ mPenE8ZIBFC ┆ 2024-10-02 ┆ 25.860088 │
│ mPenE8ZIBFC ┆ 2024-10-03 ┆ 26.068018 │
│ mPenE8ZIBFC ┆ 2024-10-04 ┆ 26.103462 │
│ mPenE8ZIBFC ┆ 2024-10-05 ┆ 24.362678 │
│ …           ┆ …          ┆ …         │
│ eKYyXbBdvmB ┆ 2024-12-06 ┆ 25.130324 │
│ eKYyXbBdvmB ┆ 2024-12-07 ┆ 24.946449 │
│ eKYyXbBdvmB ┆ 2024-12-08 ┆ 24.840832 │
│ eKYyXbBdvmB ┆ 2024-12-09 ┆ 25.242334 │
│ eKYyXbBdvmB ┆ 2024-12-10 ┆ 26.697817 │
└─────────────┴────────────┴───────────┘
```

Likewise, to aggregate in time (e.g. weekly averages):

```python
t2m_weekly = aggregate_in_time(
    dataframe=t2m_agg,
    period=Period.WEEK,
    agg="mean",
)
print(t2m_weekly)
```

```
shape: (770, 3)
┌─────────────┬─────────┬───────────┐
│ boundary    ┆ period  ┆ value     │
│ ---         ┆ ---     ┆ ---       │
│ str         ┆ str     ┆ f64       │
╞═════════════╪═════════╪═══════════╡
│ AKVCJJ2TKSi ┆ 2024W40 ┆ 27.33611  │
│ AKVCJJ2TKSi ┆ 2024W41 ┆ 27.011093 │
│ AKVCJJ2TKSi ┆ 2024W42 ┆ 27.905081 │
│ AKVCJJ2TKSi ┆ 2024W43 ┆ 28.239824 │
│ AKVCJJ2TKSi ┆ 2024W44 ┆ 27.34595  │
│ …           ┆ …       ┆ …         │
│ yhs1ecKsLOc ┆ 2024W46 ┆ 27.711391 │
│ yhs1ecKsLOc ┆ 2024W47 ┆ 26.394333 │
│ yhs1ecKsLOc ┆ 2024W48 ┆ 24.863514 │
│ yhs1ecKsLOc ┆ 2024W49 ┆ 24.714464 │
│ yhs1ecKsLOc ┆ 2024W50 ┆ 24.923738 │
└─────────────┴─────────┴───────────┘
```

Or per week starting on Sundays:

``` python
t2m_sunday_week = aggregate_in_time(
    dataframe=t2m_agg,
    period=Period.WEEK_SUNDAY,
    agg="mean",
)
print(t2m_sunday_week)
```

```
shape: (770, 3)
┌─────────────┬────────────┬───────────┐
│ boundary    ┆ period     ┆ value     │
│ ---         ┆ ---        ┆ ---       │
│ str         ┆ str        ┆ f64       │
╞═════════════╪════════════╪═══════════╡
│ AKVCJJ2TKSi ┆ 2024SunW40 ┆ 27.898345 │
│ AKVCJJ2TKSi ┆ 2024SunW41 ┆ 26.483939 │
│ AKVCJJ2TKSi ┆ 2024SunW42 ┆ 27.9347   │
│ AKVCJJ2TKSi ┆ 2024SunW43 ┆ 28.291441 │
│ AKVCJJ2TKSi ┆ 2024SunW44 ┆ 27.510819 │
│ …           ┆ …          ┆ …         │
│ yhs1ecKsLOc ┆ 2024SunW46 ┆ 27.691862 │
│ yhs1ecKsLOc ┆ 2024SunW47 ┆ 26.316256 │
│ yhs1ecKsLOc ┆ 2024SunW48 ┆ 25.249807 │
│ yhs1ecKsLOc ┆ 2024SunW49 ┆ 24.751227 │
│ yhs1ecKsLOc ┆ 2024SunW50 ┆ 24.542277 │
└─────────────┴────────────┴───────────┘
```

Or per month:

``` python
t2m_monthly = aggregate_in_time(
    dataframe=t2m_agg,
    period=Period.MONTH,
    agg="mean",
)
print(t2m_monthly)
```

```
shape: (210, 3)
┌─────────────┬────────┬───────────┐
│ boundary    ┆ period ┆ value     │
│ ---         ┆ ---    ┆ ---       │
│ str         ┆ str    ┆ f64       │
╞═════════════╪════════╪═══════════╡
│ AKVCJJ2TKSi ┆ 202410 ┆ 27.615368 │
│ AKVCJJ2TKSi ┆ 202411 ┆ 26.527692 │
│ AKVCJJ2TKSi ┆ 202412 ┆ 25.080745 │
│ AVb6wBstPAo ┆ 202410 ┆ 29.747595 │
│ AVb6wBstPAo ┆ 202411 ┆ 26.137431 │
│ …           ┆ …      ┆ …         │
│ vQ6AJUeqBpc ┆ 202411 ┆ 25.915338 │
│ vQ6AJUeqBpc ┆ 202412 ┆ 23.130632 │
│ yhs1ecKsLOc ┆ 202410 ┆ 29.050539 │
│ yhs1ecKsLOc ┆ 202411 ┆ 26.628291 │
│ yhs1ecKsLOc ┆ 202412 ┆ 24.688542 │
└─────────────┴────────┴───────────┘
```

Note that the period column uses DHIS2 format (e.g. `2024W40` for week 40 of 2024).

## Tests

The module uses Pytest. To run tests, install development dependencies and execute
Pytest in the virtual environment.

```bash
uv sync --dev
uv run pytest tests/era5/*
```