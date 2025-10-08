**ERA5 Toolbox**

Package for downloading, processing, and aggregating ERA5-Land reanalysis data from the ECMWF Climate Data Store (CDS).

- [Overview](#overview)
- [Data Source](#data-source)
- [Data Flow](#data-flow)
- [Usage](#usage)
  - [Data Acquisition](#data-acquisition)
  - [Data Aggregation](#data-aggregation)
- [Supported variables](#supported-variables)
- [Zarr store](#zarr-store)
  - [Why Zarr instead of GRIB?](#why-zarr-instead-of-grib)
  - [How is the Zarr store managed?](#how-is-the-zarr-store-managed)
  - [Reading data from the Zarr store](#reading-data-from-the-zarr-store)

## Overview

This package provides tools to:
- Download ERA5-Land hourly data from ECMWF's Climate Data Store
- Convert GRIB files to analysis-ready Zarr format
- Perform spatial aggregation using geographic boundaries
- Aggregate data temporally across various periods (daily, weekly, monthly, yearly)
- Support DHIS2-compatible weekly periods (standard, Wednesday, Thursday, Saturday, Sunday weeks)

## Data Source

ERA5-Land is a reanalysis dataset providing hourly estimates of land variables from 1950 to present at 9km resolution. Data is accessed via the [ECMWF Climate Data Store](https://cds.climate.copernicus.eu/).

**Requirements:**
- CDS API account and credentials
- Dataset license accepted in the CDS

## Data Flow

```mermaid
flowchart LR
    CDS[(ECMWF CDS)] --> GRIB[GRIB Files] --> ZARR[Zarr Store] --> PROCESS[Aggregate]

    style CDS fill:#e1f5fe
    style ZARR fill:#f3e5f5
```

## Usage

### Data Acquisition

Use `prepare_requests()` to build data requests for a specific variable and time range.
If the Zarr store already contains data, only missing data will be requested. If the
Zarr store does not exist, all data in the range will be requested and the store
created.

```python
from pathlib import Path

from ecmwf.datastores.client import Client
from era5.extract import prepare_requests, submit_requests, retrieve_requests, grib_to_zarr

client = Client(url=CDS_API_URL, key=CDS_API_KEY)
zarr_store = Path("data/2m_temperature.zarr")

# Prepare and chunk data requests
# Existing data in the zarr store will not be requested
requests = prepare_requests(
    client,
    dataset_id="reanalysis-era5-land",
    start_date=date(2025, 3, 1),
    end_date=date(2025, 9, 10),
    variable="2m_temperature",
    area=[12, -2, 8, 2],  # North, West, South, East
    zarr_store=zarr_store
)

raw_dir = Path("data/2m_temperature/raw")
raw_dir.mkdir(parents=True, exist_ok=True)

# Retrieve data requests when they are ready
# This will download raw GRIB files to `raw_dir`
retrieve_requests(
    client,
    dataset_id="reanalysis-era5-land",
    requests=requests,
    dst_dir=raw_dir,
)

# Convert raw GRIB data to Zarr format
# NB: The zarr store will be created if it does not already existed
grib_to_zarr(raw_dir, zarr_store)
```

### Data Aggregation

Use `aggregate_in_space()` to perform spatial aggregation.

```python
import geopandas as gpd
from era5.transform import create_masks, aggregate_in_space

boundaries = gpd.read_file("boundaries.geojson")
dataset = xr.open_zarr(zarr_store, decode_timedelta=True)

# Create spatial masks for aggregation
masks = create_masks(
    gdf=boundaries,
    id_column="boundary_id",
    ds=dataset
)

# Convert from hourly to daily data 1st
daily = dataset.mean(dim="step")

# Aggregate spatially
results = aggregate_in_space(
    ds=daily,
    masks=masks,
    variable="t2m",
    agg="mean"
)
print(results)
```

```
shape: (36, 3)
┌──────────┬────────────┬────────────┐
│ boundary ┆ time       ┆ value      │
│ ---      ┆ ---        ┆ ---        │
│ str      ┆ date       ┆ f64        │
╞══════════╪════════════╪════════════╡
│ geom1    ┆ 2025-03-28 ┆ 305.402924 │
│ geom1    ┆ 2025-03-29 ┆ 306.365845 │
│ geom1    ┆ 2025-03-30 ┆ 306.80304  │
│ geom1    ┆ 2025-03-31 ┆ 307.176575 │
│ geom1    ┆ 2025-04-01 ┆ 306.338745 │
│ …        ┆ …          ┆ …          │
│ geom4    ┆ 2025-04-01 ┆ 305.957886 │
│ geom4    ┆ 2025-04-02 ┆ 306.503937 │
│ geom4    ┆ 2025-04-03 ┆ 305.563995 │
│ geom4    ┆ 2025-04-04 ┆ 306.381927 │
│ geom4    ┆ 2025-04-05 ┆ 307.367096 │
└──────────┴────────────┴────────────┘
```

Use `aggregate_in_time()` to perform temporal aggregation.

```python
from era5.transform import Period, aggregate_in_time

# Aggregate to weekly periods
weekly_data = aggregate_in_time(
    results,
    period=Period.WEEK,
    agg="mean"
)

# DHIS2-compatible Sunday weeks
sunday_weekly = aggregate_in_time(
    results,
    period=Period.WEEK_SUNDAY,
    agg="mean"
)

print(sunday_weekly)
```
```
shape: (8, 3)
┌──────────┬────────────┬────────────┐
│ boundary ┆ period     ┆ value      │
│ ---      ┆ ---        ┆ ---        │
│ str      ┆ str        ┆ f64        │
╞══════════╪════════════╪════════════╡
│ geom1    ┆ 2025WedW13 ┆ 306.417426 │
│ geom1    ┆ 2025WedW14 ┆ 307.149551 │
│ geom2    ┆ 2025WedW13 ┆ 306.327582 │
│ geom2    ┆ 2025WedW14 ┆ 306.987686 │
│ geom3    ┆ 2025WedW13 ┆ 306.03266  │
│ geom3    ┆ 2025WedW14 ┆ 306.774063 │
│ geom4    ┆ 2025WedW13 ┆ 305.77348  │
│ geom4    ┆ 2025WedW14 ┆ 306.454239 │
└──────────┴────────────┴────────────┘
```

## Supported variables

The package supports the following ERA5-Land variables:

```
[10m_u_component_of_wind]
name = "10m_u_component_of_wind"
short_name = "u10"
unit = "m s**-1"
time = ["01:00", "07:00", "13:00", "19:00"]

[10m_v_component_of_wind]
name = "10m_v_component_of_wind"
short_name = "v10"
unit = "m s**-1"
time = ["01:00", "07:00", "13:00", "19:00"]

[2m_dewpoint_temperature]
name = "2m_dewpoint_temperature"
short_name = "d2m"
unit = "K"
time = ["01:00", "07:00", "13:00", "19:00"]

[2m_temperature]
name = "2m_temperature"
short_name = "t2m"
unit = "K"
time = ["01:00", "07:00", "13:00", "19:00"]

[runoff]
name = "runoff"
short_name = "ro"
unit = "m"
time = ["00:00"]

[soil_temperature_level_1]
name = "soil_temperature_level_1"
short_name = "stl1"
unit = "K"
time = ["01:00", "07:00", "13:00", "19:00"]

[volumetric_soil_water_layer_1]
name = "volumetric_soil_water_layer_1"
short_name = "swvl1"
unit = "m**3 m**-3"
time = ["01:00", "07:00", "13:00", "19:00"]

[volumetric_soil_water_layer_2]
name = "volumetric_soil_water_layer_2"
short_name = "swvl2"
unit = "m**3 m**-3"
time = ["01:00", "07:00", "13:00", "19:00"]

[total_precipitation]
name = "total_precipitation"
short_name = "tp"
unit = "m"
time = ["00:00"]

[total_evaporation]
name = "total_evaporation"
short_name = "e"
unit = "m"
time = ["00:00"]
```

See [documentation](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land) for details.

## Zarr store

### Why Zarr instead of GRIB?

The package converts GRIB files to Zarr format for several reasons:

1. **Efficient data access**: Zarr provides chunked, compressed storage that allows reading specific temporal/spatial subsets without loading entire files
2. **Cloud-optimized**: Unlike GRIB files which require sequential reading, Zarr enables parallel and partial reads, ideal for cloud storage
3. **Consolidated metadata**: All metadata is stored in a single `.zmetadata` file, making dataset discovery instant
4. **Append-friendly**: New time steps can be efficiently appended without rewriting existing data
5. **Analysis-ready**: Direct integration with xarray makes the data immediately usable for scientific computing

### How is the Zarr store managed?

The ERA5 toolbox implements the following data pipeline:

1. **Initial download**: GRIB files from CDS are treated as temporary artifacts
2. **Conversion**: `grib_to_zarr()` converts GRIB to Zarr, handling:
   - Automatic creation of new stores
   - Appending to existing stores without duplicating time steps
   - Metadata consolidation for optimal performance
3. **Incremental updates**: When requesting new data, the package:
   - Checks existing time coverage in the Zarr store
   - Only downloads missing time periods
   - Appends new data

### Reading data from the Zarr store

```python
import xarray as xr

# Open the zarr store (lazy loading - no data read yet)
ds = xr.open_zarr("data/2m_temperature.zarr", consolidated=True)

# Explore the dataset structure
print(ds)  # Shows dimensions, coordinates, and variables
print(ds.time.values)  # Time range available

# Access specific time ranges
subset = ds.sel(time=slice("2025-01", "2025-03"))

# Load only specific variables
temperature = ds["t2m"]  # Still lazy
temp_values = temperature.values  # Triggers actual data read

# Spatial subsetting
region = ds.sel(latitude=slice(10, 5), longitude=slice(-1, 2))

# Time aggregation (hourly to daily)
daily_mean = ds.resample(time="1D").mean()

# Direct computation without loading everything
monthly_max = ds["t2m"].resample(time="1M").max().compute()
```
