"""Spatial aggregation of ERA5-Land data."""

import logging
from enum import StrEnum
from typing import Literal

import geopandas as gpd
import numpy as np
import polars as pl
import rasterio.features
import rasterio.transform
import xarray as xr

from openhexa.toolbox.era5.dhis2weeks import WeekType, to_dhis2_week

logger = logging.getLogger(__name__)


def create_masks(gdf: gpd.GeoDataFrame, id_column: str, ds: xr.Dataset) -> xr.DataArray:
    """Create masks for each boundary in the GeoDataFrame.

    Input polygons are rasterized into a grid matching the spatial dimensions of the
    dataset.
    We use the `all_touched=True` option, so that any pixel touched by a polygon is included in the
    mask. This is because we don't want small geometries ending up with zero pixel. As a result,
    each polygon has its own mask because some pixels may belong to multiple polygons.

    Args:
        gdf: A GeoDataFrame containing the boundaries, with a 'geometry' column
        id_column: Column in the GeoDataFrame that contains unique identifiers for each
        boundary
        ds: An xarray Dataset containing the spatial dimensions (latitude and longitude)

    Returns:
        An xarray DataArray with dimensions ['boundary', 'latitude', 'longitude']
        containing the masks. Each mask corresponds to a boundary in the GeoDataFrame.

    """
    logger.debug("Creating masks for %s boundaries", len(gdf))
    lat = ds.latitude.values
    lon = ds.longitude.values
    lat_res = abs(lat[1] - lat[0])
    lon_res = abs(lon[1] - lon[0])
    transform = rasterio.transform.from_bounds(  # type: ignore
        west=lon.min() - lon_res / 2,
        east=lon.max() + lon_res / 2,
        north=lat.max() + lat_res / 2,
        south=lat.min() - lat_res / 2,
        width=len(lon),
        height=len(lat),
    )

    masks: list[np.ndarray] = []
    names: list[str] = []

    for _, row in gdf.iterrows():
        mask = rasterio.features.rasterize(  # type: ignore
            [row.geometry],
            out_shape=(len(lat), len(lon)),
            transform=transform,  # type: ignore
            fill=0,
            all_touched=True,
            dtype=np.uint8,
        )
        masks.append(mask)  # type: ignore
        names.append(row[id_column])  # type: ignore

    logger.debug("Created masks with shape %s", (len(masks), len(lat), len(lon)))

    return xr.DataArray(
        np.stack(masks),
        dims=["boundary", "latitude", "longitude"],
        coords={
            "boundary": names,
            "latitude": lat,
            "longitude": lon,
        },
    )


def aggregate_in_space(
    ds: xr.Dataset,
    masks: xr.DataArray,
    data_var: str,
    agg: Literal["mean", "sum", "min", "max"],
) -> pl.DataFrame:
    """Perform spatial aggregation on the dataset using the provided masks.

    Args:
        ds: The data containing the variable to aggregate. Dataset is expected to have
            'latitude' and 'longitude' coordinates, and daily data.
        masks: An xarray DataArray containing the masks for spatial aggregation, as returned by the
            `create_masks()` function.
        data_var: Name of the variable to aggregate in input dataset (e.g. "t2m")
        agg: Spatial aggregation method (one of "mean", "sum", "min", "max").

    Returns:
        A Polars DataFrame of shape (n_boundaries, n_days) with columns: "boundary", "time", and
        "value".

    Raises:
        ValueError: If the specified variable is not found in the dataset.
        ValueError: If the dataset still contains the 'step' dimension (i.e. data is not daily).
        ValueError: If an unsupported aggregation method is specified.

    """
    logger.debug("Aggregating data for variable '%s' using masks", data_var)
    if data_var not in ds.data_vars:
        msg = f"Variable '{data_var}' not found in dataset"
        raise ValueError(msg)
    if "step" in ds.dims:
        msg = "Dataset still contains 'step' dimension. Please aggregate to daily data first."
        raise ValueError(msg)
    da = ds[data_var]
    area_weights = np.cos(np.deg2rad(ds.latitude))
    results: list[xr.DataArray] = []
    for boundary in masks.boundary:
        mask = masks.sel(boundary=boundary)
        if agg == "mean":
            weights = area_weights * mask
            result = da.weighted(weights).mean(["latitude", "longitude"])
        elif agg == "sum":
            result = da.where(mask > 0).sum(["latitude", "longitude"])
        elif agg == "min":
            result = da.where(mask > 0).min(["latitude", "longitude"])
        elif agg == "max":
            result = da.where(mask > 0).max(["latitude", "longitude"])
        else:
            msg = f"Unsupported aggregation method: {agg}"
            raise ValueError(msg)
        results.append(result)
    result = xr.concat(results, dim="boundary").assign_coords(boundary=masks.boundary, time=ds.time)

    n_boundaries = len(result.boundary)
    n_times = len(result.time)

    schema = {
        "boundary": pl.String,
        "time": pl.Date,
        "value": pl.Float64,
    }
    data = {
        "boundary": np.repeat(result.boundary.values, n_times),
        "time": np.tile(result.time.values, n_boundaries),
        "value": result.values.flatten(order="C"),
    }
    return pl.DataFrame(data, schema=schema)


class Period(StrEnum):
    """Temporal aggregation periods."""

    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    YEAR = "YEAR"
    WEEK_WEDNESDAY = "WEEK_WEDNESDAY"
    WEEK_THURSDAY = "WEEK_THURSDAY"
    WEEK_SATURDAY = "WEEK_SATURDAY"
    WEEK_SUNDAY = "WEEK_SUNDAY"


def aggregate_in_time(
    dataframe: pl.DataFrame,
    period: Period,
    agg: Literal["mean", "sum", "min", "max"] = "mean",
) -> pl.DataFrame:
    """Aggregate the dataframe over the specified temporal period.

    Args:
        dataframe: The dataframe to aggregate.
        period: The temporal period to aggregate over.
        agg: Temporal aggregation method (one of "mean", "sum", "min", "max").

    Returns:
        The aggregated dataframe.

    """
    logger.debug("Aggregating dataframe over period '%s' with method '%s'", period, agg)
    # We 1st create a "period" column to be able to group by it
    if period == Period.DAY:
        df = dataframe.with_columns(
            pl.col("time").dt.strftime("%Y%m%d").alias("period"),
        )
    elif period == Period.MONTH:
        df = dataframe.with_columns(
            pl.col("time").dt.strftime("%Y%m").alias("period"),
        )
    elif period == Period.YEAR:
        df = dataframe.with_columns(
            pl.col("time").dt.strftime("%Y").alias("period"),
        )
    elif period in (
        Period.WEEK,
        Period.WEEK_WEDNESDAY,
        Period.WEEK_THURSDAY,
        Period.WEEK_SATURDAY,
        Period.WEEK_SUNDAY,
    ):
        df = dataframe.with_columns(
            pl.col("time")
            .map_elements(lambda dt: to_dhis2_week(dt, WeekType(period)), return_dtype=pl.String)
            .alias("period"),
        )
    else:
        msg = f"Unsupported period: {period}"
        raise NotImplementedError(msg)

    if agg == "mean":
        df = df.group_by(["boundary", "period"]).agg(pl.col("value").mean().alias("value"))
    elif agg == "sum":
        df = df.group_by(["boundary", "period"]).agg(pl.col("value").sum().alias("value"))
    elif agg == "min":
        df = df.group_by(["boundary", "period"]).agg(pl.col("value").min().alias("value"))
    elif agg == "max":
        df = df.group_by(["boundary", "period"]).agg(pl.col("value").max().alias("value"))
    else:
        msg = f"Unsupported aggregation method: {agg}"
        raise ValueError(msg)

    return df.select(["boundary", "period", "value"]).sort(["boundary", "period"])


def calculate_relative_humidity(t2m: xr.DataArray, d2m: xr.DataArray) -> xr.Dataset:
    """Calculate relative humidity from 2m temperature and 2m dewpoint temperature.

    Uses Magnus formula to calculate RH from t2m and d2m.

    Args:
        t2m: 2m temperature in Kelvin.
        d2m: 2m dewpoint temperature in Kelvin.

    Returns:
        Relative humidity in percentage.
    """
    t2m_c = t2m - 273.15
    d2m_c = d2m - 273.15

    a = 17.1  # temperature coefficient
    b = 235.0  # temperature offset (°C)
    base_pressure = 6.1078
    vapor_pressure = base_pressure * np.exp(a * d2m_c / (b + d2m_c))
    sat_vapor_pressure = base_pressure * np.exp(a * t2m_c / (b + t2m_c))
    rh = vapor_pressure / sat_vapor_pressure
    rh = rh.clip(0, 1)
    rh_da = xr.DataArray(
        rh * 100,
        dims=t2m.dims,
        coords=t2m.coords,
        attrs={"units": "%"},
    )
    return xr.Dataset({"rh": rh_da})


def calculate_wind_speed(u10: xr.DataArray, v10: xr.DataArray) -> xr.Dataset:
    """Calculate wind speed from u10 and v10 components.

    Args:
        u10: U component of wind at 10m in m/s.
        v10: V component of wind at 10m in m/s.

    Returns:
        Wind speed in m/s.
    """
    wind_speed = np.sqrt(u10**2 + v10**2)
    wind_speed_da = xr.DataArray(
        wind_speed,
        dims=u10.dims,
        coords=u10.coords,
        name="ws",
        attrs={"units": "m/s"},
    )
    return xr.Dataset({"ws": wind_speed_da})
