"""Module for spatial and temporal aggregation of ERA5 data."""

from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import polars as pl
import rasterio
import xarray as xr
from epiweeks import Week
from rasterio.features import rasterize
from rasterio.transform import Affine, from_bounds


def clip_dataset(ds: xr.Dataset, xmin: float, ymin: float, xmax: float, ymax: float) -> xr.Dataset:
    """Clip input xarray dataset according to the provided bounding box.

    Assumes lat & lon dimensions are named "latitude" and "longitude". Longitude in the
    source dataset is expected to be in the range [0, 360], and will be converted to
    [-180, 180].

    Parameters
    ----------
    ds : xr.Dataset
        Input xarray dataset.
    xmin : float
        Minimum longitude.
    ymin : float
        Minimum latitude.
    xmax : float
        Maximum longitude.
    ymax : float
        Maximum latitude.

    Returns
    -------
    xr.Dataset
        Clipped xarray dataset.
    """
    ds = ds.assign_coords(longitude=(((ds.longitude + 180) % 360) - 180)).sortby("longitude")
    ds = ds.where((ds.longitude >= xmin) & (ds.longitude <= xmax), drop=True)
    ds = ds.where((ds.latitude >= ymin) & (ds.latitude <= ymax), drop=True)
    return ds


def get_transform(ds: xr.Dataset) -> Affine:
    """Get rasterio affine transform from xarray dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input xarray dataset.

    Returns
    -------
    Affine
        Rasterio affine transform.
    """
    transform = from_bounds(
        ds.longitude.values.min(),
        ds.latitude.values.min(),
        ds.longitude.values.max(),
        ds.latitude.values.max(),
        len(ds.longitude),
        len(ds.latitude),
    )
    return transform


def build_masks(
    boundaries: gpd.GeoDataFrame, height: int, width: int, transform: rasterio.Affine
) -> tuple[np.ndarray, rasterio.Affine]:
    """Build binary masks for all geometries in a dataframe.

    We build a raster of shape (n_boundaries, n_height, n_width) in order to store one binary mask
    per boundary. Boundaries shapes cannot be stored in a single array as we want masks to overlap
    if needed.

    Parameters
    ----------
    boundaries : gpd.GeoDataFrame
        Input GeoDataFrame containing the boundaries.
    height : int
        Height of the raster (number of pixels)
    width : int
        Width of the raster (number of pixels)
    transform : rasterio.Affine
        Raster affine transform

    Returns
    -------
    np.ndarray
        Binary masks as a numpy ndarray of shape (n_boundaries, height, width)
    """
    masks = np.ndarray(shape=(len(boundaries), height, width), dtype=np.bool_)
    for i, geom in enumerate(boundaries.geometry):
        mask = rasterize(
            shapes=[geom.__geo_interface__],
            out_shape=(height, width),
            fill=0,
            default_value=1,
            all_touched=True,
            transform=transform,
        )
        masks[i, :, :] = mask == 1
    return masks


def merge(data_dir: Path | str) -> xr.Dataset:
    """Merge all .grib files in a directory into a single xarray dataset.

    If multiple values are available for a given time, step, longitude & latitude dimensions, the
    maximum value is kept.

    Parameters
    ----------
    data_dir : Path | str
        Directory containing the .grib files.

    Returns
    -------
    xr.Dataset
        Merged xarray dataset with time, step, longitude and latitude dimensions.
    """
    if isinstance(data_dir, str):
        data_dir = Path(data_dir)

    files = data_dir.glob("*.grib")
    ds = xr.open_dataset(next(files), engine="cfgrib")

    for f in files:
        ds = xr.concat([ds, xr.open_dataset(f, engine="cfgrib")], dim="tmp_dim").max(dim="tmp_dim")

    return ds


def _np_to_datetime(dt64: np.datetime64) -> datetime:
    epoch = np.datetime64(0, "s")
    one_second = np.timedelta64(1, "s")
    seconds_since_epoch = (dt64 - epoch) / one_second
    return datetime.fromtimestamp(seconds_since_epoch)


def _has_missing_data(da: xr.DataArray) -> bool:
    """A DataArray is considered to have missing data if not all hours have measurements."""
    missing = False

    # if da.step.size == 1, da.step is just an int so we cannot iterate over it
    # if da.step size > 1, da.step is an array of int (one per step)
    if da.step.size > 1:
        for step in da.step:
            if da.sel(step=step).isnull().all():
                missing = True
    else:
        missing = da.isnull().all()

    return missing


def _week(date: datetime) -> str:
    year = date.isocalendar()[0]
    week = date.isocalendar()[1]
    return f"{year}W{week}"


def _epi_week(date: datetime) -> str:
    epiweek = Week.fromdate(date)
    year = epiweek.year
    week = epiweek.week
    return f"{year}W{week}"


def _month(date: datetime) -> str:
    return date.strftime("%Y%m")


def aggregate(ds: xr.Dataset, var: str, masks: np.ndarray, boundaries_id: list[str]) -> pl.DataFrame:
    """Aggregate hourly measurements in space and time.

    Parameters
    ----------
    ds : xr.Dataset
        Input xarray dataset with time, step, longitude and latitude dimensions
    var : str
        Variable to aggregate (ex: "t2m" or "tp")
    masks : np.ndarray
        Binary masks as a numpy ndarray of shape (n_boundaries, height, width)
    boundaries_id : list[str]
        List of boundary IDs (same order as n_boundaries dimension in masks)

    Notes
    -----
    The function aggregates hourly measurements to daily values for each boundary.

    Temporal aggregation is applied first. 3 statistics are computed for each day: daily mean,
    daily min, and daily max.

    Spatial aggregation is then applied. For each boundary, 3 statistics are computed: average of
    daily means, average of daily min, and average of daily max. These 3 statistics are stored in
    the "mean", "min", and "max" columns of the output dataframe.
    """
    rows = []

    for day in ds.time.values:
        da = ds[var].sel(time=day)

        if _has_missing_data(da):
            continue

        # if there is a step dimension (= hourly measurements), aggregate to daily
        # if not, data is already daily
        if "step" in da.dims:
            da_mean = da.mean(dim="step").values
            da_min = da.min(dim="step").values
            da_max = da.max(dim="step").values
        else:
            da_mean = da.values
            da_min = da.values
            da_max = da.values

        for i, uid in enumerate(boundaries_id):
            v_mean = np.nanmean(da_mean[masks[i, :, :]])
            v_min = np.nanmin(da_min[masks[i, :, :]])
            v_max = np.nanmax(da_max[masks[i, :, :]])

            rows.append(
                {
                    "boundary_id": uid,
                    "date": _np_to_datetime(day).date(),
                    "mean": v_mean,
                    "min": v_min,
                    "max": v_max,
                }
            )

    SCHEMA = {
        "boundary_id": pl.String,
        "date": pl.Date,
        "mean": pl.Float64,
        "min": pl.Float64,
        "max": pl.Float64,
    }

    df = pl.DataFrame(data=rows, schema=SCHEMA)

    # add week, month, and epi_week period columns
    df = df.with_columns(
        pl.col("date").map_elements(_week, str).alias("week"),
        pl.col("date").map_elements(_month, str).alias("month"),
        pl.col("date").map_elements(_epi_week, str).alias("epi_week"),
    )

    return df


def aggregate_per_week(
    daily: pl.DataFrame,
    column_uid: str,
    use_epidemiological_weeks: bool = False,
    sum_aggregation: bool = False,
) -> pl.DataFrame:
    """Aggregate daily data per week.

    Parameters
    ----------
    daily : pl.DataFrame
        Daily data with a "week" or "epi_week", "mean", "min", and "max" columns
        Length of the dataframe should be (n_boundaries * n_days).
    column_uid : str
        Column containing the boundary ID.
    use_epidemiological_weeks : bool, optional
        Use epidemiological weeks instead of iso weeks.
    sum_aggregation : bool, optional
        If True, sum values instead of computing the mean, for example for total precipitation data.

    Returns
    -------
    pl.DataFrame
        Weekly aggregated data of length (n_boundaries * n_weeks).
    """
    if use_epidemiological_weeks:
        week_column = "epi_week"
    else:
        week_column = "week"

    df = daily.select([column_uid, pl.col(week_column).alias("week"), "mean", "min", "max"])

    if sum_aggregation:
        df = df.group_by([column_uid, "week"]).agg(
            [
                pl.col("mean").sum().alias("mean"),
                pl.col("min").sum().alias("min"),
                pl.col("max").sum().alias("max"),
            ]
        )
    else:
        df = df.group_by([column_uid, "week"]).agg(
            [
                pl.col("mean").mean().alias("mean"),
                pl.col("min").min().alias("min"),
                pl.col("max").max().alias("max"),
            ]
        )

    # sort per date since dhis2 period format is "2012W9", we need to extract year and week number
    # from the period string and cast them to int before sorting, else "2012W9" will be superior to
    # "2012W32"
    df = df.sort(
        by=[
            pl.col("week").str.split("W").list.get(0).cast(int),
            pl.col("week").str.split("W").list.get(1).cast(int),
            pl.col(column_uid),
        ]
    )

    return df


def aggregate_per_month(daily: pl.DataFrame, column_uid: str, sum_aggregation: bool = False) -> pl.DataFrame:
    """Aggregate daily data per month.

    Parameters
    ----------
    daily : pl.DataFrame
        Daily data with a "month", "mean", "min", and "max" columns
        Length of the dataframe should be (n_boundaries * n_days).
    column_uid : str
        Column containing the boundary ID.
    sum_aggregation : bool, optional
        If True, sum values instead of computing the mean, for example for total precipitation data.

    Returns
    -------
    pl.DataFrame
        Monthly aggregated data of length (n_boundaries * n_months).
    """
    df = daily.select([column_uid, "month", "mean", "min", "max"])

    if sum_aggregation:
        df = df.group_by([column_uid, "month"]).agg(
            [
                pl.col("mean").sum().alias("mean"),
                pl.col("min").sum().alias("min"),
                pl.col("max").sum().alias("max"),
            ]
        )
    else:
        df = df.group_by([column_uid, "month"]).agg(
            [
                pl.col("mean").mean().alias("mean"),
                pl.col("min").min().alias("min"),
                pl.col("max").max().alias("max"),
            ]
        )

    df = df.sort(
        by=[
            pl.col("month").cast(int),
            pl.col(column_uid),
        ]
    )

    return df
