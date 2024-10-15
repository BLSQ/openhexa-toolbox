"""Module for spatial and temporal aggregation of ERA5 data."""

from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import polars as pl
import rasterio
import rasterio.transform
import xarray as xr


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


def get_transform(ds: xr.Dataset) -> rasterio.transform.Affine:
    """Get rasterio affine transform from xarray dataset.

    Parameters
    ----------
    ds : xr.Dataset
        Input xarray dataset.

    Returns
    -------
    rasterio.transform.Affine
        Rasterio affine transform.
    """
    transform = rasterio.transform.from_bounds(
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
        mask = rasterio.features.rasterize(
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

    datasets = []
    for fp in data_dir.glob("*.grib"):
        datasets.append(xr.open_dataset(fp, engine="cfgrib"))

    ds = xr.concat(datasets, dim="tmp_dim").max(dim="tmp_dim")
    return ds


def _np_to_datetime(dt64: np.datetime64) -> datetime:
    epoch = np.datetime64(0, "s")
    one_second = np.timedelta64(1, "s")
    seconds_since_epoch = (dt64 - epoch) / one_second
    return datetime.fromtimestamp(seconds_since_epoch)


def _has_missing_data(da: xr.DataArray) -> bool:
    """A DataArray is considered to have missing data if not all hours have measurements."""
    missing = False
    for step in da.step:
        if da.sel(step=step).isnull().all():
            missing = True
    return missing


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

        da_mean = da.mean(dim="step").values
        da_min = da.min(dim="step").values
        da_max = da.max(dim="step").values

        for i, uid in enumerate(boundaries_id):
            v_mean = da_mean[masks[i, :, :]].mean()
            v_min = da_min[masks[i, :, :]].mean()
            v_max = da_max[masks[i, :, :]].mean()

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

    return df
