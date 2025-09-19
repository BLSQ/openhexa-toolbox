"""Download ERA5-Land hourly data from the ECMWF Climate Data Store (CDS).

Provides functions to build requests, submit them to the CDS API, retrieve results, and
move GRIB data to an analysis-ready Zarr store for further processing.
"""

import importlib.resources
import logging
import shutil
import tempfile
import tomllib
from collections import defaultdict
from datetime import date
from pathlib import Path
from time import sleep
from typing import Literal, TypedDict

import numpy as np
import numpy.typing as npt
import xarray as xr
import zarr
from dateutil.relativedelta import relativedelta
from ecmwf.datastores import Remote
from ecmwf.datastores.client import Client

logger = logging.getLogger(__name__)


class Variable(TypedDict):
    """Metadata for a single variable in the ERA5-Land dataset."""

    name: str
    short_name: str
    unit: str
    time: list[str]


def _get_variables() -> dict[str, Variable]:
    """Load ERA5-Land variables metadata.

    Returns:
        A dictionary mapping variable names to their metadata.

    """
    with importlib.resources.path("era5", "data", "variables.toml") as path, path.open("rb") as f:
        return tomllib.load(f)


class Request(TypedDict):
    """Request parameters for the 'reanalysis-era5-land' dataset."""

    variable: list[str]
    year: str
    month: str
    day: list[str]
    time: list[str]
    data_format: Literal["grib", "netcdf"]
    download_format: Literal["unarchived", "zip"]
    area: list[int]


def get_date_range(
    start_date: date,
    end_date: date,
) -> list[date]:
    """Get inclusive date range from start and end dates.

    Returns:
        A list of dates from start to end, inclusive.

    """
    if start_date > end_date:
        msg = "Start date must be before end date"
        logger.error(msg)
        raise ValueError(msg)

    date_range: list[date] = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date)
        current_date += relativedelta(days=1)
    return date_range


def bound_date_range(
    start_date: date,
    end_date: date,
    collection_start_date: date,
    collection_end_date: date,
) -> tuple[date, date]:
    """Bound input date range to the collection's start and end dates.

    Args:
        start_date: Requested start date.
        end_date: Requested end date.
        collection_start_date: Earliest date in the collection.
        collection_end_date: Latest date in the collection.

    Returns:
        A new date range tuple (start, end) within the collection's date limits.

    """
    start = max(start_date, collection_start_date)
    end = min(end_date, collection_end_date)
    return start, end


class RequestTemporal(TypedDict):
    """Temporal request parameters."""

    year: str
    month: str
    day: list[str]


def get_temporal_chunks(dates: list[date]) -> list[RequestTemporal]:
    """Get monthly temporal request chunks for the given list of dates.

    Args:
        dates: A list of dates to chunk.

    Returns:
        A list of RequestTemporal objects, one per month.

    """
    by_month: dict[tuple[int, int], list[int]] = defaultdict(list)
    for d in dates:
        by_month[(d.year, d.month)].append(d.day)

    chunks: list[RequestTemporal] = []
    for (year, month), days in by_month.items():
        chunks.append(
            RequestTemporal(
                year=f"{year:04d}",
                month=f"{month:02d}",
                day=[f"{day:02d}" for day in sorted(set(days))],
            ),
        )
    return chunks


def submit_requests(
    client: Client,
    collection_id: str,
    requests: list[Request],
) -> list[Remote]:
    """Submit a list of requests to the CDS API.

    Args:
        client: CDS API client.
        collection_id: ID of the CDS dataset (e.g. "reanalysis-era5-land").
        requests: List of request parameters.

    Returns:
        List of Remote objects representing the submitted requests.

    """
    remotes: list[Remote] = []
    for request in requests:
        r = client.submit(
            collection_id=collection_id,
            request=dict(request),
        )
        logger.info("Submitted request %s", r.request_id)
        remotes.append(r)
    return remotes


def build_requests(
    dates: list[date],
    variable: str,
    time: list[str],
    area: list[int],
    data_format: Literal["grib", "netcdf"] = "grib",
    download_format: Literal["unarchived", "zip"] = "unarchived",
) -> list[Request]:
    """Build requests for the reanalysis-era5-land dataset.

    Args:
        dates: Requested dates.
        variable: Requested variable (ex: "2m_temperature").
        time: List of times to request (ex: ["00:00", "01:00", ..., "23:00"]).
        area: Geographical area to request (north, west, south, east).
        data_format: Data format to request ("grib" or "netcdf").
        download_format: Download format ("unarchived" or "zip").

    Returns:
        A list of Request objects to be submitted to the CDS API.

    """
    requests: list[Request] = []
    temporal_chunks = get_temporal_chunks(dates)
    for chunk in temporal_chunks:
        request = Request(
            variable=[variable],
            year=chunk["year"],
            month=chunk["month"],
            day=chunk["day"],
            time=time,
            data_format=data_format,
            download_format=download_format,
            area=area,
        )
        requests.append(request)
    return requests


def _get_name(remote: Remote) -> str:
    """Create file name from remote request.

    Returns:
        File name with format: {year}{month}_{request_id}.{ext}

    """
    request = remote.request
    data_format = request["data_format"]
    download_format = request["download_format"]
    year = request["year"]
    month = request["month"]
    ext = "zip" if download_format == "zip" else data_format
    return f"{year}{month}_{remote.request_id}.{ext}"


def retrieve_remotes(
    queue: list[Remote],
    output_dir: Path,
) -> list[Remote]:
    """Retrieve the results of the submitted remotes.

    Args:
        queue: List of Remote objects to check and download if ready.
        output_dir: Directory to save downloaded files.

    Returns:
        List of Remote objects that are still pending (not ready).

    """
    output_dir.mkdir(parents=True, exist_ok=True)
    pending: list[Remote] = []

    for remote in queue:
        if remote.results_ready:
            name = _get_name(remote)
            fp = output_dir / name
            remote.download(target=fp.as_posix())
            logger.info("Downloaded %s", name)
        else:
            pending.append(remote)
    return pending


def _times_in_zarr(store: Path) -> npt.NDArray[np.datetime64]:
    """List time dimensions in the zarr store.

    Args:
        store: Path to the zarr store.

    Returns:
        Numpy array of datetime64 values in the time dimension of the entire zarr store.

    """
    ds = xr.open_zarr(store, consolidated=True, decode_timedelta=False)
    return ds.time.values


def create_zarr(ds: xr.Dataset, zarr_store: Path) -> None:
    """Create a new zarr store from the dataset.

    Args:
        ds: The xarray Dataset to store.
        zarr_store: Path to the zarr store to create.

    """
    ds.to_zarr(zarr_store, mode="w", consolidated=True, zarr_format=2)
    logger.debug("Created Zarr store at %s", zarr_store)


def append_zarr(ds: xr.Dataset, zarr_store: Path) -> None:
    """Append new data to an existing zarr store.

    The function checks for overlapping time values and only appends new data.

    Args:
        ds: The xarray Dataset to append.
        zarr_store: Path to the existing zarr store.

    """
    existing_times = _times_in_zarr(zarr_store)
    new_times = ds.time.values
    overlap = np.isin(new_times, existing_times)
    if overlap.any():
        logger.warning("Time dimension of GRIB file overlaps with existing Zarr store")
        ds = ds.sel(time=~overlap)
        if len(ds.time) == 0:
            logger.debug("No new data to add to Zarr store")
            return
    ds.to_zarr(zarr_store, mode="a", append_dim="time", zarr_format=2)
    logger.debug("Appended %s values to Zarr store", len(ds.time))


def consolidate_zarr(zarr_store: Path) -> None:
    """Consolidate metadata and ensure dimensions are properly sorted.

    The function consolidates the metadata of the zarr store and checks if the time
    dimension is sorted. If not, it sorts the time dimension and rewrites the zarr
    store.

    Args:
        zarr_store: Path to the zarr store to consolidate.

    """
    zarr.consolidate_metadata(zarr_store)
    ds = xr.open_zarr(zarr_store, consolidated=True, decode_timedelta=False)
    ds_sorted = ds.sortby("time")
    if not np.array_equal(ds.time.values, ds_sorted.time.values):
        logger.debug("Sorting time dimension in Zarr store")
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_zarr_store = Path(tmp_dir) / zarr_store.name
            ds_sorted.to_zarr(tmp_zarr_store, mode="w", consolidated=True, zarr_format=2)
            shutil.rmtree(zarr_store)
            shutil.move(tmp_zarr_store, zarr_store)
    else:
        zarr.consolidate_metadata(zarr_store, zarr_format=2)


def grib_to_zarr(
    src_dir: Path,
    zarr_store: Path,
) -> None:
    """Move data in multiple GRIB files to a zarr store.

    The function processes all GRIB files in the source directory and moves the data
    to the specified Zarr store (creating or appending as necessary).

    Args:
        src_dir: Directory containing the GRIB files.
        zarr_store: Path to the zarr store to create or update.

    """
    for fp in src_dir.glob("*.grib"):
        ds = xr.open_dataset(fp, engine="cfgrib", decode_timedelta=False)
        ds = ds.assign_coords(
            {
                "latitude": np.round(ds.latitude.values, 1),
                "longitude": np.round(ds.longitude.values, 1),
            },
        )
        if not zarr_store.exists():
            create_zarr(ds, zarr_store)
        else:
            append_zarr(ds, zarr_store)
    consolidate_zarr(zarr_store)


def diff_zarr(
    start_date: date,
    end_date: date,
    zarr_store: Path,
) -> list[date]:
    """Get dates between start and end dates that are not in the zarr store.

    Args:
        start_date: Start date for data retrieval.
        end_date: End date for data retrieval.
        zarr_store: The Zarr store to check for existing data.

    Returns:
        The list of dates that are not in the Zarr store.

    """
    if not zarr_store.exists():
        return get_date_range(start_date, end_date)

    zarr_dtimes = _times_in_zarr(zarr_store)
    zarr_dates = zarr_dtimes.astype("datetime64[D]").astype(date).tolist()

    date_range = get_date_range(start_date, end_date)
    return [d for d in date_range if d not in zarr_dates]


def get_missing_dates(
    client: Client,
    dataset_id: str,
    start_date: date,
    end_date: date,
    zarr_store: Path,
) -> list[date]:
    """Get the list of dates between start_date and end_date that are not in the Zarr store.

    Args:
        client: The CDS API client.
        dataset_id: The ID of the dataset to check.
        start_date: Start date for data retrieval.
        end_date: End date for data retrieval.
        zarr_store: The Zarr store to check for existing data.

    Returns:
        A list of dates that are not in the Zarr store.

    """
    collection = client.get_collection(dataset_id)
    if not collection.begin_datetime or not collection.end_datetime:
        msg = f"Dataset {dataset_id} does not have a defined date range"
        raise ValueError(msg)
    start_date, end_date = bound_date_range(
        start_date,
        end_date,
        collection.begin_datetime.date(),
        collection.end_datetime.date(),
    )
    dates = diff_zarr(start_date, end_date, zarr_store)
    logger.debug("Missing dates: %s", dates)
    return dates


def prepare_requests(
    client: Client,
    dataset_id: str,
    start_date: date,
    end_date: date,
    variable: str,
    area: list[int],
    zarr_store: Path,
) -> list[Request]:
    """Prepare requests for data retrieval from the CDS API.

    This function checks the available dates in the Zarr store and prepares
    requests for the missing dates.

    Args:
        client: The CDS API client.
        dataset_id: ID of the CDS dataset (e.g. "reanalysis-era5-land").
        start_date: Start date for data synchronization.
        end_date: End date for data synchronization.
        variable: The variable to synchronize (e.g. "2m_temperature").
        area: The geographical area to synchronize (north, west, south, east).
        zarr_store: The Zarr store to update or create.

    Returns:
        A list of requests to be submitted to the CDS API.

    """
    variables = _get_variables()
    if variable not in variables:
        msg = f"Variable '{variable}' not supported"
        raise ValueError(msg)

    dates = get_missing_dates(
        client=client,
        dataset_id=dataset_id,
        start_date=start_date,
        end_date=end_date,
        zarr_store=zarr_store,
    )

    requests = build_requests(
        dates=dates,
        variable=variable,
        time=variables[variable]["time"],
        area=area,
        data_format="grib",
        download_format="unarchived",
    )

    max_requests = 100
    if len(requests) > max_requests:
        msg = f"Too many data requests ({len(requests)}), max is {max_requests}"
        logger.error(msg)
        raise ValueError(msg)

    return requests


def retrieve_requests(
    client: Client,
    dataset_id: str,
    requests: list[Request],
    src_dir: Path,
    wait: int = 30,
) -> None:
    """Retrieve the results of the submitted requests.

    Args:
        client: The CDS API client.
        dataset_id: The ID of the dataset to retrieve.
        requests: The list of requests to retrieve.
        src_dir: The directory containing the source data files.
        wait: Time in seconds to wait between checking for completed requests.

    """
    logger.debug("Submitting %s requests", len(requests))
    remotes = submit_requests(
        client=client,
        collection_id=dataset_id,
        requests=requests,
    )
    while remotes:
        remotes = retrieve_remotes(remotes, src_dir)
        sleep(wait)
