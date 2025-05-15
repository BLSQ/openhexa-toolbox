"""Client to download ERA5-Land data products from the climate data store.

See <https://cds.climate.copernicus.eu/datasets/reanalysis-era5-land?tab=overview>.
"""

from __future__ import annotations

import importlib.resources
import json
import logging
import tempfile
import zipfile
from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import cached_property
from math import ceil
from pathlib import Path
from time import sleep
from typing import Iterator

import geopandas as gpd
import xarray as xr
from datapi import ApiClient, Remote
from requests.exceptions import HTTPError

with importlib.resources.open_text("openhexa.toolbox.era5", "variables.json") as f:
    VARIABLES = json.load(f)

DATASET = "reanalysis-era5-land"

log = logging.getLogger(__name__)


@dataclass
class DataRequest:
    """CDS data request as expected by the API."""

    variable: list[str]
    year: str
    month: str
    day: list[str]
    time: list[str]
    data_format: str = "grib"
    area: list[float] | None = None


def bounds_from_file(fp: Path, buffer: float = 0.5) -> list[float]:
    """Get bounds from file.

    Parameters
    ----------
    fp : Path
        File path.
    buffer : float, optional
        Buffer to add to the bounds (default=0.5).

    Returns
    -------
    list[float]
        Bounds (north, west, south, east).
    """
    boundaries = gpd.read_parquet(fp)
    xmin, ymin, xmax, ymax = boundaries.total_bounds
    xmin = ceil(xmin - buffer)
    ymin = ceil(ymin - buffer)
    xmax = ceil(xmax + buffer)
    ymax = ceil(ymax + buffer)
    return ymax, xmin, ymin, xmax


def get_period_chunk(dtimes: list[datetime]) -> dict:
    """Get the period chunk for a list of datetimes.

    The period chunk is a dictionary with the "year", "month", "day" and "time" keys as expected by
    the CDS API. A period chunk cannot contain more than 1 year and 1 month. However, it can
    contain any number of days and times.

    This is the temporal part of a CDS data request.

    Parameters
    ----------
    dtimes : list[datetime]
        A list of datetimes for which we want data

    Returns
    -------
    dict
        The period chunk, in other words the temporal part of the request payload

    Raises
    ------
    ValueError
        If the list of datetimes contains more than 1 year or more than 1 month
    """
    years = {dtime.year for dtime in dtimes}
    if len(years) > 1:
        msg = "Cannot create a period chunk for multiple years"
        raise ValueError(msg)
    months = {dtime.month for dtime in dtimes}
    if len(months) > 1:
        msg = "Cannot create a period chunk for multiple months"
        raise ValueError(msg)

    year = next(iter(years))
    month = next(iter(months))
    days = []

    for dtime in sorted(dtimes):
        if dtime.day not in days:
            days.append(dtime.day)

    return {
        "year": str(year),
        "month": f"{month:02}",
        "day": [f"{day:02}" for day in days],
    }


def iter_chunks(dtimes: list[datetime]) -> Iterator[dict]:
    """Get the period chunks for a list of datetimes.

    The period chunks are a list of dictionaries with the "year", "month", "day" and "time" keys as
    expected by the CDS API. A period chunk cannot contain more than 1 year and 1 month. However,
    it can contain any number of days and times.

    The function tries its best to generate the minimum amount of chunks to minimize the amount of requests.

    Parameters
    ----------
    dtimes : list[datetime]
        A list of datetimes for which we want data

    Returns
    -------
    Iterator[dict]
        The period chunks (one per month max)
    """
    for year in range(min(dtimes).year, max(dtimes).year + 1):
        for month in range(12):
            dtimes_month = [dtime for dtime in dtimes if dtime.year == year and dtime.month == month + 1]
            if dtimes_month:
                yield get_period_chunk(dtimes_month)


def list_datetimes_in_dataset(ds: xr.Dataset) -> list[datetime]:
    """List datetimes in input dataset for which data is available.

    It is assumed that the dataset has a `time` dimension, in addition to `latitude` and `longitude`
    dimensions. We consider that a datetime is available in a dataset if non-null data values are
    present for more than 1 step.
    """
    dtimes = []
    data_vars = list(ds.data_vars)
    var = data_vars[0]

    for time in ds.time.values:
        dtime = datetime.fromtimestamp(time.astype(int) / 1e9, tz=timezone.utc)
        if dtime in dtimes:
            continue
        non_null = ds.sel(time=time)[var].notnull().sum().values.item()
        if non_null > 0:
            dtimes.append(dtime)

    return dtimes


def list_datetimes_in_dir(data_dir: Path) -> list[datetime]:
    """List datetimes in datasets that can be found in an input directory."""
    dtimes = []

    for f in data_dir.glob("*.grib"):
        # sometimes gribs are actually zip files
        with tempfile.NamedTemporaryFile(mode="wb") as tmp:
            if zipfile.is_zipfile(f):
                with zipfile.ZipFile(f, "r") as zip:
                    tmp.write(zip.read("data.grib"))
                ds = xr.open_dataset(tmp.name, engine="cfgrib")

            else:
                ds = xr.open_dataset(f, engine="cfgrib")

            dtimes += list_datetimes_in_dataset(ds)

    dtimes = sorted(set(dtimes))

    msg = f"Scanned {data_dir.as_posix()}, found data for {len(dtimes)} dates"
    log.info(msg)

    return dtimes


def date_range(start: datetime, end: datetime) -> list[datetime]:
    """Get a range of dates with a 1-day step."""
    drange = []
    dt = start
    while dt <= end:
        drange.append(dt)
        dt += timedelta(days=1)
    return drange


def build_request(
    variable: str,
    year: int,
    month: int,
    day: list[int] | list[str] | None = None,
    time: list[int] | list[str] | None = None,
    data_format: str = "grib",
    area: list[float] | None = None,
) -> DataRequest:
    """Build request payload.

    Parameters
    ----------
    variable : str
        Climate data store variable name (ex: "2m_temperature").
    year : int
        Year of interest.
    month : int
        Month of interest.
    day : list[int] | list[str] | None, optional
        Days of interest. Defauls to None (all days).
    time : list[int] | list[str] | None, optional
        Hours of interest (ex: [1, 6, 18]). Defaults to None (all hours).
    data_format : str, optional
        Output data format ("grib" or "netcdf"). Defaults to "grib".
    area : list[float] | None, optional
        Area of interest (north, west, south, east). Defaults to None (world).

    Returns
    -------
    DataRequest
        CDS data equest payload.

    Raises
    ------
    ValueError
        Request parameters are not valid.
    """
    if variable not in VARIABLES:
        msg = f"Variable {variable} not supported"
        raise ValueError(msg)

    if data_format not in ["grib", "netcdf"]:
        msg = f"Data format {data_format} not supported"
        raise ValueError(msg)

    # in the CDS data request, area is an array of float or int in the following order:
    # [north, west, south, east]
    if area:
        n, w, s, e = area
        msg = "Invalid area of interest"
        max_lat = 90
        max_lon = 180
        if ((abs(n) > max_lat) or (abs(s) > max_lat)) or ((abs(w) > max_lon) or (abs(e) > max_lon)):
            raise ValueError(msg)
        if (n < s) or (e < w):
            raise ValueError(msg)

    # in the CDS data request, days must be an array of strings (one string per day)
    # ex: ["01", "02", "03"]
    if not day:
        dmax = monthrange(year, month)[1]
        day = list(range(1, dmax + 1))

    if isinstance(day[0], int):
        day = [f"{d:02}" for d in day]

    # in the CDS data request, time must be an array of strings (one string per hour)
    # only hours between 00:00 and 23:00 are valid
    # ex: ["00:00", "03:00", "06:00"]
    if not time:
        time = range(24)

    if isinstance(time[0], int):
        time = [f"{hour:02}:00" for hour in time]

    return DataRequest(
        variable=[variable],
        year=str(year),
        month=f"{month:02}",
        day=day,
        time=time,
        data_format="grib",
        area=list(area) if area else None,
    )


class CDS:
    """Climate data store API client based on datapi."""

    def __init__(self, key: str, url: str = "https://cds.climate.copernicus.eu/api") -> None:
        """Initialize CDS client."""
        self.client = ApiClient(key=key, url=url)
        self.client.check_authentication()
        msg = f"Sucessfully authenticated to {url}"
        log.info(msg)

    @cached_property
    def latest(self) -> datetime:
        """Get date of latest available product."""
        collection = self.client.get_collection(DATASET)
        return collection.end_datetime

    def get_remote_requests(self) -> list[dict]:
        """Fetch list of the last 100 data requests in the CDS account."""
        requests = []
        jobs = self.client.get_jobs(limit=100)
        for request_id in jobs.request_uids:
            try:
                remote = self.client.get_remote(request_id)
                if remote.status in ["failed", "dismissed", "deleted"]:
                    continue
                requests.append({"request_id": request_id, "request": remote.request})
            except HTTPError:
                continue
        return requests

    def get_remote_from_request(self, request: DataRequest, existing_requests: list[dict]) -> Remote | None:
        """Look for a remote object that matches the provided request payload.

        Parameters
        ----------
        request : DataRequest
            Data request payload to look for.
        existing_requests : list[dict]
            List of existing data requests (as returned by self.get_remote_requests()).

        Returns
        -------
        Remote | None
            Remote object if found, None otherwise.
        """
        if not existing_requests:
            return None

        for remote_request in existing_requests:
            if remote_request["request"] == request.__dict__:
                return self.client.get_remote(remote_request["request_id"])

        return None

    def submit(self, request: DataRequest) -> Remote:
        """Submit an async data request to the CDS API.

        If an identical data request has already been submitted, the Remote object corresponding to
        the existing data request is returned instead of submitting a new one.
        """
        return self.client.submit(DATASET, request=request.__dict__)

    def retrieve(self, request: DataRequest, dst_file: Path | str) -> None:
        """Submit and download a data request to the CDS API."""
        dst_file = Path(dst_file)
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        self.client.retrieve(collection_id=DATASET, target=dst_file, request=request.__dict__)

    def download_between(
        self,
        start: datetime,
        end: datetime,
        variable: str,
        area: list[float],
        dst_dir: str | Path,
        time: list[int] | None = None,
    ) -> None:
        """Download all ERA5 data files needed to cover the period.

        Data requests are sent asynchronously (max one per month) to the CDS API and fetched when
        they are completed.

        Parameters
        ----------
        start : datetime
            Start date.
        end : datetime
            End date.
        variable : str
            Climate data store variable name (ex: "2m_temperature").
        area : list[float]
            Area of interest (north, west, south, east).
        dst_dir : str | Path
            Output directory.
        time : list[int] | None, optional
            Hours of interest (ex: [1, 6, 18]). Defaults to None (all hours).
        """
        dst_dir = Path(dst_dir)
        dst_dir.mkdir(parents=True, exist_ok=True)

        if not start.tzinfo:
            start = start.astimezone(tz=timezone.utc)
        if not end.tzinfo:
            end = end.astimezone(tz=timezone.utc)

        if end > self.latest:
            end = self.latest
            msg = "End date is after latest available product, setting end date to {}".format(end.strftime("%Y-%m-%d"))
            log.info(msg)

        # get the list of dates for which we will want to download data, which is the difference
        # between the available (already downloaded) and the requested dates
        drange = date_range(start, end)
        available = [dtime.date() for dtime in list_datetimes_in_dir(dst_dir)]
        dates = [d for d in drange if d.date() not in available]
        msg = f"Will request data for {len(dates)} dates"
        log.info(msg)

        existing_requests = self.get_remote_requests()
        remotes: list[Remote] = []

        for chunk in iter_chunks(dates):
            request = build_request(variable=variable, data_format="grib", area=area, time=time, **chunk)

            # has a similar request been submitted recently? if yes, use it instead of submitting
            # a new one
            remote = self.get_remote_from_request(request, existing_requests)
            if remote:
                remotes.append(remote)
                msg = f"Found existing request for date {request.year}-{request.month}"
                log.info(msg)
            else:
                remote = self.submit(request)
                remotes.append(remote)
                msg = f"Submitted new data request {remote.request_id} for {request.year}-{request.month}"

        while remotes:
            for remote in remotes:
                if remote.results_ready:
                    request = remote.request
                    fname = f"{request['year']}{request['month']}_{remote.request_id}.grib"
                    dst_file = Path(dst_dir, fname)
                    remote.download(dst_file.as_posix())
                    msg = f"Downloaded {dst_file.name}"
                    log.info(msg)
                    remotes.remove(remote)
                    remote.delete()

            if remotes:
                msg = f"Still {len(remotes)} files to download. Waiting 30s before retrying..."
                log.info(msg)
                sleep(30)
