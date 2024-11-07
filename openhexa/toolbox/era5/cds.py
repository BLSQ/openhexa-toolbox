from __future__ import annotations

import importlib.resources
import json
import logging
from calendar import monthrange
from datetime import date, datetime, timedelta
from functools import cached_property
from math import ceil
from pathlib import Path
from time import sleep
from typing import Optional, Union

import geopandas as gpd
import numpy as np
import xarray as xr
from cads_api_client import ApiClient, Remote, Results

with importlib.resources.open_text("openhexa.toolbox.era5", "variables.json") as f:
    VARIABLES = json.load(f)

DATASET = "reanalysis-era5-land"

logging.basicConfig(level=logging.DEBUG, format="%(name)s %(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

URL = "https://cds-beta.climate.copernicus.eu/api"


class ParameterError(ValueError):
    pass


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
    xmin = ceil(xmin - 0.5)
    ymin = ceil(ymin - 0.5)
    xmax = ceil(xmax + 0.5)
    ymax = ceil(ymax + 0.5)
    return ymax, xmin, ymin, xmax


def get_period_chunk(dtimes: list[datetime]) -> dict:
    """Get the period chunk for a list of datetimes.

    The period chunk is a dictionary with the "year", "month", "day" and "time" keys as expected by
    the CDS API. A period chunk cannot contain more than 1 year and 1 month. However, it can
    contain any number of days and times.

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
    years = list(set([dtime.year for dtime in dtimes]))
    if len(set(years)) > 1:
        raise ValueError("Cannot create a period chunk for multiple years")
    months = list(set([dtime.month for dtime in dtimes]))
    if len(months) > 1:
        raise ValueError("Cannot create a period chunk for multiple months")

    year = years[0]
    month = months[0]
    days = []

    for dtime in sorted(dtimes):
        if dtime.day not in days:
            days.append(dtime.day)

    return {
        "year": year,
        "month": month,
        "days": days,
        "time": [h for h in range(0, 24)],
    }


def get_period_chunks(dtimes: list[datetime]) -> list[dict]:
    """Get the period chunks for a list of datetimes.

    The period chunks are a list of dictionaries with the "year", "month", "day" and "time" keys as
    expected by the CDS API. A period chunk cannot contain more than 1 year and 1 month. However,
    it can contain any number of days and times.

    The function tries its best to generate the minimum amount of chunks to minize the amount of requests.

    Parameters
    ----------
    dtimes : list[datetime]
        A list of datetimes for which we want data

    Returns
    -------
    list[dict]
        The period chunks (one per month max)
    """
    chunks = []
    for year in range(min(dtimes).year, max(dtimes).year + 1):
        for month in range(1, 13):
            dtimes_month = [dtime for dtime in dtimes if dtime.year == year and dtime.month == month]
            if dtimes_month:
                chunk = get_period_chunk(dtimes_month)
                chunks.append(chunk)
    return chunks


def _np_to_datetime(dt64: np.datetime64) -> datetime:
    epoch = np.datetime64(0, "s")
    one_second = np.timedelta64(1, "s")
    seconds_since_epoch = (dt64 - epoch) / one_second
    return datetime.fromtimestamp(seconds_since_epoch)


def available_datetimes(data_dir: Path) -> list[date]:
    """Get available datetimes from a directory of ERA5 data files.

    Dates are considered as available if data for all 24 hours of the day are found in the file.

    Parameters
    ----------
    data_dir : Path
        Directory containing the ERA5 data files.

    Returns
    -------
    list[date]
        List of available dates.
    """
    dtimes = []

    for f in data_dir.glob("*.grib"):
        ds = xr.open_dataset(f, engine="cfgrib")
        var = [v for v in ds.data_vars][0]

        for time in ds.time:
            dtime = _np_to_datetime(time.values).date()
            if dtime in dtimes:
                continue

            is_complete = True
            for hour in range(1, 25):
                step = timedelta(hours=hour)
                if not ds.sel(time=time, step=step).get(var).notnull().any().values.item():
                    is_complete = False
                    break

            if is_complete:
                dtimes.append(dtime)

    log.debug(f"Scanned {data_dir.as_posix()}, found {len(dtimes)} available dates")

    return dtimes


def date_range(start: date, end: date) -> list[date]:
    """Get a range of dates with a 1-day step."""
    drange = []
    dt = start
    while dt <= end:
        drange.append(dt)
        dt += timedelta(days=1)
    return drange


class Client:
    def __init__(self, key: str):
        self.client = ApiClient(key=key, url=URL)
        self.client.check_authentication()

    @cached_property
    def latest(self) -> datetime:
        """Get date of latest available product."""
        collection = self.client.get_collection(DATASET)
        dt = collection.end_datetime
        # make datetime unaware of timezone for comparability with other datetimes
        dt = datetime(dt.year, dt.month, dt.day)
        return dt

    def get_jobs(self, **kwargs) -> Optional[list[dict]]:
        """Get list of current jobs for the account in the CDS."""
        r = self.client.get_jobs(limit=100, **kwargs)
        return r.json.get("jobs")

    def get_remote(self, request_id: str) -> Remote:
        """Get remote object from request uid."""
        return self.client.get_remote(request_id)

    def get_remote_from_request(self, request: dict, max_age: int = 1) -> Optional[Remote]:
        """Look for a remote object that matches the provided request payload.

        Parameters
        ----------
        request : dict
            Request payload.
        max_age : int, optional
            Maximum age of the remote object in days (default=1).

        Returns
        -------
        Optional[Remote]
            Remote object if found, None otherwise.
        """
        jobs = self.get_jobs()
        if not jobs:
            return None

        jobs = sorted(jobs, key=lambda job: job["status"], reverse=True)

        for job in jobs:
            remote = self.get_remote(job["jobID"])
            if remote.request == request:
                age = datetime.now() - remote.creation_datetime
                if age.days <= max_age:
                    return remote
        return None

    def submit(self, request: dict) -> str:
        """Submit an async data request to the CDS API."""
        r = self.client.submit(DATASET, **request)
        log.debug("Submitted data request %s", r.request_uid)
        return r.request_uid

    def submit_and_wait(self, request: dict) -> Results:
        """Submit a data request and wait for completion."""
        result = self.client.submit_and_wait_on_results(DATASET, **request)
        return result

    def download(self, request: dict, dst_file: Union[str, Path]):
        """Submit a data request and wait for completion before download."""
        if isinstance(dst_file, str):
            dst_file = Path(dst_file)
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        result = self.submit_and_wait(request)
        result.download(dst_file.as_posix())
        log.debug("Downloaded %s", dst_file.name)

    def download_between(self, start: date, end: date, variable: str, area: list[float], dst_dir: Union[str, Path]):
        """Download all ERA5 data files needed to cover the period.

        Data requests are sent asynchronously (max one per month) to the CDS API and fetched when
        they are completed.

        Parameters
        ----------
        start : date
            Start date.
        end : date
            End date.
        variable : str
            Climate data store variable name (ex: "2m_temperature").
        area : list[float]
            Area of interest (north, west, south, east).
        dst_dir : Path
            Output directory.
        """
        if isinstance(dst_dir, str):
            dst_dir = Path(dst_dir)
        dst_dir.mkdir(parents=True, exist_ok=True)

        if end > self.latest.date():
            end = self.latest.date()
            log.debug(f"End date is after latest available product, setting end date to {end.strftime('%Y-%m-%d')}")

        drange = date_range(start, end)
        available = available_datetimes(dst_dir)
        dates = [d for d in drange if d not in available]

        chunks = get_period_chunks(dates)
        requests = []
        remotes = []

        for chunk in chunks:
            request = self.build_request(variable=variable, data_format="grib", area=area, **chunk)

            # has a similar request been submitted recently? if yes, use it
            remote = self.get_remote_from_request(request)
            if remote:
                remotes.append(remote)
                log.debug(f"Found existing request for date {request['year']}-{request['month']}")
            else:
                requests.append(self.submit(request))
            sleep(3)

        for request in requests:
            remotes.append(self.get_remote(request))
        done = []

        while not all([remote.request_uid in done for remote in remotes]):
            for remote in remotes:
                if remote.results_ready:
                    fname = f"{request['year']}{request['month']}_{remote.request_uid}.grib"
                    dst_file = Path(dst_dir, fname)
                    remote.download(dst_file.as_posix())
                    log.debug(f"Downloaded {dst_file.name}")
                    done.append(remote.request_uid)
                    remote.delete()
            sleep(60)

    @staticmethod
    def build_request(
        variable: str,
        year: int,
        month: int,
        days: list[int] = None,
        time: list[int] = None,
        data_format: str = "grib",
        area: list[float] = None,
    ) -> dict:
        """Build request payload.

        Parameters
        ----------
        variable : str
            Climate data store variable name (ex: "2m_temperature").
        year : int
            Year of interest.
        month : int
            Month of interest.
        days : list[int]
            Days of interest. Defauls to None (all days).
        time : list[int]
            Hours of interest (ex: [1, 6, 18]). Defaults to None (all hours).
        data_format : str
            Output data format ("grib" or "netcdf"). Defaults to "grib".
        area : list[float]
            Area of interest (north, west, south, east). Defaults to None (world).

        Returns
        -------
        dict
            Request payload.

        Raises
        ------
        ParameterError
            Request parameters are not valid.
        """
        if variable not in VARIABLES:
            raise ParameterError("Variable %s not supported", variable)

        if data_format not in ["grib", "netcdf"]:
            raise ParameterError("Data format %s not supported", data_format)

        if area:
            n, w, s, e = area
            if ((abs(n) > 90) or (abs(s) > 90)) or ((abs(w) > 180) or (abs(e) > 180)):
                raise ParameterError("Invalid area of interest")
            if (n < s) or (e < w):
                raise ParameterError("Invalid area of interest")

        if not days:
            dmax = monthrange(year, month)[1]
            days = [day for day in range(1, dmax + 1)]

        if not time:
            time = [hour for hour in range(0, 24)]
        time = [f"{hour:02}:00" for hour in time]

        year = str(year)
        month = f"{month:02}"
        days = [f"{day:02}" for day in days]

        payload = {
            "variable": [variable],
            "year": year,
            "month": month,
            "day": days,
            "time": time,
            "data_format": data_format,
        }

        if area:
            payload["area"] = area

        return payload
