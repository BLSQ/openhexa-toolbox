from __future__ import annotations

import importlib.resources
import json
import logging
import shutil
import tempfile
from calendar import monthrange
from datetime import datetime, timedelta
from functools import cached_property
from math import ceil
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
from cads_api_client import ApiClient, Remote, Results
from dateutil.relativedelta import relativedelta

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


class Client:
    def __init__(self, key: str):
        self.client = ApiClient(key=key, url=URL)
        self.check_authentication()

    @cached_property
    def latest(self) -> datetime:
        """Get date of latest available product."""
        collection = self.client.get_collection(DATASET)
        dt = collection.end_datetime
        # make datetime unaware of timezone for comparability with other datetimes
        dt = datetime(dt.year, dt.month, dt.day)
        return dt

    def get_jobs(self) -> list[dict]:
        """Get list of current jobs for the account in the CDS."""
        r = self.client.get_jobs()
        return "jobs" in r.json.get("jobs")

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

    @staticmethod
    def _filename(variable: str, year: int, month: int, day: int = None, data_format: str = "grib") -> str:
        """Get filename from variable name and date."""
        EXTENSION = {"grib": "grib", "netcdf": "nc"}
        if day is not None:
            return f"{variable}_{year}-{month:02}-{day:02}.{EXTENSION[data_format]}"
        else:
            return f"{variable}_{year}-{month:02}.{EXTENSION[data_format]}"

    def download(self, request: dict, dst_file: str | Path, overwrite: bool = False):
        """Download Era5 product.

        Parameters
        ----------
        request : dict
            Request payload as returned by the build_request() method.
        dst_file : Path
            Output file path.
        overwrite : bool, optional
            Overwrite existing file (default=False).
        """
        dst_file = Path(dst_file)
        dst_file.parent.mkdir(parents=True, exist_ok=True)

        if dst_file.exists() and not overwrite:
            log.debug("File %s already exists, skipping download", str(dst_file.absolute()))
            return

        # if we request daily data while a monthly file is already present, also skip download
        if len(request["day"]) == 1:
            dst_file_monthly = Path(
                dst_file.parent, self._filename(request["variable"], request["year"], request["month"])
            )
            if dst_file_monthly.exists() and not overwrite:
                log.debug("Monthly file `{}` already exists, skipping download".format(dst_file_monthly.name))

        with tempfile.NamedTemporaryFile() as tmp:
            self.client.retrieve(name=DATASET, request=request, target=tmp.name)
            shutil.copy(tmp.name, dst_file)

        log.debug("Downloaded Era5 product to %s", str(dst_file.absolute()))

    @staticmethod
    def _period_chunks(start: datetime, end: datetime) -> list[dict]:
        """Generate list of period chunks to prepare CDS API requests.

        If we can, prepare requests for full months to optimize wait times. If we can't, prepare
        daily requests.

        Parameters
        ----------
        start : datetime
            Start date.
        end : datetime
            End date.

        Returns
        -------
        list[dict]
            List of period chunks as dicts with `year`, `month` and `days` keys.
        """
        chunks = []
        date = start
        while date <= end:
            last_day_in_month = datetime(date.year, date.month, monthrange(date.year, date.month)[1])
            if last_day_in_month <= end:
                chunks.append(
                    {"year": date.year, "month": date.month, "days": [day for day in range(1, last_day_in_month.day)]}
                )
                date += relativedelta(months=1)
            else:
                chunks.append({"year": date.year, "month": date.month, "days": [date.day]})
                date += timedelta(days=1)
        return chunks

    def download_between(
        self,
        variable: str,
        start: datetime,
        end: datetime,
        dst_dir: str | Path,
        area: list[float] = None,
        overwrite: bool = False,
    ):
        """Download all ERA5 products between two dates.

        Parameters
        ----------
        variable : str
            Climate data store variable name (ex: "2m_temperature").
        start : datetime
            Start date.
        end : datetime
            End date.
        dst_dir : Path
            Output directory.
        area : list[float], optional
            Area of interest (north, west, south, east). Defaults to None (world).
        overwrite : bool, optional
            Overwrite existing files (default=False).
        """
        if end > self.latest:
            end = self.latest
            log.debug("End date is after latest available product, setting end date to %s", end)

        chunks = self._period_chunks(start, end)

        for chunk in chunks:
            request = self.build_request(
                variable=variable, year=chunk["year"], month=chunk["month"], days=chunk["days"], area=area
            )

            if len(chunk["days"]) == 1:
                dst_file = Path(dst_dir, self._filename(variable, chunk["year"], chunk["month"], chunk["days"][0]))
            else:
                dst_file = Path(dst_dir, self._filename(variable, chunk["year"], chunk["month"]))

            self.download(request=request, dst_file=dst_file, overwrite=overwrite)
