from __future__ import annotations

import importlib.resources
import json
import logging
import shutil
import tempfile
from calendar import monthrange
from datetime import datetime
from pathlib import Path

import cads_api_client
import cdsapi

with importlib.resources.open_text("openhexa.toolbox.era5", "variables.json") as f:
    VARIABLES = json.load(f)

DATASET = "reanalysis-era5-land"

logging.basicConfig(level=logging.DEBUG, format="%(name)s %(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

URL = "https://cds-beta.climate.copernicus.eu/api"


class ParameterError(ValueError):
    pass


class Client:
    def __init__(self, key: str):
        self.client = cdsapi.Client(url=URL, key=key, wait_until_complete=True, quiet=True, progress=False)
        self.cads_api_client = cads_api_client.ApiClient(key=key, url=URL)

    def latest(self) -> datetime:
        """Get date of latest available product."""
        collection = self.cads_api_client.collection(DATASET)
        _, end = collection.json["extent"]["temporal"]["interval"][0]
        end = datetime.strptime(end, "%Y-%m-%dT00:00:00Z")
        return end

    @staticmethod
    def build_request(
        variable: str,
        year: int,
        month: int,
        days: list[int] = None,
        time: list[str] = None,
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
        time : list[str]
            Hours of interest (ex: ["01:00", "06:00", "18:00"]). Defaults to None (all hours).
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
            time = [f"{hour:02}:00" for hour in range(0, 24)]

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

        with tempfile.NamedTemporaryFile() as tmp:
            self.client.retrieve(name=DATASET, request=request, target=tmp.name)
            shutil.copy(tmp.name, dst_file)

        log.debug("Downloaded Era5 product to %s", str(dst_file.absolute()))
