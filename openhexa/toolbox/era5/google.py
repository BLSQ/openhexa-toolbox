"""Download raw historical Era5 products from Google Cloud:
https://console.cloud.google.com/storage/browser/gcp-public-data-arco-era5

Products are provided as raw NetCDF files and are usually available with a ~3 month lag.
"""

from __future__ import annotations

import importlib.resources
import json
import logging
import shutil
import tempfile
from datetime import datetime, timedelta
from functools import cached_property
from pathlib import Path

import requests
from google.cloud import storage

with importlib.resources.open_text("openhexa.toolbox.era5", "variables.json") as f:
    VARIABLES = json.load(f)

log = logging.getLogger(__name__)


class NotFoundError(Exception):
    pass


class ParameterError(ValueError):
    pass


class Client:
    def __init__(self):
        self.client = storage.Client.create_anonymous_client()
        self.bucket = self.client.bucket("gcp-public-data-arco-era5")

    @staticmethod
    def prefix(variable: str, date: datetime) -> str:
        """Build key prefix for a given product."""
        return f"raw/date-variable-single_level/{date.year}/{date.month:02}/{date.day:02}/{variable}/surface.nc"

    def _subdirs(self, prefix: str) -> list[str]:
        """List subdirs."""
        blobs = self.client.list_blobs(self.bucket, prefix=prefix, delimiter="/")
        prefixes = []
        for page in blobs.pages:
            prefixes += page.prefixes
        return prefixes

    @cached_property
    def latest(self) -> datetime:
        """Get date of latest available product."""
        root = "raw/date-variable-single_level/"
        subdirs = self._subdirs(root)  # years
        subdirs = self._subdirs(max(subdirs))  # months
        subdirs = self._subdirs(max(subdirs))  # days
        subdir = max(subdirs).split("/")
        year = int(subdir[-4])
        month = int(subdir[-3])
        day = int(subdir[-2])
        return datetime(year, month, day)

    def find(self, variable: str, date: datetime) -> str | None:
        """Find public URL of product. Return None if not found."""
        prefix = self.prefix(variable, date)
        blobs = self.client.list_blobs(self.bucket, prefix=prefix, max_results=1)
        blobs = list(blobs)
        if blobs:
            return blobs[0].public_url
        else:
            return None

    def download(self, variable: str, date: datetime, dst_file: str | Path, overwrite=False):
        """Download an Era5 NetCDF product for a given day.

        Parameters
        ----------
        variable : str
            Climate data store variable name (ex: "2m_temperature").
        date : datetime
            Product date (year, month, day).
        dst_file : str | Path
            Output file.
        overwrite : bool, optional
            Overwrite existing file (default=False).

        Raises
        ------
        ParameterError
            Product request parameters are invalid.
        NotFoundError
            Product not found in bucket.
        """
        dst_file = Path(dst_file)
        dst_file.parent.mkdir(parents=True, exist_ok=True)

        if dst_file.exists() and not overwrite:
            log.debug("Skipping download of %s because file already exists", str(dst_file.absolute()))
            return

        if variable not in VARIABLES:
            raise ParameterError("%s is not a valid climate data store variable name", variable)

        url = self.find(variable, date)
        if not url:
            raise NotFoundError("%s product not found for date %s", variable, date.strftime("%Y-%m-%d"))

        with tempfile.NamedTemporaryFile() as tmp:
            with open(tmp.name, "wb") as f:
                with requests.get(url, stream=True) as r:
                    for chunk in r.iter_content(chunk_size=1024**2):
                        if chunk:
                            f.write(chunk)

            shutil.copy(tmp.name, dst_file)

        log.debug("Downloaded %s", str(dst_file.absolute()))

    def sync(self, variable: str, start_date: datetime, end_date: datetime, dst_dir: str | Path):
        """Download all products for a given variable and date range.

        If products are already present in the destination directory, they will be skipped.
        Expects file names to be formatted as "YYYY-MM-DD_VARIABLE.nc".

        Parameters
        ----------
        variable : str
            Climate data store variable name (ex: "2m_temperature").
        start_date : datetime
            Start date (year, month, day).
        end_date : datetime
            End date (year, month, day).
        dst_dir : str | Path
            Output directory.
        """
        dst_dir = Path(dst_dir)
        dst_dir.mkdir(parents=True, exist_ok=True)

        if start_date > end_date:
            raise ParameterError("`start_date` must be before `end_date`")

        date = start_date
        if end_date > self.latest:
            log.info("Setting `end_date` to the latest available date: %s" % date.strftime("%Y-%m-%d"))
            end_date = self.latest

        while date <= end_date:
            expected_filename = f"{date.strftime('%Y-%m-%d')}_{variable}.nc"
            fpath = Path(dst_dir, expected_filename)
            fpath_grib = Path(dst_dir, expected_filename.replace(".nc", ".grib"))
            if fpath.exists() or fpath_grib.exists():
                log.debug("%s already exists, skipping download" % expected_filename)
            else:
                self.download(variable=variable, date=date, dst_file=fpath, overwrite=False)
            date += timedelta(days=1)
