from dataclasses import dataclass
from typing import Literal, TypedDict


class Variable(TypedDict):
    """Metadata for a single variable in the ERA5-Land dataset."""

    name: str
    short_name: str
    unit: str
    time: list[str]
    accumulated: bool


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


class RequestTemporal(TypedDict):
    """Temporal request parameters."""

    year: str
    month: str
    day: list[str]


class JobLink(TypedDict):
    """A link related to a data request job."""

    href: str
    rel: str
    type: str | None
    title: str | None


class JobMetadataResults(TypedDict):
    """Metadata about the results of a data request job."""

    type: str
    title: str
    status: int
    detail: str
    trace_id: str


class JobMetadata(TypedDict):
    """Metadata about a data request job."""

    results: JobMetadataResults
    datasetMetadata: dict[str, str]
    qos: dict[str, dict]
    origin: str


@dataclass
class Job:
    """A data request job in the CDS."""

    processID: str
    type: str
    jobID: str
    status: str
    created: str
    started: str
    finished: str
    updated: str
    links: list[JobLink]
    metadata: JobMetadata

    @property
    def expired(self) -> bool:
        """Whether the job results have expired.

        Means that a data request has been successfully processed by the server,
        but the results expired and cannot be downloaded anymore. This doesn't change
        the status, we have to dig into job metadata for this info.
        """
        if "results" in self.metadata:
            if "type" in self.metadata["results"]:
                if self.metadata["results"]["type"] == "results expired":
                    return True
        return False
