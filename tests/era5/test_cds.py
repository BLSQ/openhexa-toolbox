"""Unit tests for the ERA5 Climate Data Store client."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import Mock, patch

import datapi
import pytest

from openhexa.toolbox.era5.cds import (
    CDS,
    DataRequest,
)


class TestCollection(datapi.catalogue.Collection):
    """Datapi Collection object with mocked end_datetime property."""

    __test__ = False

    def __init__(self, end_datetime: datetime) -> None:
        self._end_datetime = end_datetime

    @property
    def end_datetime(self):
        return self._end_datetime


@patch("datapi.ApiClient.check_authentication")
def test_cds_init(mock_check_authentication: Mock):
    """Test CDS class initialization."""
    mock_check_authentication.return_value = True
    CDS(key="xxx")


@pytest.fixture
@patch("datapi.ApiClient.check_authentication")
def fake_cds(mock_check_authentication: Mock):
    mock_check_authentication.return_value = True
    return CDS(key="xxx")


@patch("datapi.ApiClient.get_collection")
def test_latest(mock_get_collection: Mock, fake_cds: CDS):
    mock_get_collection.return_value = TestCollection(end_datetime=datetime(2023, 1, 1).astimezone())
    assert fake_cds.latest == datetime(2023, 1, 1).astimezone()


class TestJobs(datapi.processing.Jobs):
    """Datapi Jobs class with mocked request_uids property."""

    __test__ = False

    def __init__(self, request_uids: list[str]) -> None:
        self._request_uids = request_uids

    @property
    def request_uids(self):
        return self._request_uids


class TestRemote(datapi.processing.Remote):
    """Datapi Remote class with mocked properties."""

    __test__ = False

    def __init__(self, request_uid: str, status: str, results_ready: bool, request: dict) -> None:
        self._request_uid = request_uid
        self._status = status
        self._results_ready = results_ready
        self._request = request
        self.cleanup = False

    @property
    def status(self):
        return self._status

    @property
    def request_uid(self):
        return self._request_uid

    @property
    def results_ready(self):
        return self._results_ready

    @property
    def request(self):
        return self._request


@patch("datapi.ApiClient.get_jobs")
@patch("datapi.ApiClient.get_remote")
def test_cds_get_remote_requests(mock_get_remote: Mock, mock_get_jobs: Mock, fake_cds: CDS):
    mock_get_jobs.return_value = TestJobs(
        request_uids=[
            "73dc0d2d-8288-4041-a84d-87e70772d5a8",
            "3973ec55-4b38-449b-b7f1-5edd1034f663",
            "a5c7093d-56d9-40a4-a363-c60cd242ce66",
        ]
    )

    mock_get_remote.return_value = TestRemote(
        request_uid="73dc0d2d-8288-4041-a84d-87e70772d5a8", status="successful", results_ready=True, request={}
    )

    remote_requests = fake_cds.get_remote_requests()

    assert len(remote_requests) == 3
    assert remote_requests[0]["request_id"] == "73dc0d2d-8288-4041-a84d-87e70772d5a8"
    assert isinstance(remote_requests[0]["request"], dict)


@pytest.fixture
def tp_request() -> DataRequest:
    return DataRequest(
        variable=["total_precipitation"],
        year="2024",
        month="12",
        day=["01", "02", "03", "04", "05"],
        time=["01:00", "06:00", "18:00"],
        data_format="grib",
        area=[16, -6, 9, 3],
    )


@pytest.fixture
def tp_request_remote() -> dict:
    return {
        "request_id": "73dc0d2d-8288-4041-a84d-87e70772d5a8",
        "request": {
            "day": ["01", "02", "03", "04", "05"],
            "area": [16, -6, 9, 3],
            "time": ["01:00", "06:00", "18:00"],
            "year": "2024",
            "month": "12",
            "variable": ["total_precipitation"],
            "data_format": "grib",
        },
    }


@patch("datapi.ApiClient.get_remote")
def test_cds_get_remote_from_request(
    mock_get_remote: Mock, fake_cds: CDS, tp_request: DataRequest, tp_request_remote: dict
):
    mock_get_remote.return_value = TestRemote(
        request_uid="73dc0d2d-8288-4041-a84d-87e70772d5a8",
        status="successful",
        results_ready=True,
        request=tp_request_remote,
    )

    existing_requests = [tp_request_remote]
    remote = fake_cds.get_remote_from_request(tp_request, existing_requests=existing_requests)
    assert remote
    assert remote.request_uid == "73dc0d2d-8288-4041-a84d-87e70772d5a8"
    assert remote.request["request"] == tp_request.__dict__
