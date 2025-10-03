"""Test requests to the CDS API and handling of responses."""

import shutil
import tarfile
import tempfile
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock

import numpy as np
import pytest
import xarray as xr

from openhexa.toolbox.era5.extract import (
    Client,
    Remote,
    Request,
    bound_date_range,
    get_date_range,
    get_temporal_chunks,
    grib_to_zarr,
    prepare_requests,
    retrieve_requests,
    submit_requests,
)


@pytest.fixture
def mock_client() -> Mock:
    client = Mock(spec=Client)
    collection = Mock()
    collection.begin_datetime = datetime(2020, 1, 1)
    collection.end_datetime = datetime(2025, 4, 4)
    client.get_collection.return_value = collection
    return client


@pytest.fixture
def mock_request() -> Request:
    return {
        "variable": ["2m_temperature"],
        "year": "2025",
        "month": "03",
        "day": ["28", "29", "30", "31"],
        "time": ["01:00", "07:00", "13:00", "19:00"],
        "data_format": "grib",
        "download_format": "unarchived",
        "area": [10, -1, 8, 1],
    }


@pytest.fixture
def sample_grib_file_march() -> Path:
    """Small sample GRIB file with 2m_temperature data for March."""
    return Path(__file__).parent / "data" / "sample_202503.grib"


@pytest.fixture
def sample_grib_file_april() -> Path:
    """Small sample GRIB file with 2m_temperature data for April."""
    return Path(__file__).parent / "data" / "sample_202504.grib"


@pytest.fixture
def sample_zarr_store() -> Path:
    """Path to a sample Zarr store with data from sample GRIB files."""
    return Path(__file__).parent / "data" / "sample_2m_temperature.zarr.tar.gz"


def test_prepare_requests(mock_client):
    requests = prepare_requests(
        client=mock_client,
        dataset_id="reanalysis-era5-land",
        start_date=date(2025, 3, 28),
        end_date=date(2025, 4, 5),
        variable="2m_temperature",
        area=[10, -1, 8, 1],
        zarr_store=Path("/tmp/do-not-exist.zarr"),
    )

    # The mock client has collection end date of 2025-04-04
    # So we expect requests only up to 2025-04-04 despire the requested end date
    # We also expect 2 prepared requests: one for March and one for April
    assert len(requests) == 2
    assert requests[0] == {
        "variable": ["2m_temperature"],
        "year": "2025",
        "month": "03",
        "day": ["28", "29", "30", "31"],
        "time": ["01:00", "07:00", "13:00", "19:00"],
        "data_format": "grib",
        "download_format": "unarchived",
        "area": [10, -1, 8, 1],
    }
    assert requests[1] == {
        "variable": ["2m_temperature"],
        "year": "2025",
        "month": "04",
        "day": ["01", "02", "03", "04"],
        "time": ["01:00", "07:00", "13:00", "19:00"],
        "data_format": "grib",
        "download_format": "unarchived",
        "area": [10, -1, 8, 1],
    }


def test_prepare_requests_with_existing_data(sample_zarr_store, mock_client):
    # Sample zarr store has data from 2025-03-28 to 2025-04-05
    with tempfile.TemporaryDirectory() as tmpdir, tarfile.open(sample_zarr_store, "r:gz") as tar:
        tar.extractall(path=tmpdir, filter="data")
        zarr_store = Path(tmpdir) / "2m_temperature.zarr"
        requests = prepare_requests(
            client=mock_client,
            dataset_id="reanalysis-era5-land",
            start_date=date(2025, 3, 27),
            end_date=date(2025, 4, 6),
            variable="2m_temperature",
            area=[10, -1, 8, 1],
            zarr_store=zarr_store,
        )

    # In the sample zarr store, we already have data between 2025-03-28 and 2025-04-05
    # In the mock client, the end date of the collection is 2025-04-04
    # As a result, we expect only 1 request to be prepared: for 2025-03-27
    assert len(requests) == 1
    assert requests[0] == {
        "variable": ["2m_temperature"],
        "year": "2025",
        "month": "03",
        "day": ["27"],
        "time": ["01:00", "07:00", "13:00", "19:00"],
        "data_format": "grib",
        "download_format": "unarchived",
        "area": [10, -1, 8, 1],
    }


def test_submit_requests(mock_client, mock_request):
    remote = Mock(spec=Remote)
    mock_client.submit.return_value = remote
    remotes = submit_requests(
        client=mock_client,
        collection_id="reanalysis-era5-land",
        requests=[mock_request, mock_request],
    )
    # We expect 1 remote per request here
    assert len(remotes) == 2


def test_retrieve_requests(mock_client, mock_request):
    remote1 = Mock(spec=Remote)
    remote1.request_id = "remote1"
    remote1.request = mock_request
    remote1.status = "successful"
    remote1.results_ready = True
    remote1.download = Mock(side_effect=lambda target: Path(target).touch())

    remote2 = Mock(spec=Remote)
    remote2.request_id = "remote2"
    remote2.request = mock_request
    remote2.status = "successful"
    remote2.results_ready = True
    remote2.download = Mock(side_effect=lambda target: Path(target).touch())

    mock_client.submit.side_effect = [remote1, remote2]

    with tempfile.TemporaryDirectory() as tmpdir:
        retrieve_requests(
            client=mock_client,
            dataset_id="reanalysis-era5-land",
            requests=[mock_request, mock_request],
            src_dir=Path(tmpdir),
            wait=0,
        )
        # We expect 2 grib files to have been downloaded
        assert len(list(Path(tmpdir).glob("*.grib"))) == 2


def test_get_date_range():
    start = date(2024, 12, 27)
    end = date(2025, 1, 3)
    result = get_date_range(start, end)
    assert result == [
        date(2024, 12, 27),
        date(2024, 12, 28),
        date(2024, 12, 29),
        date(2024, 12, 30),
        date(2024, 12, 31),
        date(2025, 1, 1),
        date(2025, 1, 2),
        date(2025, 1, 3),
    ]


def test_get_date_range_single_day():
    start = date(2025, 3, 15)
    end = date(2025, 3, 15)
    result = get_date_range(start, end)
    assert result == [date(2025, 3, 15)]


def test_get_date_range_invalid():
    start = date(2025, 3, 15)
    end = date(2025, 3, 14)
    with pytest.raises(ValueError, match="Start date must be before end date"):
        get_date_range(start, end)


def test_bound_date_range():
    start = date(2024, 12, 27)
    end = date(2025, 1, 3)
    collection_start = date(2024, 1, 1)
    collection_end = date(2024, 12, 31)
    bounded_start, bounded_end = bound_date_range(start, end, collection_start, collection_end)
    assert bounded_start == date(2024, 12, 27)
    assert bounded_end == date(2024, 12, 31)


def test_get_temporal_chunks():
    dates = [
        date(2024, 1, 31),
        date(2024, 2, 1),
        date(2024, 2, 15),
        date(2024, 3, 1),
    ]
    result = get_temporal_chunks(dates)

    # We expect 3 chunks: one per month
    assert len(result) == 3
    assert result[0]["year"] == "2024"
    assert result[0]["month"] == "01"
    assert result[0]["day"] == ["31"]
    assert result[1]["year"] == "2024"
    assert result[1]["month"] == "02"
    assert result[1]["day"] == ["01", "15"]
    assert result[2]["year"] == "2024"
    assert result[2]["month"] == "03"
    assert result[2]["day"] == ["01"]


def test_grib_to_zarr(sample_grib_file_march, sample_grib_file_april):
    def _move_grib_to_tmp_dir(grib_file: Path, dst_dir: Path):
        dst_file = dst_dir / grib_file.name
        shutil.copy(grib_file, dst_file)

    with tempfile.TemporaryDirectory() as tmpdir:
        grib_dir = Path(tmpdir) / "grib_files"
        grib_dir.mkdir()
        _move_grib_to_tmp_dir(sample_grib_file_march, grib_dir)
        _move_grib_to_tmp_dir(sample_grib_file_april, grib_dir)
        zarr_store = Path(tmpdir) / "store.zarr"
        grib_to_zarr(grib_dir, zarr_store, "t2m")
        # We expect the Zarr store have been created and contains data from both GRIB files
        # Sample GRIB files contains data from 2025-03-28 to 2025-04-05 (9 days)
        assert zarr_store.exists()
        ds = xr.open_zarr(zarr_store, decode_timedelta=True)
        assert "t2m" in ds
        times = np.array(ds["time"])
        assert np.datetime_as_string(times[0], unit="D") == "2025-03-28"
        assert np.datetime_as_string(times[-1], unit="D") == "2025-04-05"
        assert len(times) == 9 * 4  # 9 days, 4 time steps per day
