"""Test transform module."""

import tarfile
import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import polars as pl
import pytest
import xarray as xr

from openhexa.toolbox.era5.transform import (
    Period,
    aggregate_in_space,
    aggregate_in_time,
    calculate_relative_humidity,
    calculate_wind_speed,
    create_masks,
)


@pytest.fixture
def sample_boundaries() -> gpd.GeoDataFrame:
    fp = Path(__file__).parent / "data" / "geoms.parquet"
    return gpd.read_parquet(fp)


@pytest.fixture
def sample_dataset() -> xr.Dataset:
    archive = Path(__file__).parent / "data" / "sample_2m_temperature.zarr.tar.gz"
    with tempfile.TemporaryDirectory() as tmp_dir:
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(path=tmp_dir, filter="data")
        ds = xr.open_zarr(Path(tmp_dir) / "2m_temperature.zarr")
        ds.load()
        return ds


def test_create_masks(sample_boundaries, sample_dataset):
    masks = create_masks(gdf=sample_boundaries, id_column="boundary_id", ds=sample_dataset)
    # We have 4 boundaries in the sample data and the dataset has 21x21 lat/lon points
    assert masks.shape == (4, 21, 21)
    assert masks.dims == ("boundary", "latitude", "longitude")
    # Each boolean mask should contain only 0s and 1s, and at least some 1s
    assert sorted(np.unique(masks.data).tolist()) == [0, 1]
    assert np.count_nonzero(masks.data) > 100


@pytest.fixture
def sample_masks(sample_boundaries, sample_dataset) -> xr.DataArray:
    return create_masks(gdf=sample_boundaries, id_column="boundary_id", ds=sample_dataset)


def test_aggregate_in_space(sample_dataset, sample_masks):
    ds = sample_dataset.mean(dim="step")
    df = aggregate_in_space(ds=ds, masks=sample_masks, data_var="t2m", agg="mean")

    # We have 4 boundaries and 9 days in the sample data, so shape should be 9*4=36 rows
    # and 3 columns (boundary, time, value)
    assert df.shape == (36, 3)

    expected = pl.Schema({"boundary": pl.String, "time": pl.Date, "value": pl.Float64})
    assert df.schema == expected

    assert df["boundary"].n_unique() == 4
    assert df["time"].n_unique() == 9

    assert pytest.approx(df["value"].min(), 0.1) == 302.38
    assert pytest.approx(df["value"].max(), 0.1) == 307.44

    # The following aggregation methods do not make sense for 2m_temperature,
    # but values should match expected results nonetheless
    df = aggregate_in_space(ds=ds, masks=sample_masks, data_var="t2m", agg="sum")
    assert df.shape == (36, 3)
    assert pytest.approx(df["value"].min(), 0.1) == 11589.94
    assert pytest.approx(df["value"].max(), 0.1) == 68461.84
    df = aggregate_in_space(ds=ds, masks=sample_masks, data_var="t2m", agg="max")
    assert df.shape == (36, 3)
    assert pytest.approx(df["value"].min(), 0.1) == 305.08
    assert pytest.approx(df["value"].max(), 0.1) == 308.07


def test_aggregate_in_time(sample_masks, sample_dataset):
    ds = sample_dataset.mean(dim="step")
    df = aggregate_in_space(ds=ds, masks=sample_masks, data_var="t2m", agg="mean")

    weekly = aggregate_in_time(df, Period.WEEK, agg="mean")
    # 4 boundaries * 2 weeks = 8 rows
    assert weekly.shape[0] == 8
    assert weekly.schema == pl.Schema(
        {"boundary": pl.String, "period": pl.String, "value": pl.Float64},
    )
    assert set(weekly.columns) == {"boundary", "period", "value"}
    assert weekly["period"].str.starts_with("2025W").all()

    sunday_weekly = aggregate_in_time(df, Period.WEEK_SUNDAY, agg="mean")
    assert sunday_weekly["period"].str.starts_with("2025SunW").all()

    monthly = aggregate_in_time(df, Period.MONTH, agg="mean")
    assert monthly.shape[0] == 8  # 4 boundaries * 2 months
    assert "202503" in monthly["period"].to_list()


def test_calculate_relative_humidty():
    t2m = xr.DataArray(
        np.array([[[300.0, 305.0]], [[310.0, 290.0]]]),
        dims=["time", "latitude", "longitude"],
        coords={
            "time": ["2025-01-01", "2025-01-02"],
            "latitude": [45.0],
            "longitude": [10.0, 11.0],
        },
    )

    # when t2m == d2m, RH should be 100%
    d2m = t2m.copy()
    result = calculate_relative_humidity(t2m, d2m)

    assert "rh" in result.data_vars
    assert result["rh"].dims == ("time", "latitude", "longitude")
    assert result["rh"].attrs["units"] == "%"
    assert np.allclose(result["rh"].values, 100.0, rtol=0.01)

    # RH should be between 0 and 100% with lower dewpoint
    d2m_lower = t2m - 10.0
    result2 = calculate_relative_humidity(t2m, d2m_lower)
    assert (result2["rh"] < 100.0).all()
    assert (result2["rh"] > 0.0).all()

    # check that clipping works (dewpoint higher than temperature = invalid, should clip to 100%)
    d2m_higher = t2m + 5.0
    result_clipped = calculate_relative_humidity(t2m, d2m_higher)
    assert (result_clipped["rh"] <= 100.0).all()


def test_calculate_wind_speed():
    u10 = xr.DataArray(
        np.array([[[0.0, 3.0]], [[4.0, 5.0]]]),
        dims=["time", "latitude", "longitude"],
        coords={
            "time": ["2025-01-01", "2025-01-02"],
            "latitude": [45.0],
            "longitude": [10.0, 11.0],
        },
    )
    v10 = xr.DataArray(
        np.array([[[0.0, 4.0]], [[0.0, 12.0]]]),
        dims=["time", "latitude", "longitude"],
        coords={
            "time": ["2025-01-01", "2025-01-02"],
            "latitude": [45.0],
            "longitude": [10.0, 11.0],
        },
    )

    result = calculate_wind_speed(u10, v10)

    assert "ws" in result.data_vars
    assert result["ws"].dims == ("time", "latitude", "longitude")
    assert result["ws"].attrs["units"] == "m/s"

    expected = np.array([[[0.0, 5.0]], [[4.0, 13.0]]])
    assert np.allclose(result["ws"].values, expected, rtol=1e-10)

    # wind speed should always be non-negative
    assert (result["ws"] >= 0).all()
