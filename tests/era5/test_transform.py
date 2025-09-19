"""Test transform module."""

import tarfile
import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import polars as pl
import pytest
import xarray as xr

from era5.transform import Period, aggregate_in_space, aggregate_in_time, create_masks


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
    df = aggregate_in_space(ds=ds, masks=sample_masks, variable="t2m", agg="mean")

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
    df = aggregate_in_space(ds=ds, masks=sample_masks, variable="t2m", agg="sum")
    assert df.shape == (36, 3)
    assert pytest.approx(df["value"].min(), 0.1) == 11589.94
    assert pytest.approx(df["value"].max(), 0.1) == 68461.84
    df = aggregate_in_space(ds=ds, masks=sample_masks, variable="t2m", agg="max")
    assert df.shape == (36, 3)
    assert pytest.approx(df["value"].min(), 0.1) == 305.08
    assert pytest.approx(df["value"].max(), 0.1) == 308.07


def test_aggregate_in_time(sample_masks, sample_dataset):
    ds = sample_dataset.mean(dim="step")
    df = aggregate_in_space(ds=ds, masks=sample_masks, variable="t2m", agg="mean")

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
