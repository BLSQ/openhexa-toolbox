"""
Unit tests for the Expectations class.

These tests focus on validating internal logic, argument validation,
and YAML schema checks without running full Great Expectations validation.
"""

from pathlib import Path

import pandas as pd
import pytest
import yaml

from openhexa.toolbox.expectation.expectations import Expectations


# -----------------
# Fixtures
# -----------------

@pytest.fixture
def sample_df() -> pd.DataFrame:
    """A simple sample DataFrame for unit tests."""
    return pd.DataFrame(
        {
            "age": [25, 30, 40],
            "height": [5.5, 6.1, 5.9],
            "gender": ["male", "female", "male"],
        }
    )


@pytest.fixture
def tmp_expectations_file(tmp_path: Path) -> str:
    """A temporary expectations.yml file with a minimal valid schema."""
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 3, "no_rows": 3},
        "columns": {
            "age": {"type": "int64", "minimum": 20, "maximum": 60, "not-null": True},
            "height": {"type": "float64", "minimum": 5, "maximum": 7, "not-null": False},
            "gender": {
                "type": "object",
                "classes": ["male", "female"],
                "not-null": True,
            },
        },
    }
    file_path = tmp_path / "expectations.yml"
    with file_path.open("w") as f:
        yaml.safe_dump(expectations, f)
    return str(file_path)


def _write_yaml(tmp_path: Path, content: dict, filename: str = "bad.yml") -> str:
    """Utility to write a YAML file and return its path."""
    file_path = tmp_path / filename
    with file_path.open("w") as f:
        yaml.safe_dump(content, f)
    return str(file_path)


# -----------------
# Tests
# -----------------

def test_init_with_invalid_dataset_type():
    """Fails if dataset is not a pandas DataFrame."""
    with pytest.raises(ValueError, match="dataset should be a pandas or polars dataframe"):
        Expectations(dataset="not-a-df", expectations_yml_file=None)


def test_init_with_invalid_file_type(sample_df: pd.DataFrame):
    """Fails if expectations_yml_file is not a string."""
    with pytest.raises(ValueError, match="expectations_yml_file should be a string"):
        Expectations(dataset=sample_df, expectations_yml_file=123)


def test_missing_yaml_file(sample_df: pd.DataFrame):
    """Fails if expectations.yml file does not exist."""
    validator = Expectations(sample_df, "non_existent.yml")
    with pytest.raises(FileNotFoundError):
        validator._read_definitions()


def test_missing_dataframe_section(sample_df: pd.DataFrame, tmp_path: Path):
    """Fails if 'dataframe' section is missing from expectations.yml."""
    bad_file = _write_yaml(tmp_path, {"columns": {}})
    validator = Expectations(sample_df, bad_file)
    with pytest.raises(ValueError, match="must contain 'dataframe'"):
        validator._read_definitions()


def test_missing_column_section(sample_df: pd.DataFrame, tmp_path: Path):
    """Fails if 'columns' section is missing from expectations.yml."""
    bad_file = _write_yaml(tmp_path, {"dataframe": {}})
    validator = Expectations(sample_df, bad_file)
    with pytest.raises(ValueError, match="must contain 'columns'"):
        validator._read_definitions()
