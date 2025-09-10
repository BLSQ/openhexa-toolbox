"""Unit tests for the Expectations class.

These tests focus on validating internal logic, argument validation,
and YAML schema checks, without running full Great Expectations validation.
"""

from pathlib import Path

import pandas as pd
import pytest
import yaml

from openhexa.toolbox.expectation.expectations import Expectations


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Return a simple sample DataFrame for unit tests.

    Returns:
        Test dataframe
    """
    return pd.DataFrame(
        {
            "age": [25, 30, 40],
            "height": [5.5, 6.1, 5.9],
            "gender": ["male", "female", "male"],
        }
    )


@pytest.fixture
def tmp_expectations_file(tmp_path: str) -> str:
    """Create a temporary expectations.yml file with minimal valid schema.

    Used for tests that require a valid file path.

    Returns:
        Path
    """
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 3, "no_rows": 3},
        "columns": {
            "age": {"type": "int64", "minimum": 20, "maximum": 60, "not-null": True},
            "height": {"type": "float64", "minimum": 5, "maximum": 7, "not-null": False},
            "gender": {"type": "object", "classes": ["male", "female"], "not-null": True},
        },
    }
    file_path = tmp_path / "expectations.yml"
    with Path.open(file_path, "w") as f:
        yaml.safe_dump(expectations, f)
    return str(file_path)


def test_init_with_invalid_dataset_type():
    """Raise ValueError if dataset is not a pandas DataFrame."""
    with pytest.raises(ValueError, match="dataset should be a pandas dataframe"):
        Expectations(dataset="not-a-df", expectations_yml_file=None)


def test_init_with_invalid_file_type(sample_df: pd.DataFrame):
    """Raise ValueError if expectations_yml_file is not a string."""
    with pytest.raises(ValueError, match="expectations_yml_file should be a string"):
        Expectations(dataset=sample_df, expectations_yml_file=123)


def test_missing_yaml_file(sample_df: pd.DataFrame):
    """Raise FileNotFoundError if expectations.yml file does not exist."""
    validator = Expectations(sample_df, "non_existent.yml")
    with pytest.raises(FileNotFoundError):
        validator._read_definitions()


def test_missing_dataframe_section(sample_df: pd.DataFrame, tmp_path: str):
    """Raise ValueError if 'dataframe' section is missing from expectations.yml."""
    file = tmp_path / "bad.yml"
    yaml.safe_dump({"columns": {}}, Path.open(file, "w", encoding="utf-8"))
    validator = Expectations(sample_df, str(file))
    with pytest.raises(ValueError, match="must contain 'dataframe'"):
        validator._read_definitions()


def test_missing_column_section(sample_df: pd.DataFrame, tmp_path: str):
    """Raise ValueError if 'columns' section is missing from expectations.yml."""
    file = tmp_path / "bad.yml"
    yaml.safe_dump({"dataframe": {}}, Path.open(file, "w", encoding="utf-8"))
    validator = Expectations(sample_df, str(file))
    with pytest.raises(ValueError, match="must contain 'columns'"):
        validator._read_definitions()
