"""Feature tests for the Expectations class.

These tests run end-to-end checks with real DataFrames and expectations.yml
to ensure that validation behaves correctly across scenarios.
"""

from pathlib import Path

import pandas as pd
import pytest
import yaml

from openhexa.toolbox.expectation.expectations import Expectations


@pytest.fixture
def valid_df() -> pd.DataFrame:
    """Return a valid DataFrame that matches the expectations schema.
    
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
def expectations_file(tmp_path: str) -> str:
    """Create a valid expectations.yml file that matches the valid_df fixture.
    
    Returns:
     Path
    """
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 3, "no_rows": 3},
        "columns": {
            "age": {"type": "int64", "minimum": 20, "maximum": 60, "not-null": True},
            "height": {"type": "float64", "minimum": 5, "maximum": 7, "not-null": False},
            "gender": {
                "type": "object",
                "classes": ["male", "female"],
                "not-null": True,
                "length-between": [4, 6],
            },
        },
    }
    file_path = tmp_path / "expectations.yml"
    with Path.open(file_path, "w") as f:
        yaml.safe_dump(expectations, f)
    return str(file_path)


def test_valid_dataset_passes(valid_df: pd.DataFrame, expectations_file: str, capsys: None):
    """Ensure that a valid DataFrame passes validation successfully."""
    validator = Expectations(valid_df, expectations_file)
    validator.validate_expectations()
    captured = capsys.readouterr()
    assert "Validation Results" in captured.out or "Statistics" in captured.out


def test_empty_dataset_fails(expectations_file: str):
    """Raise ValueError if expectations require non-empty DataFrame but it is empty."""
    df = pd.DataFrame(columns=["age", "height", "gender"])
    validator = Expectations(df, expectations_file)
    with pytest.raises(ValueError, match="empty"):
        validator.validate_expectations()


def test_wrong_column_count(valid_df: pd.DataFrame, tmp_path: str):
    """Raise ValueError if DataFrame has different number of columns than expected."""
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 4},
        "columns": {"age": {"type": "int64"}},
    }
    file_path = tmp_path / "expectations.yml"
    yaml.safe_dump(expectations, Path.open(file_path, "w"))
    validator = Expectations(valid_df, str(file_path))
    with pytest.raises(ValueError, match="Columns mismatch"):
        validator.validate_expectations()


def test_missing_column_in_dataset(valid_df: pd.DataFrame, tmp_path: str):
    """Raise ValueError if expectations define a column missing in the DataFrame."""
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 3, "no_rows": 3},
        "columns": {
            "weight": {"type": "float64"}  # column doesn't exist in DF
        },
    }
    file_path = tmp_path / "expectations.yml"
    yaml.safe_dump(expectations, Path.open(file_path, "w"))
    validator = Expectations(valid_df, str(file_path))
    with pytest.raises(ValueError, match="missing in dataset"):
        validator.validate_expectations()


def test_invalid_length_between(valid_df: pd.DataFrame, tmp_path: str):
    """Raise ValueError if 'length-between' has more than 2 entries."""
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 3, "no_rows": 3},
        "columns": {
            "gender": {"type": "object", "length-between": [1, 2, 3]},
        },
    }
    file_path = tmp_path / "expectations.yml"
    yaml.safe_dump(expectations, Path.open(file_path, "w"))
    validator = Expectations(valid_df, str(file_path))
    with pytest.raises(ValueError, match="length-between"):
        validator.validate_expectations()
