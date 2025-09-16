"""
Feature tests for the Expectations class.

These tests perform end-to-end validation using real DataFrames and
`expectations.yml` to ensure correct behavior across different scenarios.
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
def valid_df() -> pd.DataFrame:
    """A valid DataFrame that matches the expectations schema."""
    return pd.DataFrame(
        {
            "age": [25, 30, 40],
            "height": [5.5, 6.1, 5.9],
            "gender": ["male", "female", "male"],
        }
    )


@pytest.fixture
def expectations_file(tmp_path: Path) -> str:
    """A valid expectations.yml file matching the `valid_df` fixture."""
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 3, "no_rows": 3},
        "columns": {
            "age": {"type": "int64", "minimum": 20, "maximum": 60, "not-null": True},
            "height": {
                "type": "float64",
                "minimum": 5,
                "maximum": 7,
                "not-null": False,
            },
            "gender": {
                "type": "object",
                "classes": ["male", "female"],
                "not-null": True,
                "length-between": [4, 6],
            },
        },
    }
    file_path = tmp_path / "expectations.yml"
    with file_path.open("w") as f:
        yaml.safe_dump(expectations, f)
    return str(file_path)


def _write_expectations(tmp_path: Path, expectations: dict) -> str:
    """Utility to write expectations dict to a temp file and return its path."""
    file_path = tmp_path / "expectations.yml"
    with file_path.open("w") as f:
        yaml.safe_dump(expectations, f)
    return str(file_path)


# -----------------
# Tests
# -----------------

def test_empty_dataset_fails(expectations_file: str):
    """Fails if expectations require a non-empty DataFrame but it's empty."""
    df = pd.DataFrame(columns=["age", "height", "gender"])
    validator = Expectations(df, expectations_file)
    with pytest.raises(ValueError, match="empty"):
        validator.validate_expectations()


def test_wrong_column_count(valid_df: pd.DataFrame, tmp_path: Path):
    """Fails if DataFrame has different number of columns than expected."""
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 4},
        "columns": {"age": {"type": "int64"}},
    }
    validator = Expectations(valid_df, _write_expectations(tmp_path, expectations))
    with pytest.raises(ValueError, match="Columns mismatch"):
        validator.validate_expectations()


def test_missing_column_in_dataset(valid_df: pd.DataFrame, tmp_path: Path):
    """Fails if expectations define a column missing from the DataFrame."""
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 3, "no_rows": 3},
        "columns": {"weight": {"type": "float64"}},  # missing column
    }
    validator = Expectations(valid_df, _write_expectations(tmp_path, expectations))
    with pytest.raises(ValueError, match="missing in dataset"):
        validator.validate_expectations()


def test_invalid_length_between(valid_df: pd.DataFrame, tmp_path: Path):
    """Fails if 'length-between' is defined with more than 2 entries."""
    expectations = {
        "dataframe": {"size": "not empty", "no_columns": 3, "no_rows": 3},
        "columns": {"gender": {"type": "object", "length-between": [1, 2, 3]}},
    }
    validator = Expectations(valid_df, _write_expectations(tmp_path, expectations))
    with pytest.raises(ValueError, match="length-between"):
        validator.validate_expectations()
