import inspect
import pathlib

import great_expectations as gx
import pandas as pd
import yaml


class Expectations:
    """A utility class for validating the quality of a dataset against a set of expectations.

    This class supports:
    - DataFrame-level checks (row count, column count, emptiness).
    - Column-level checks (type, numeric ranges, allowed values, nullability, string length).
    """

    def __init__(self, dataset: pd.DataFrame, expectations_yml_file: str | None = None):
        """Initialize the Expectations validator.

        Args:
            dataset (pd.DataFrame): The dataset to validate.
            expectations_yml_file (str | None, optional): Path to expectations YAML file.
                If not provided, defaults to `expectations.yml` in the caller's directory.

        Raises:
            ValueError: If `dataset` is not a pandas DataFrame or
                if `expectations_yml_file` is not a string.
        """
        if not isinstance(expectations_yml_file, str) and expectations_yml_file is not None:
            raise ValueError("expectations_yml_file should be a string")

        if not isinstance(dataset, pd.DataFrame):
            raise ValueError("dataset should be a pandas dataframe")

        if expectations_yml_file is None:
            caller_file = inspect.stack()[1].filename
            caller_dir = pathlib.Path(pathlib.Path(caller_file).resolve()).parent
            self.expectations_yml_file = f"{caller_dir}/expectations.yml"
        else:
            self.expectations_yml_file = expectations_yml_file

        self.dataset = dataset
        # normalize to pandas dtypes
        self.numeric_types = {"int64", "int32", "float64", "float32"}
        self.string_types = {"object", "string"}

    def _read_definitions(self) -> dict:
        """Load expectations definitions from the YAML file.

        Returns:
            dict: Parsed expectations dictionary.

        Raises:
            FileNotFoundError: If expectations.yml does not exist.
            ValueError: If parsing fails or required keys are missing.
        """
        try:
            with pathlib.Path(self.expectations_yml_file).open(encoding="utf-8") as file:
                expectations = yaml.safe_load(file) or {}
        except FileNotFoundError:
            raise FileNotFoundError("Error: 'expectations.yml' not found.") from None
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing 'expectations.yml' file: {e}") from e

        # basic schema guard
        if "dataframe" not in expectations:
            raise ValueError("expectations.yml must contain 'dataframe' section.")
        if "columns" not in expectations:
            raise ValueError("expectations.yml must contain 'columns' section.")

        return expectations

    def validate_expectations(self):
        """Validate the dataset against expectations defined in the YAML file.

        - DataFrame-level checks:
          * Size (empty or not empty)
          * Number of rows
          * Number of columns

        - Column-level checks:
          * Existence of expected columns
          * Data type enforcement
          * Numeric ranges (minimum, maximum)
          * Nullability
          * Allowed categorical values (classes)
          * String length constraints

        Raises:
            ValueError: If expectations do not match dataset properties.
        """
        context = gx.get_context()
        data_source = context.data_sources.add_pandas(name="pandas")
        data_asset = data_source.add_dataframe_asset(name="pd_dataframe_asset")

        batch_definition = data_asset.add_batch_definition_whole_dataframe("batch-def")
        batch_parameters = {"dataframe": self.dataset}
        batch = batch_definition.get_batch(batch_parameters=batch_parameters)  # noqa: F841

        expectations = self._read_definitions()

        # ------------------------
        # Dataframe-level checks
        # ------------------------
        size_expect = expectations["dataframe"].get("size")
        if size_expect == "not empty" and self.dataset.empty:
            raise ValueError("DataFrame is empty but expectations require non-empty.")
        if size_expect == "empty" and not self.dataset.empty:
            raise ValueError("DataFrame is not empty but expectations require empty.")

        expected_no_columns = expectations["dataframe"].get("no_columns")
        if expected_no_columns is not None:
            if self.dataset.shape[1] != int(expected_no_columns):
                raise ValueError(f"Columns mismatch: expected {expected_no_columns}, got {self.dataset.shape[1]}")

        expected_no_rows = expectations["dataframe"].get("no_rows")
        if expected_no_rows is not None:
            if self.dataset.shape[0] != int(expected_no_rows):
                raise ValueError(f"Rows mismatch: expected {expected_no_rows}, got {self.dataset.shape[0]}")

        # ------------------------
        # Column-level checks
        # ------------------------
        suite = context.suites.add(gx.core.expectation_suite.ExpectationSuite(name="expectations_suite"))

        for column, column_expectation in expectations["columns"].items():
            # column presence
            if column not in self.dataset.columns:
                raise ValueError(
                    f"""
                            Column '{column}' defined in expectations.yml but missing in dataset.
                                 """
                )

            col_type = column_expectation.get("type")

            # ------------------------
            # datatype
            # ------------------------
            if col_type:
                suite.add_expectation(gx.expectations.ExpectColumnValuesToBeOfType(column=column, type_=col_type))

            # ------------------------
            # numeric ranges
            # ------------------------
            if col_type in self.numeric_types:
                min_val = column_expectation.get("minimum")
                max_val = column_expectation.get("maximum")

                if min_val is not None or max_val is not None:
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToBeBetween(
                            column=column,
                            min_value=min_val,
                            max_value=max_val,
                        )
                    )

            # ------------------------
            # not-null
            # ------------------------
            if column_expectation.get("not-null", False):
                suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=column))

            # ------------------------
            # string expectations
            # ------------------------
            if col_type in self.string_types:
                classes = column_expectation.get("classes")
                if classes:
                    suite.add_expectation(
                        gx.expectations.ExpectColumnDistinctValuesToBeInSet(column=column, value_set=classes)
                    )

                length_between = column_expectation.get("length-between")
                if length_between:
                    if not isinstance(length_between, (list, tuple)):
                        raise ValueError(
                            f"""
                                'length-between' for column {column} must be a list or tuple.
                                """
                        )
                    if len(length_between) == 1:
                        suite.add_expectation(
                            gx.expectations.ExpectColumnValueLengthsToEqual(column=column, value=length_between[0])
                        )
                    elif len(length_between) == 2:
                        suite.add_expectation(
                            gx.expectations.ExpectColumnValueLengthsToBeBetween(
                                column=column,
                                min_value=min(length_between),
                                max_value=max(length_between),
                            )
                        )
                    else:
                        raise ValueError(
                            f"""
                                    Column {column}: 'length-between' should have 1 or 2 entries.
                                         """
                        )

        # ------------------------
        # Validation definition
        # ------------------------
        validation_definition = context.validation_definitions.add(
            gx.core.validation_definition.ValidationDefinition(
                name="validation definition", data=batch_definition, suite=suite
            )
        )

        checkpoint = context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(name="context", validation_definitions=[validation_definition])
        )
        checkpoint_result = checkpoint.run(batch_parameters=batch_parameters)
        print(checkpoint_result.describe())


if __name__ == "__main__":
    df = pd.DataFrame(
        {
            "age": [19, 20, 30],
            "height": [7, 5, 6],
            "gender": ["male", "female", None],
            "phone": ["0711222333", "0722111333", "+256744123432"],
            "shirt_size": ["s", "m", "l"],
        }
    )
    validator = Expectations(df)
    validator.validate_expectations()
