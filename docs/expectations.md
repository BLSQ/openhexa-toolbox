# Expectations Module

## Overview

The `Expectations` class provides a structured way to **validate datasets** against defined data quality rules using [Great Expectations](https://greatexpectations.io/).

It supports both **DataFrame-level** and **Column-level** checks, with validation rules defined in an external `expectations.yml` file.

The class supports datasets in both **pandas** and **polars**, automatically normalizing to pandas for validation.

### Features

- **DataFrame-level checks**
  - Validate row/column count
  - Validate emptiness/non-emptiness
- **Column-level checks**
  - Column existence
  - Data type enforcement
  - Numeric range validation
  - Nullability checks
  - Allowed categorical values
  - String length validation (fixed length or range)

---

## Installation Requirements

Ensure the following dependencies are installed:

```bash
pip install pandas polars pyyaml great-expectations
````

---

## Class: `Expectations`

### Initialization

```python
Expectations(
    dataset: pd.DataFrame | pl.DataFrame,
    expectations_yml_file: str | None = None
)
```

#### Parameters

* **dataset** (`pd.DataFrame | pl.DataFrame`)
  The dataset to validate. Both pandas and polars are supported.
  If a `polars.DataFrame` is provided, it will be automatically converted to pandas for validation.

* **expectations\_yml\_file** (`str | None`, optional)
  Path to the expectations YAML file.
  If not provided, defaults to `expectations.yml` located in the caller’s directory.

#### Raises

* `ValueError`

  * If `dataset` is not a pandas or polars DataFrame
  * If `expectations_yml_file` is not a string
* `FileNotFoundError`
  If `expectations.yml` file is missing
* `yaml.YAMLError` or `ValueError`
  If the YAML file cannot be parsed or is missing required sections

---

## YAML Expectations Schema

The `expectations.yml` file must define **two sections**:

```yaml
dataframe:
  size: not empty # or empty
  no_columns: 5
  no_rows: 3

columns:
  age:
    type: int64
    minimum: 18
    maximum: 70
    not-null: true

  height:
    type: int64
    minimum: 5
    maximum: 8
    not-null: false

  gender:
    type: object
    classes:
      - male
      - female
      - other
    not-null: false

  phone:
    type: object
    not-null: false
    length-between:
      - 10
      - 13

  shirt_size:
    type: object
    classes:
      - s
      - m
      - l
    not-null: true
    length-between:
      - 1   # exact length of 1
```

---

## Supported Expectations Mapping

The YAML configuration is translated into the following **Great Expectations classes**:

| YAML Key                     | Great Expectations Class                                                      |
| ---------------------------- | ----------------------------------------------------------------------------- |
| `type`                       | `ExpectColumnValuesToBeOfType`                                                |
| `minimum` / `maximum`        | `ExpectColumnValuesToBeBetween` (only for numeric types)                      |
| `not-null: true`             | `ExpectColumnValuesToNotBeNull`                                               |
| `classes`                    | `ExpectColumnDistinctValuesToBeInSet`                                         |
| `length-between: [N]`        | `ExpectColumnValueLengthsToEqual` (exact length `N`)                          |
| `length-between: [min, max]` | `ExpectColumnValueLengthsToBeBetween` (string length between `min` and `max`) |

At the **DataFrame-level** (outside columns), the following checks are enforced internally:

* `size: not empty` → raises `ValueError` if DataFrame is empty
* `size: empty` → raises `ValueError` if DataFrame is not empty
* `no_columns` → raises `ValueError` if column count mismatches
* `no_rows` → raises `ValueError` if row count mismatches

---

## Methods

### `_read_definitions() -> dict`

Loads and validates expectations from the YAML file.

#### Returns

* Dictionary containing expectations.

#### Raises

* `FileNotFoundError`: If YAML file not found
* `ValueError`: If YAML is invalid or missing required sections (`dataframe`, `columns`)

---

### `validate_expectations()`

Validates the dataset against defined expectations.

#### Performs

* **DataFrame-level checks**

  * Enforces `size` (empty / not empty)
  * Enforces `no_rows` and `no_columns`

* **Column-level checks**

  * Ensures required columns exist
  * Validates data types
  * Validates numeric ranges
  * Enforces `not-null`
  * Restricts categorical values
  * Validates string length constraints

#### Raises

* `ValueError`: If dataset does not meet defined expectations
* `Exception`: If Great Expectations validation checkpoint fails

---

## Example Usage

```python
import polars as pl
from expectations import Expectations

# Example dataset
df = pl.DataFrame(
    {
        "age": [19, 20, 30],
        "height": [7, 5, 6],
        "gender": ["male", "female", None],
        "phone": ["0711222333", "0722111333", "+256744123432"],
        "shirt_size": ["s", "m", "l"],
    }
)

# Initialize with default expectations.yml in caller's directory
validator = Expectations(df)

# Run validation
validator.validate_expectations()
```

---

## Output

On execution, a Great Expectations **checkpoint run report** is generated.
If validation fails, an exception is raised with a detailed message.

Example success log:

```text
INFO:root:Data passed validation check.
```

Example failure:

```text
Exception: Data failed validation check!
{
  "success": false,
  "results": [...]
}
```

---

## Best Practices

* Store `expectations.yml` alongside your pipeline scripts for maintainability.
* Version control `expectations.yml` to track schema changes over time.
* Start with broad rules (row/column counts, non-null constraints) and refine incrementally.
* Use `polars` for data wrangling if performance is critical — the class will handle conversion to pandas for validation.

