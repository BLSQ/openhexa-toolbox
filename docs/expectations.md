# Expectations Module Documentation

## Overview

The `Expectations` class provides a structured way to **validate datasets** against defined data quality rules using [Great Expectations](https://greatexpectations.io/).  

It supports both **DataFrame-level** and **Column-level** checks, with validation rules defined in an external `expectations.yml` file.

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
  - String length validation

---

## Installation Requirements

Ensure the following dependencies are installed:

```bash
pip install pandas pyyaml great-expectations
````

---

## Class: `Expectations`

### Initialization

```python
Expectations(
    dataset: pd.DataFrame,
    expectations_yml_file: str | None = None
)
```

#### Parameters

* **dataset** (`pd.DataFrame`)
  The dataset to validate.
* **expectations\_yml\_file** (`str | None`, optional)
  Path to the expectations YAML file.

  * If not provided, defaults to `expectations.yml` located in the caller’s directory.

#### Raises

* `ValueError`:

  * If `dataset` is not a DataFrame
  * If `expectations_yml_file` is not a string
* `FileNotFoundError`:
  If `expectations.yml` file is missing
* `yaml.YAMLError`:
  If the YAML file cannot be parsed

---

## YAML Expectations Schema

The `expectations.yml` file must define **two sections**:

```yaml
dataframe:
  size: "not empty"        # or "empty"
  no_rows: 3
  no_columns: 5

columns:
  age:
    type: int64
    minimum: 0
    maximum: 120
    not-null: true

  gender:
    type: string
    classes: ["male", "female", "other"]

  phone:
    type: string
    length-between: [10, 15]

  shirt_size:
    type: string
    classes: ["s", "m", "l", "xl"]
```

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

#### Performs:

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

---

## Example Usage

```python
import pandas as pd
from expectations import Expectations

# Example dataset
df = pd.DataFrame(
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

On execution, a Great Expectations **checkpoint run report** is generated and printed:

```text
Validation Checkpoint "context" Results:
  - Success: True
  - Details: <summary of checks>
```

---

## Best Practices

* Store `expectations.yml` alongside your pipeline scripts for maintainability.
* Version control `expectations.yml` to track schema changes over time.
* Start with broad rules (row/column counts, non-null constraints) and refine incrementally.
