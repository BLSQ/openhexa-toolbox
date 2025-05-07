from datetime import date

import pytest
from dateutil.relativedelta import relativedelta

from openhexa.sdk.workspaces.connection import DHIS2Connection
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dhis2 import _batch, _batch_dates, _iter_batches


def test_dhis2_init_connection():
    con = DHIS2Connection(
        url="http://localhost:8080",
        username="admin",
        password="district",
    )
    client = DHIS2(con)
    assert client.api.session.auth.username == "admin"


def test_dhis2_init_password():
    client = DHIS2(
        url="http://localhost:8080",
        username="admin",
        password="district",
    )
    assert client.api.session.auth.username == "admin"


@pytest.fixture
def client():
    return DHIS2(url="http://localhost:8080", username="admin", password="district")


def test_batch():
    items = list(range(10))
    batches = list(_batch(items, 3))
    assert len(batches) == 4
    assert batches[0] == (0, 1, 2)
    assert batches[1] == (3, 4, 5)
    assert batches[2] == (6, 7, 8)
    assert batches[3] == (9,)


def test_batch_dates():
    start = date(2023, 9, 1)
    end = date(2024, 3, 1)
    delta = relativedelta(months=1)
    batches = list(_batch_dates(start, end, delta))
    assert len(batches) == 6
    assert batches[0] == (date(2023, 9, 1), date(2023, 10, 1))
    assert batches[1] == (date(2023, 10, 1), date(2023, 11, 1))
    assert batches[2] == (date(2023, 11, 1), date(2023, 12, 1))
    assert batches[3] == (date(2023, 12, 1), date(2024, 1, 1))
    assert batches[4] == (date(2024, 1, 1), date(2024, 2, 1))
    assert batches[5] == (date(2024, 2, 1), date(2024, 3, 1))


def test_iter_batches():
    data_elements = ["dx1", "dx2", "dx3", "dx4", "dx5", "dx6", "dx7"]
    datasets = ["ds1", "ds2"]
    data_element_groups = ["deg1", "deg2"]
    org_units = ["ou1", "ou2", "ou3"]
    org_unit_groups = ["oug1"]
    periods = ["202301", "202302", "202303"]
    start_date = "2021-10-01"
    end_date = "2022-02-01"

    batches = list(
        _iter_batches(
            data_elements=data_elements,
            datasets=datasets,
            data_element_groups=data_element_groups,
            org_units=org_units,
            org_unit_groups=org_unit_groups,
            periods=periods,
            start_date=start_date,
            end_date=end_date,
            max_data_elements=5,
            max_datasets=2,
            max_data_element_groups=2,
            max_org_units=2,
            max_org_unit_groups=1,
            max_periods=3,
            max_dates_delta=relativedelta(months=2),
        )
    )

    assert len(batches) == 8

    assert batches[0] == {
        "data_elements": ("dx1", "dx2", "dx3", "dx4", "dx5"),
        "datasets": ("ds1", "ds2"),
        "data_element_groups": ("deg1", "deg2"),
        "org_units": ("ou1", "ou2"),
        "org_unit_groups": ("oug1",),
        "periods": ("202301", "202302", "202303"),
        "dates": (date(2021, 10, 1), date(2021, 12, 1)),
    }

    assert batches[7] == {
        "data_elements": ("dx6", "dx7"),
        "datasets": ("ds1", "ds2"),
        "data_element_groups": ("deg1", "deg2"),
        "org_units": ("ou3",),
        "org_unit_groups": ("oug1",),
        "periods": ("202301", "202302", "202303"),
        "dates": (date(2021, 12, 1), date(2022, 2, 1)),
    }
