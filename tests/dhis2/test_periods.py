from openhexa.toolbox.dhis2.periods import get_range


def test_periods_range():
    start = "201904"
    end = "202002"
    periods = get_range(start, end)
    assert len(periods) == 11
