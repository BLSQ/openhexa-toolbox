"""Test dhis2weeks module."""

from datetime import date

from openhexa.toolbox.era5.dhis2weeks import WeekType, get_calendar_week


def test_standard_iso_week():
    # 2024 Jan 1 is a Monday, so we expect it to be week 1 of 2024
    dt = date(2024, 1, 1)
    assert get_calendar_week(dt, WeekType.WEEK) == (2024, 1)

    # 2023 Dec 31 is a Sunday, so it belongs to week 52 of 2023
    dt = date(2023, 12, 31)
    assert get_calendar_week(dt, WeekType.WEEK) == (2023, 52)


def test_sunday_week_year_boundary():
    """Test Sunday weeks crossing year boundaries."""
    # 2023 Dec 31 is a Sunday starting a week containing Jan 4th, so it should belong
    # to week 1 of 2024 for Sunday weeks
    dt = date(2023, 12, 31)
    assert get_calendar_week(dt, WeekType.WEEK_SUNDAY) == (2024, 1)

    # Next Sunday should be in Week 2
    dt = date(2024, 1, 7)
    assert get_calendar_week(dt, WeekType.WEEK_SUNDAY) == (2024, 2)


def test_saturday_week_year_start():
    """Test Saturday weeks when year starts on Saturday."""
    # Jan 1 2022 is a Saturday so it should be week 1 of 2022 for Saturday weeks
    # However, for Sunday weeks it should belong to the last week of 2021
    dt = date(2022, 1, 1)
    assert get_calendar_week(dt, WeekType.WEEK_SATURDAY) == (2022, 1)
    assert get_calendar_week(dt, WeekType.WEEK_SUNDAY) == (2021, 52)


def test_different_week_types_same_date():
    """Test that the same date can belong to different year/weeks."""
    # 2022 Jan 1 is a Saturday and is expected to be:
    #   - Week 52 of 2021 for standard ISO weeks (Monday start)
    #   - Week 1 of 2022 for Wednesday weeks
    #   - Week 1 of 2022 for Thursday weeks
    #   - Week 1 of 2022 for Saturday weeks
    #   - Week 52 of 2021 for Sunday weeks
    dt = date(2022, 1, 1)
    assert get_calendar_week(dt, WeekType.WEEK) == (2021, 52)
    assert get_calendar_week(dt, WeekType.WEEK_WEDNESDAY) == (2022, 1)
    assert get_calendar_week(dt, WeekType.WEEK_THURSDAY) == (2022, 1)
    assert get_calendar_week(dt, WeekType.WEEK_SATURDAY) == (2022, 1)
    assert get_calendar_week(dt, WeekType.WEEK_SUNDAY) == (2021, 52)
