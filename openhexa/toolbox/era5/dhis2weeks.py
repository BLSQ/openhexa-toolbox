"""A set of functions to convert dates to all types of DHIS2 periods."""

from datetime import date, timedelta
from enum import StrEnum


class WeekType(StrEnum):
    """DHIS2 weekly period types."""

    WEEK = "WEEK"
    WEEK_WEDNESDAY = "WEEK_WEDNESDAY"
    WEEK_THURSDAY = "WEEK_THURSDAY"
    WEEK_SATURDAY = "WEEK_SATURDAY"
    WEEK_SUNDAY = "WEEK_SUNDAY"


start_days = {
    WeekType.WEEK_WEDNESDAY: 3,
    WeekType.WEEK_THURSDAY: 4,
    WeekType.WEEK_SATURDAY: 6,
    WeekType.WEEK_SUNDAY: 7,
}


def get_calendar_week(dt: date, week_type: WeekType) -> tuple[int, int]:
    """Get week number and year for a given date and week type.

    Args:
        dt: The date to convert.
        week_type: The type of week period. One of 'WEEK', 'WEEK_WEDNESDAY',
            'WEEK_THURSDAY', 'WEEK_SATURDAY', 'WEEK_SUNDAY'.

    Returns:
        A tuple (year, week number).

    """
    # We can use the ISO calendar for standard Monday weeks
    if week_type == WeekType.WEEK:
        iso_year, iso_week, _ = dt.isocalendar()
        return (iso_year, iso_week)

    # 1st week of the year always contain Jan 4th
    week_start = adjust_to_week_start(dt, start_days[week_type])
    jan4 = date(week_start.year, 1, 4)
    first_week_start = adjust_to_week_start(jan4, start_days[week_type])

    # Week start is before the 1st week of the year, so it belongs to the last week of
    # the previous year
    if week_start < first_week_start:
        jan4_prev = date(week_start.year - 1, 1, 4)
        first_week_start_prev = adjust_to_week_start(jan4_prev, start_days[week_type])
        weeks_from_start = (week_start - first_week_start_prev).days // 7
        return week_start.year - 1, weeks_from_start + 1

    # If we are in late December, we might belong to next year's first week
    if week_start.month == 12:
        week_end = week_start + timedelta(days=6)
        if week_end.month == 1:
            jan4_next = date(week_start.year + 1, 1, 4)
            if week_start <= jan4_next <= week_end:
                return week_start.year + 1, 1

    # Happy path: we are in the current year's weeks
    weeks_from_start = (week_start - first_week_start).days // 7
    return week_start.year, weeks_from_start + 1


def to_dhis2_week(dt: date, week_type: WeekType) -> str:
    """Convert a date to a DHIS2 period string.

    Args:
        dt: The date to convert.
        week_type: The type of week period. One of 'WEEK', 'WEEK_WEDNESDAY',
            'WEEK_THURSDAY', 'WEEK_SATURDAY', 'WEEK_SUNDAY'.

    Returns:
        The DHIS2 period string.

    """
    year, week = get_calendar_week(dt, week_type)

    prefix = {
        WeekType.WEEK: "W",
        WeekType.WEEK_WEDNESDAY: "WedW",
        WeekType.WEEK_THURSDAY: "ThuW",
        WeekType.WEEK_SATURDAY: "SatW",
        WeekType.WEEK_SUNDAY: "SunW",
    }[week_type]

    return f"{year}{prefix}{week}"


def adjust_to_week_start(dt: date, start_day: int) -> date:
    """Adjust date to the start of the week.

    Args:
        dt: The date to adjust.
        start_day: The day of the week the week starts on (1=Monday, 7=Sunday).

    Returns:
        The adjusted date.

    """
    days_to_adjust = (dt.weekday() - (start_day - 1)) % 7
    return dt - timedelta(days=days_to_adjust)
