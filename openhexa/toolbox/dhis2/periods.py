from __future__ import annotations

import calendar
import datetime
import logging
import re
from dataclasses import dataclass
from typing import Generator, Self
from warnings import warn

from dateutil import relativedelta

from openhexa.toolbox.dhis2.api import DHIS2ToolboxError

logger = logging.getLogger(__name__)


class InvalidPeriodError(DHIS2ToolboxError):
    """Exception raised for invalid period formats."""

    pass


class MixedPeriodsError(DHIS2ToolboxError):
    """Exception raised for mixed period types."""

    pass


class Period:
    """Base class for all period types."""

    def range(self, other: Self) -> Generator[Self]:
        """Generate all periods between self and other, inclusive."""
        if not isinstance(other, type(self)):
            msg = "Range can only be generated between two Period objects of the same type."
            raise InvalidPeriodError(msg)

        start_date = self.start
        end_date = other.start

        if start_date >= end_date:
            raise InvalidPeriodError("Start date must be before end date.")

        current_date = start_date
        while current_date <= end_date:
            yield type(self).from_date(current_date)
            current_date += self.delta

    def get_range(self, other: Self) -> list[Self]:
        """Get a range of periods between self and other, inclusive.

        DEPRECATED: Use the range method instead.
        """
        warn(
            "Period.get_range is deprecated. Use Period.range instead.",
            DeprecationWarning,
        )
        return list(self.range(other))

    @property
    def _delta(self) -> datetime.timedelta | relativedelta.relativedelta:
        """Return the delta for the period.

        DEPRECATED: Use Period.delta instead.
        """
        warn(
            "Period._delta is deprecated. Use Period.delta instead.",
            DeprecationWarning,
        )
        return self.delta

    @property
    def datetime(self) -> datetime.datetime | datetime.date:
        """Return the datetime representation of the period.

        DEPRECATED: Use Period.start or Period.end instead.
        """
        warn(
            "Period.datetime is deprecated. Use Period.start or Period.end instead.",
            DeprecationWarning,
        )
        return self.start

    @property
    def period(self) -> str:
        """Return the string representation of the period.

        DEPRECATED: Use str(Period) instead.
        """
        warn(
            "Period.period is deprecated. Use str(Period) instead.",
            DeprecationWarning,
        )
        return str(self)


@dataclass
class Day(Period):
    """Represents a single day period.

    Format: yyyyMMdd
    Example: 20040315
    Description: March 15, 2004

    Attributes
    ----------
    year : int
        The year of the period.
    month : int
        The month of the period.
    day : int
        The day of the period.
    """

    year: int
    month: int
    day: int
    delta: datetime.timedelta = datetime.timedelta(days=1)

    def __str__(self):
        return f"{self.year:04}{self.month:02}{self.day:02}"

    @property
    def start(self) -> datetime.date:
        return datetime.date(self.year, self.month, self.day)

    @property
    def end(self) -> datetime.date:
        return datetime.date(self.year, self.month, self.day)

    @classmethod
    def from_date(cls, dt: datetime.datetime | datetime.date):
        return cls(dt.year, dt.month, dt.day)

    @classmethod
    def from_string(cls, date_str: str):
        if not re.match(_PERIODS_REGEX[Day], date_str):
            raise InvalidPeriodError(f"Invalid period string: {date_str}")
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:])
        return cls(year, month, day)


@dataclass
class Week(Period):
    """Represents a week period.

    Format: yyyyWn
    Example: 2004W10
    Description: Week 10 2004

    Attributes
    ----------
    year : int
        The year of the period.
    week : int
        The week of the period.
    """

    year: int
    week: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(weeks=1)

    def __str__(self):
        return f"{self.year:04}W{self.week}"

    def __post_init__(self):
        if self.week < 1 or self.week > 53:
            msg = f"Invalid week number: {self.week}"
            raise InvalidPeriodError(msg)

        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        return cls(year=date.isocalendar()[0], week=date.isocalendar()[1])

    @classmethod
    def from_string(cls, week_str: str) -> Self:
        if not re.match(_PERIODS_REGEX[Week], week_str):
            raise InvalidPeriodError(f"Invalid period string: {week_str}")
        year, week = map(int, week_str.split("W"))
        return cls(year, week)

    @property
    def start(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 1)

    @property
    def end(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 7)


class WeekWednesday(Week):
    """Represents a week period starting on Wednesday.

    Format: yyyyWedWn
    Example: 2015WedW5
    Description: Week 5 with start Wednesday

    Attributes
    ----------
    year : int
        The year of the period.
    week : int
        The week of the period.
    """

    def __str__(self):
        return f"{self.year:04}WedW{self.week}"

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        adjusted_date = date - datetime.timedelta(days=2)
        return cls(year=adjusted_date.isocalendar()[0], week=adjusted_date.isocalendar()[1])

    @classmethod
    def from_string(cls, week_str: str) -> Self:
        if not re.match(_PERIODS_REGEX[WeekWednesday], week_str):
            raise InvalidPeriodError(f"Invalid period string: {week_str}")
        year, week = map(int, week_str.split("WedW"))
        return cls(year, week)

    @property
    def start(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 1) + datetime.timedelta(days=2)

    @property
    def end(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 7) + datetime.timedelta(days=2)


class WeekThursday(Week):
    """Represents a week period starting on Thursday.

    Format: yyyyThuWn
    Example: 2015ThuW6
    Description: Week 6 with start Thursday

    Attributes
    ----------
    year : int
        The year of the period.
    week : int
        The week of the period.
    """

    def __str__(self):
        return f"{self.year:04}ThuW{self.week}"

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        adjusted_date = date - datetime.timedelta(days=3)
        return cls(year=adjusted_date.isocalendar()[0], week=adjusted_date.isocalendar()[1])

    @classmethod
    def from_string(cls, week_str: str) -> Self:
        if not re.match(_PERIODS_REGEX[WeekThursday], week_str):
            raise InvalidPeriodError(f"Invalid period string: {week_str}")
        year, week = map(int, week_str.split("ThuW"))
        return cls(year, week)

    @property
    def start(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 1) + datetime.timedelta(days=3)

    @property
    def end(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 7) + datetime.timedelta(days=3)


class WeekSaturday(Week):
    """Represents a week period starting on Saturday.

    Format: yyyySatWn
    Example: 2015SatW7
    Description: Week 7 with start Saturday

    Attributes
    ----------
    year : int
        The year of the period.
    week : int
        The week of the period.
    """

    def __str__(self):
        return f"{self.year:04}SatW{self.week}"

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        adjusted_date = date + datetime.timedelta(days=2)
        return cls(year=adjusted_date.isocalendar()[0], week=adjusted_date.isocalendar()[1])

    @classmethod
    def from_string(cls, week_str: str) -> Self:
        if not re.match(_PERIODS_REGEX[WeekSaturday], week_str):
            raise InvalidPeriodError(f"Invalid period string: {week_str}")
        year, week = map(int, week_str.split("SatW"))
        return cls(year, week)

    @property
    def start(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 1) - datetime.timedelta(days=2)

    @property
    def end(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 7) - datetime.timedelta(days=2)


class WeekSunday(Week):
    """Represents a week period starting on Sunday.

    Format: yyyySunWn
    Example: 2015SunW8
    Description: Week 8 with start Sunday

    Attributes
    ----------
    year : int
        The year of the period.
    week : int
        The week of the period.
    """

    def __str__(self):
        return f"{self.year:04}SunW{self.week}"

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        adjusted_date = date + datetime.timedelta(days=1)
        return cls(year=adjusted_date.isocalendar()[0], week=adjusted_date.isocalendar()[1])

    @classmethod
    def from_string(cls, week_str: str) -> Self:
        if not re.match(_PERIODS_REGEX[WeekSunday], week_str):
            raise InvalidPeriodError(f"Invalid period string: {week_str}")
        year, week = map(int, week_str.split("SunW"))
        return cls(year, week)

    @property
    def start(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 1) - datetime.timedelta(days=1)

    @property
    def end(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 7) - datetime.timedelta(days=1)


@dataclass
class BiWeek(Period):
    """Represents a bi-week period.

    Format: yyyyBiWn
    Example: 2015BiW1
    Description: Week 1-2 2015

    Attributes
    ----------
    year : int
        The year of the period.
    bi_week : int
        The bi-week of the period.
    """

    year: int
    bi_week: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(weeks=2)

    def __str__(self):
        return f"{self.year:04}BiW{self.bi_week}"

    def __post_init__(self):
        if self.bi_week < 1 or self.bi_week > 27:
            msg = f"Invalid biweek number: {self.bi_week}"
            raise InvalidPeriodError(msg)

        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        week = Week.from_date(date)
        bi_week = (week.week - 1) // 2 + 1
        return cls(year=week.year, bi_week=bi_week)

    @classmethod
    def from_string(cls, bi_week_str: str):
        if not re.match(_PERIODS_REGEX[BiWeek], bi_week_str):
            raise InvalidPeriodError(f"Invalid period string: {bi_week_str}")
        year, bi_week = map(int, bi_week_str.split("BiW"))
        return cls(year, bi_week)

    @property
    def start(self) -> datetime.date:
        week = (self.bi_week - 1) * 2 + 1
        return datetime.date.fromisocalendar(self.year, week, 1)

    @property
    def end(self) -> datetime.date:
        week = self.bi_week * 2
        return datetime.date.fromisocalendar(self.year, week, 7)


@dataclass
class Month(Period):
    """Represents a month period.

    Format: yyyyMM
    Example: 200403
    Description: March 2004

    Attributes
    ----------
    year : int
        The year of the period.
    month : int
        The month of the period.
    """

    year: int
    month: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(months=1)

    def __str__(self):
        return f"{self.year:04}{self.month:02}"

    def __post_init__(self):
        if self.month < 1 or self.month > 12:
            msg = f"Invalid month: {self.month}"
            raise InvalidPeriodError(msg)

        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        return cls(year=date.year, month=date.month)

    @classmethod
    def from_string(cls, month_str: str):
        if not re.match(_PERIODS_REGEX[Month], month_str):
            raise InvalidPeriodError(f"Invalid period string: {month_str}")
        year = int(month_str[:4])
        month = int(month_str[4:])
        return cls(year, month)

    @property
    def start(self) -> datetime.date:
        return datetime.date(self.year, self.month, 1)

    @property
    def end(self) -> datetime.date:
        last_day_of_month = calendar.monthrange(self.year, self.month)[1]
        return datetime.date(self.year, self.month, last_day_of_month)


@dataclass
class BiMonth(Period):
    """Represents a bi-month period.

    Format: yyyyMMB
    Example: 200401B
    Description: January-February 2004

    Attributes
    ----------
    year : int
        The year of the period.
    bi_month : int
        The bi-month of the period.
    """

    year: int
    bi_month: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(months=2)

    def __str__(self):
        return f"{self.year:04}{self.bi_month:02}B"

    def __post_init__(self):
        if self.bi_month < 1 or self.bi_month % 2 == 0:
            msg = f"Invalid bi-month: {self.bi_month}"
            raise InvalidPeriodError(msg)

        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        month = Month.from_date(date)
        bi_month = (month.month - 1) // 2 * 2 + 1
        return cls(year=month.year, bi_month=bi_month)

    @classmethod
    def from_string(cls, bi_month_str: str):
        if not re.match(_PERIODS_REGEX[BiMonth], bi_month_str):
            raise InvalidPeriodError(f"Invalid period string: {bi_month_str}")
        year = int(bi_month_str[:4])
        bi_month = int(bi_month_str[4:6])
        return cls(year, bi_month)

    @property
    def start(self) -> datetime.date:
        return datetime.date(self.year, self.bi_month, 1)

    @property
    def end(self) -> datetime.date:
        last_day_of_month = calendar.monthrange(self.year, self.bi_month + 1)[1]
        return datetime.date(self.year, self.bi_month + 1, last_day_of_month)


@dataclass
class Quarter(Period):
    """Represents a quarter period.

    Format: yyyyQn
    Example: 2004Q1
    Description: January-March 2004

    Attributes
    ----------
    year : int
        The year of the period.
    quarter : int
        The quarter of the period.
    """

    year: int
    quarter: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(months=3)

    def __str__(self):
        return f"{self.year:04}Q{self.quarter}"

    def __post_init__(self):
        if not (1 <= self.quarter <= 5):
            msg = f"Invalid quarter: {self.quarter}"
            raise InvalidPeriodError(msg)

        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        month = Month.from_date(date)
        quarter = (month.month - 1) // 3 + 1
        return cls(year=month.year, quarter=quarter)

    @classmethod
    def from_string(cls, quarter_str: str):
        if not re.match(_PERIODS_REGEX[Quarter], quarter_str):
            raise InvalidPeriodError(f"Invalid period string: {quarter_str}")
        year, quarter = map(int, quarter_str.split("Q"))
        return cls(year, quarter)

    @property
    def start(self) -> datetime.date:
        month = (self.quarter - 1) * 3 + 1
        return datetime.date(self.year, month, 1)

    @property
    def end(self) -> datetime.date:
        month = self.quarter * 3
        last_day_of_month = calendar.monthrange(self.year, month)[1]
        return datetime.date(self.year, month, last_day_of_month)


@dataclass
class SixMonth(Period):
    """Represents a six-month period.

    Format: yyyySn
    Example: 2004S1
    Description: January-June 2004

    Attributes
    ----------
    year : int
        The year of the period.
    six_month : int
        The six-month of the period.
    """

    year: int
    six_month: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(months=6)

    def __str__(self):
        return f"{self.year:04}S{self.six_month}"

    def __post_init__(self):
        if self.six_month not in {1, 2}:
            msg = f"Invalid six-month: {self.six_month}"
            raise InvalidPeriodError(msg)

        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        six_month = 1 if date.month <= 6 else 2
        return cls(year=date.year, six_month=six_month)

    @classmethod
    def from_string(cls, six_month_str: str):
        if not re.match(_PERIODS_REGEX[SixMonth], six_month_str):
            raise InvalidPeriodError(f"Invalid period string: {six_month_str}")
        year, six_month = map(int, six_month_str.split("S"))
        return cls(year, six_month)

    @property
    def start(self) -> datetime.date:
        month = (self.six_month - 1) * 6 + 1
        return datetime.date(self.year, month, 1)

    @property
    def end(self) -> datetime.date:
        month = self.six_month * 6
        last_day_of_month = calendar.monthrange(self.year, month)[1]
        return datetime.date(self.year, month, last_day_of_month)


@dataclass
class Year(Period):
    """Represents a year period.

    Format: yyyy
    Example: 2004
    Description: 2004

    Attributes
    ----------
    year : int
        The year of the period.
    """

    year: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(years=1)

    def __str__(self):
        return f"{self.year:04}"

    def __post_init__(self):
        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        return cls(year=date.year)

    @classmethod
    def from_string(cls, year_str: str):
        if not re.match(_PERIODS_REGEX[Year], year_str):
            raise InvalidPeriodError(f"Invalid period string: {year_str}")
        year = int(year_str)
        return cls(year)

    @property
    def start(self) -> datetime.date:
        return datetime.date(self.year, 1, 1)

    @property
    def end(self) -> datetime.date:
        return datetime.date(self.year, 12, 31)


@dataclass
class FinancialApril(Period):
    """Represents a financial year period starting in April.

    Format: yyyyApril
    Example: 2004April
    Description: April 2004-March 2005

    Attributes
    ----------
    year : int
        The year of the period.
    """

    year: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(years=1)

    def __str__(self):
        return f"{self.year:04}April"

    def __post_init__(self):
        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        return cls(year=date.year)

    @classmethod
    def from_string(cls, year_str: str):
        if not re.match(_PERIODS_REGEX[FinancialApril], year_str):
            raise InvalidPeriodError(f"Invalid period string: {year_str}")
        year = int(year_str[:4])
        return cls(year)

    @property
    def start(self) -> datetime.date:
        return datetime.date(self.year, 4, 1)

    @property
    def end(self) -> datetime.date:
        return datetime.date(self.year + 1, 3, 31)


@dataclass
class FinancialJuly(Period):
    """Represents a financial year period starting in July.

    Format: yyyyJuly
    Example: 2004July
    Description: July 2004-June 2005

    Attributes
    ----------
    year : int
        The year of the period.
    """

    year: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(years=1)

    def __str__(self):
        return f"{self.year:04}July"

    def __post_init__(self):
        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        return cls(year=date.year)

    @classmethod
    def from_string(cls, year_str: str):
        if not re.match(_PERIODS_REGEX[FinancialJuly], year_str):
            raise InvalidPeriodError(f"Invalid period string: {year_str}")
        year = int(year_str[:4])
        return cls(year)

    @property
    def start(self) -> datetime.date:
        return datetime.date(self.year, 7, 1)

    @property
    def end(self) -> datetime.date:
        return datetime.date(self.year + 1, 6, 30)


@dataclass
class FinancialOct(Period):
    """Represents a financial year period starting in October.

    Format: yyyyOct
    Example: 2004Oct
    Description: October 2004-September 2005

    Attributes
    ----------
    year : int
        The year of the period.
    """

    year: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(years=1)

    def __str__(self):
        return f"{self.year:04}Oct"

    def __post_init__(self):
        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        return cls(year=date.year)

    @classmethod
    def from_string(cls, year_str: str):
        if not re.match(_PERIODS_REGEX[FinancialOct], year_str):
            raise InvalidPeriodError(f"Invalid period string: {year_str}")
        year = int(year_str[:4])
        return cls(year)

    @property
    def start(self) -> datetime.date:
        return datetime.date(self.year, 10, 1)

    @property
    def end(self) -> datetime.date:
        return datetime.date(self.year + 1, 9, 30)


@dataclass
class FinancialNov(Period):
    """Represents a financial year period starting in November.

    Format: yyyyNov
    Example: 2004Nov
    Description: November 2004-October 2005

    Attributes
    ----------
    year : int
        The year of the period.
    """

    year: int
    delta: relativedelta.relativedelta = relativedelta.relativedelta(years=1)

    def __str__(self):
        return f"{self.year:04}Nov"

    def __post_init__(self):
        if self.year < 1900:
            msg = f"Invalid year: {self.year}"
            raise InvalidPeriodError(msg)

    @classmethod
    def from_date(cls, date: datetime.date | datetime.datetime):
        return cls(year=date.year)

    @classmethod
    def from_string(cls, year_str: str):
        if not re.match(_PERIODS_REGEX[FinancialNov], year_str):
            raise InvalidPeriodError(f"Invalid period string: {year_str}")
        year = int(year_str[:4])
        return cls(year)

    @property
    def start(self) -> datetime.date:
        return datetime.date(self.year, 11, 1)

    @property
    def end(self) -> datetime.date:
        return datetime.date(self.year + 1, 10, 31)


_PERIODS_REGEX = {
    Day: r"^\d{4}\d{2}\d{2}$",
    Week: r"^\d{4}W([1-9]\d*)$",
    Month: r"^\d{4}\d{2}$",
    BiMonth: r"^\d{4}\d{2}B$",
    Quarter: r"^\d{4}Q\d$",
    SixMonth: r"^\d{4}S\d$",
    Year: r"^\d{4}$",
    FinancialApril: r"^\d{4}April$",
    FinancialJuly: r"^\d{4}July$",
    FinancialOct: r"^\d{4}Oct$",
    FinancialNov: r"^\d{4}Nov$",
    WeekWednesday: r"^\d{4}WedW([1-9]\d*)$",
    WeekThursday: r"^\d{4}ThuW([1-9]\d*)$",
    WeekSaturday: r"^\d{4}SatW([1-9]\d*)$",
    WeekSunday: r"^\d{4}SunW([1-9]\d*)$",
    BiWeek: r"^\d{4}BiW([1-9]\d*)$",
}


def period_from_string(period_str: str) -> Period:
    """Convert a string to a Period object.

    Parameters
    ----------
    period_str : str
        The period string to convert.

    Returns
    -------
    Period
        The corresponding Period object.

    Raises
    ------
    InvalidPeriodError
        If the period string is not valid.
    """
    for period_type, regex in _PERIODS_REGEX.items():
        if re.match(regex, period_str):
            return period_type.from_string(period_str)

    raise InvalidPeriodError(f"Invalid period string: {period_str}")


def get_range(start: Period, end: Period) -> list[Period]:
    """Get a range of periods between start and end, inclusive.

    DEPRECATED: Use Period.range instead.
    """
    warn(
        "get_range is deprecated. Use Period.range instead.",
        DeprecationWarning,
    )
    if not isinstance(start, type(end)):
        raise MixedPeriodsError("Start and end periods must be of the same type.")

    return list(start.range(end))
