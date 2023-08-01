import math
from datetime import datetime
from typing import Union

from dateutil.relativedelta import relativedelta


class Period:
    def __init__(self, period: Union[str, datetime]):
        if isinstance(period, str):
            self.check_period(period)
            self.period = period
            self.datetime = self.to_datetime(period)
        elif isinstance(period, datetime):
            self.datetime = period
            self.period = self.to_string(period)
        else:
            raise ValueError("Period must be str or datetime")

    def get_range(self, end) -> list:
        """Get a range of DHIS2 periods."""
        if type(self) != type(end):
            raise ValueError("Start and end periods must be of same type")

        if end.datetime <= self.datetime:
            raise ValueError("End period must be inferior to start period")

        prange = []
        dt = self.datetime
        while dt <= end.datetime:
            prange.append(type(self)(dt))
            dt += self._delta

        return prange

    def __str__(self):
        return self.period

    def __eq__(self, other):
        return self.period == other.period

    def __ne__(self, other):
        return self.period != other.period

    def __gt__(self, other):
        return self.period > other.period

    def __lt__(self, other):
        return self.period < other.period

    def __ge__(self, other):
        return self.period >= other.period

    def __le__(self, other):
        return self.period <= other.period

    def __repr__(self):
        return f'"{self.period}"'


class Day(Period):
    """Day.

    Format: yyyyMMdd, example: 20040315
    """

    def __init__(self, period: Union[str, datetime]):
        super().__init__(period)
        self._delta = relativedelta(days=1)

    @staticmethod
    def check_period(period: str):
        if len(period) != 8:
            raise ValueError(f'"{period}" is not valid DHIS2 day')

    @staticmethod
    def to_datetime(period: str) -> datetime:
        return datetime.strptime(period, "%Y%m%d")

    @staticmethod
    def to_string(dt: str) -> str:
        return dt.strftime("%Y%m%d")


class Week(Period):
    """Week.

    Format: yyyyWn, example: 2004W10
    """

    def __init__(self, period: Union[str, datetime]):
        super().__init__(period)
        self._delta = relativedelta(weeks=1)

    @staticmethod
    def check_period(period: str):
        if (len(period) != 6 and len(period) != 7) or period[4] != "W" or period[5] == "0":
            raise ValueError(f'"{period}" is not valid DHIS2 week')

    @staticmethod
    def to_datetime(period: str) -> datetime:
        # a dummy weekday is added so that strptime can be used
        return datetime.strptime(period + "1", "%YW%W%w")

    @staticmethod
    def to_string(dt: datetime) -> str:
        return dt.strftime("%YW%-W")


class Month(Period):
    """Month.

    Format: yyyyMM, example: 200403
    """

    def __init__(self, period: Union[str, datetime]):
        super().__init__(period)
        self._delta = relativedelta(months=1)

    @staticmethod
    def check_period(period: str):
        if len(period) != 6:
            raise ValueError(f'"{period}" is not valid DHIS2 month')

    @staticmethod
    def to_datetime(period: str) -> datetime:
        return datetime.strptime(period, "%Y%m")

    @staticmethod
    def to_string(dt: datetime) -> str:
        return dt.strftime("%Y%m")


class Year(Period):
    """Year.

    Format: yyyy, example: 2004
    """

    def __init__(self, period: Union[str, datetime]):
        super().__init__(period)
        self._delta = relativedelta(years=1)

    @staticmethod
    def check_period(period: str):
        if len(period) != 4:
            raise ValueError(f'"{period}" is not valid DHIS2 year')

    @staticmethod
    def to_datetime(period: str) -> datetime:
        return datetime.strptime(period, "%Y")

    @staticmethod
    def to_string(dt: datetime) -> str:
        return dt.strftime("%Y")


class Quarter(Period):
    """Quarter.

    Format: yyyyQn, example: 2004Q1
    """

    def __init__(self, period: Union[str, datetime]):
        super().__init__(period)
        self._delta = relativedelta(months=3)

    @staticmethod
    def check_period(period: str):
        if len(period) != 6 and period[4] != "Q":
            raise ValueError(f'"{period}" is not valid DHIS2 quarter')

    @staticmethod
    def to_datetime(period: str) -> datetime:
        y = int(period[0:4])  # year
        q = int(period[5])  # quarter
        return datetime(y, 1 + 3 * (q - 1), 1)

    @staticmethod
    def to_string(dt: datetime) -> str:
        return f"{dt.strftime('%Y')}Q{math.ceil(dt.month/3)}"


class SixMonth(Period):
    """Six-month period.

    Format: yyyySn, example: 2004S1
    """

    def __init__(self, period: Union[str, datetime]):
        super().__init__(period)
        self._delta = relativedelta(months=6)

    @staticmethod
    def check_period(period: str):
        if len(period) != 6 and period[4] != "Q":
            raise ValueError(f'"{period}" is not valid DHIS2 six-month period')

    @staticmethod
    def to_datetime(period: str) -> datetime:
        y = int(period[0:4])  # year
        s = int(period[5])  # semester
        return datetime(y, 1 + 6 * (s - 1), 1)

    @staticmethod
    def to_string(dt: datetime) -> str:
        return f"{dt.strftime('%Y')}S{math.ceil(dt.month/6)}"


def period_from_string(period: str) -> Period:
    """Get a DHIS2 period object from a period string."""
    if len(period) == 4 and period.isnumeric():
        return Year(period)
    elif period[4] == "W":
        return Week(period)
    elif period[4] == "Q":
        return Quarter(period)
    elif period[4] == "S":
        return SixMonth(period)
    elif len(period) == 6 and period.isnumeric():
        return Month(period)
    elif len(period) == 8 and period.isnumeric():
        return Day(period)
    else:
        raise ValueError("Unrecognized period format")
