import math
from datetime import datetime


class Period:
    def __init__(self, dt: datetime):
        self.dt = dt

    def __repr__(self):
        return self.__str__()


class Day(Period):
    def __str__(self):
        return self.dt.strftime("%Y%m%d")


class Week(Period):
    def __str__(self):
        return self.dt.strftime("%YW%-W")


class Month(Period):
    def __str__(self):
        return self.dt.strftime("%Y%m")


class Quarter(Period):
    def __str__(self):
        return f"{self.dt.strftime('%Y')}Q{math.ceil(self.dt.month/3)}"


class SixMonth(Period):
    def __str__(self):
        return f"{self.dt.strftime('%Y')}S{math.ceil(self.dt.month/6)}"


class Year(Period):
    def __str__(self):
        return self.dt.strftime("%Y")
