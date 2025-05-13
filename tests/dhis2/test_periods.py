from datetime import date

import pytest

from openhexa.toolbox.dhis2.periods import (
    BiMonth,
    BiWeek,
    Day,
    FinancialApril,
    FinancialJuly,
    FinancialNov,
    FinancialOct,
    InvalidPeriodError,
    Month,
    Quarter,
    SixMonth,
    Week,
    WeekSaturday,
    WeekSunday,
    WeekThursday,
    WeekWednesday,
    Year,
    period_from_string,
)


def test_period_from_string_valid():
    assert isinstance(period_from_string("20230101"), Day)
    assert isinstance(period_from_string("2023W1"), Week)
    assert isinstance(period_from_string("202301"), Month)
    assert isinstance(period_from_string("202301B"), BiMonth)
    assert isinstance(period_from_string("2023Q1"), Quarter)
    assert isinstance(period_from_string("2023S1"), SixMonth)
    assert isinstance(period_from_string("2023"), Year)
    assert isinstance(period_from_string("2023April"), FinancialApril)
    assert isinstance(period_from_string("2023July"), FinancialJuly)
    assert isinstance(period_from_string("2023Oct"), FinancialOct)
    assert isinstance(period_from_string("2023Nov"), FinancialNov)
    assert isinstance(period_from_string("2023WedW1"), WeekWednesday)
    assert isinstance(period_from_string("2023ThuW1"), WeekThursday)
    assert isinstance(period_from_string("2023SatW1"), WeekSaturday)
    assert isinstance(period_from_string("2023SunW1"), WeekSunday)
    assert isinstance(period_from_string("2023BiW1"), BiWeek)


def test_period_from_string_invalid():
    with pytest.raises(InvalidPeriodError):
        period_from_string("invalid_period")
    with pytest.raises(InvalidPeriodError):
        period_from_string("2023W04")
    with pytest.raises(InvalidPeriodError):
        period_from_string("2023M13")
    with pytest.raises(InvalidPeriodError):
        period_from_string("2024-02-30")


def test_day_range():
    start = Day(2022, 12, 30)
    end = Day(2023, 1, 2)
    period_range = list(start.range(end))
    assert len(period_range) == 4
    assert period_range[0] == Day(2022, 12, 30)
    assert period_range[1] == Day(2022, 12, 31)
    assert period_range[2] == Day(2023, 1, 1)
    assert period_range[3] == Day(2023, 1, 2)


def test_month_range():
    start = Month(2022, 11)
    end = Month(2023, 2)
    period_range = list(start.range(end))
    assert len(period_range) == 4
    assert period_range[0] == Month(2022, 11)
    assert period_range[1] == Month(2022, 12)
    assert period_range[2] == Month(2023, 1)
    assert period_range[3] == Month(2023, 2)


def test_year_range():
    start = Year(2020)
    end = Year(2023)  # Extended to cover more years
    period_range = list(start.range(end))
    assert len(period_range) == 4
    assert period_range[0] == Year(2020)
    assert period_range[1] == Year(2021)
    assert period_range[2] == Year(2022)
    assert period_range[3] == Year(2023)


def test_week_range():
    start = Week(2022, 51)  # Week spanning year end
    end = Week(2023, 2)
    period_range = list(start.range(end))
    # 2022W51, 2022W52, 2023W1, 2023W2
    assert len(period_range) == 4
    assert period_range[0] == Week(2022, 51)
    assert period_range[1] == Week(2022, 52)
    assert period_range[2] == Week(2023, 1)
    assert period_range[3] == Week(2023, 2)


def test_day_properties():
    day = Day(2023, 5, 13)
    assert str(day) == "20230513"
    assert day.start == date(2023, 5, 13)
    assert day.end == date(2023, 5, 13)


def test_month_properties():
    month = Month(2023, 2)  # February, non-leap year
    assert str(month) == "202302"
    assert month.start == date(2023, 2, 1)
    assert month.end == date(2023, 2, 28)

    month_leap = Month(2024, 2)  # February, leap year
    assert str(month_leap) == "202402"
    assert month_leap.start == date(2024, 2, 1)
    assert month_leap.end == date(2024, 2, 29)


def test_year_properties():
    year = Year(2023)
    assert str(year) == "2023"
    assert year.start == date(2023, 1, 1)
    assert year.end == date(2023, 12, 31)


def test_week_properties():
    # Week 52 2023 spans Dec 25, 2023 to Dec 31, 2023
    week = Week(2023, 52)
    assert str(week) == "2023W52"
    assert week.start == date(2023, 12, 25)
    assert week.end == date(2023, 12, 31)

    # Week 1 2024 starts on 2024-01-01 and ends on 2024-01-07
    week_next_year = Week(2024, 1)
    assert str(week_next_year) == "2024W1"
    assert week_next_year.start == date(2024, 1, 1)
    assert week_next_year.end == date(2024, 1, 7)


def test_financial_april_properties():
    fin_april = FinancialApril(2023)  # FY 2023-2024
    assert str(fin_april) == "2023April"
    assert fin_april.start == date(2023, 4, 1)
    assert fin_april.end == date(2024, 3, 31)


def test_day_from_string():
    day = Day.from_string("20230513")
    assert day.year == 2023
    assert day.month == 5
    assert day.day == 13


def test_week_from_string():
    week = Week.from_string("2023W10")
    assert week.year == 2023
    assert week.week == 10


def test_month_from_string():
    month = Month.from_string("202305")
    assert month.year == 2023
    assert month.month == 5


def test_year_from_string():
    year_obj = Year.from_string("2023")
    assert year_obj.year == 2023


def test_bi_month_from_string():
    bimonth = BiMonth.from_string("202301B")
    assert bimonth.year == 2023
    assert bimonth.bi_month == 1


def test_quarter_from_string():
    quarter = Quarter.from_string("2023Q2")
    assert quarter.year == 2023
    assert quarter.quarter == 2


def test_six_month_from_string():
    six_month = SixMonth.from_string("2023S1")
    assert six_month.year == 2023
    assert six_month.six_month == 1


def test_financial_april_from_string():
    fin_april = FinancialApril.from_string("2023April")
    assert fin_april.year == 2023


def test_week_wednesday_from_string():
    week_wed = WeekWednesday.from_string("2023WedW5")
    assert week_wed.year == 2023
    assert week_wed.week == 5


def test_day_from_date():
    dt = date(2023, 5, 13)
    day = Day.from_date(dt)
    assert day.year == 2023
    assert day.month == 5
    assert day.day == 13


def test_week_from_date():
    dt = date(2023, 3, 8)
    week = Week.from_date(dt)
    assert week.year == 2023
    assert week.week == 10


def test_month_from_date():
    dt = date(2023, 5, 13)
    month = Month.from_date(dt)
    assert month.year == 2023
    assert month.month == 5


def test_year_from_date():
    dt = date(2023, 5, 13)
    year_obj = Year.from_date(dt)
    assert year_obj.year == 2023


def test_biweek_properties():
    # BiWeek 26 2023 covers week 51 and 52 of 2023
    # W51: Dec 18 - Dec 24
    # W52: Dec 25 - Dec 31
    biweek = BiWeek(2023, 26)
    assert str(biweek) == "2023BiW26"
    assert biweek.start == date(2023, 12, 18)
    assert biweek.end == date(2023, 12, 31)

    # BiWeek 1 2024 covers week 1 and 2 of 2024
    # W1 2024: Jan 1 - Jan 7
    # W2 2024: Jan 8 - Jan 14
    biweek_next_year = BiWeek(2024, 1)
    assert str(biweek_next_year) == "2024BiW1"
    assert biweek_next_year.start == date(2024, 1, 1)
    assert biweek_next_year.end == date(2024, 1, 14)


def test_biweek_from_date():
    # Date in week 52 of 2023 (BiWeek 26)
    dt1 = date(2023, 12, 28)
    biweek1 = BiWeek.from_date(dt1)
    assert biweek1.year == 2023
    assert biweek1.bi_week == 26


def test_bimonth_properties():
    bimonth = BiMonth(2023, 11)  # Nov-Dec
    assert str(bimonth) == "202311B"
    assert bimonth.start == date(2023, 11, 1)
    assert bimonth.end == date(2023, 12, 31)


def test_bimonth_from_date():
    dt_jan = date(2023, 1, 15)
    bimonth_jan = BiMonth.from_date(dt_jan)
    assert bimonth_jan.year == 2023
    assert bimonth_jan.bi_month == 1

    dt_dec = date(2023, 12, 15)
    bimonth_dec = BiMonth.from_date(dt_dec)
    assert bimonth_dec.year == 2023
    assert bimonth_dec.bi_month == 11


def test_quarter_properties():
    q4 = Quarter(2023, 4)
    assert str(q4) == "2023Q4"
    assert q4.start == date(2023, 10, 1)
    assert q4.end == date(2023, 12, 31)


def test_quarter_from_date():
    dt_q4 = date(2023, 11, 15)
    q_from_dt4 = Quarter.from_date(dt_q4)
    assert q_from_dt4.year == 2023
    assert q_from_dt4.quarter == 4


def test_sixmonth_properties():
    s2 = SixMonth(2023, 2)
    assert str(s2) == "2023S2"
    assert s2.start == date(2023, 7, 1)
    assert s2.end == date(2023, 12, 31)


def test_sixmonth_from_date():
    dt_s2 = date(2023, 12, 15)
    s_from_dt2 = SixMonth.from_date(dt_s2)
    assert s_from_dt2.year == 2023
    assert s_from_dt2.six_month == 2


def test_week_wednesday_properties():
    ww = WeekWednesday(2023, 52)
    assert str(ww) == "2023WedW52"
    assert ww.start == date(2023, 12, 27)
    assert ww.end == date(2024, 1, 2)

    ww_next = WeekWednesday(2024, 1)
    assert str(ww_next) == "2024WedW1"
    assert ww_next.start == date(2024, 1, 3)
    assert ww_next.end == date(2024, 1, 9)


def test_week_wednesday_from_date():
    # wednesday dec 27, 2023
    dt = date(2023, 12, 27)
    ww = WeekWednesday.from_date(dt)
    assert ww.year == 2023
    assert ww.week == 52

    # tuesday jan 2, 2024
    dt_tue = date(2024, 1, 2)
    ww_tue = WeekWednesday.from_date(dt_tue)
    assert ww_tue.year == 2023
    assert ww_tue.week == 52

    # wednesday jan 3, 2024
    dt_wed_next = date(2024, 1, 3)
    ww_wed_next = WeekWednesday.from_date(dt_wed_next)
    assert ww_wed_next.year == 2024
    assert ww_wed_next.week == 1


def test_week_thursday_properties():
    # 2023-12-28 is a Thursday
    # 52nd thursday week of 2023 is from 12-28 to 01-03
    wt = WeekThursday(2023, 52)
    assert str(wt) == "2023ThuW52"
    assert wt.start == date(2023, 12, 28)
    assert wt.end == date(2024, 1, 3)

    # 2024-01-04 is a Thursday
    # 1st thursday week of 2024 is from 01-04 to 01-10
    wt_next = WeekThursday(2024, 1)
    assert str(wt_next) == "2024ThuW1"
    assert wt_next.start == date(2024, 1, 4)
    assert wt_next.end == date(2024, 1, 10)


def test_week_saturday_properties():
    # 2023-12-30 is a Saturday
    # 52nd saturday week of 2023 is from 12-23 to 12-29
    ws = WeekSaturday(2023, 52)
    assert str(ws) == "2023SatW52"
    assert ws.start == date(2023, 12, 23)
    assert ws.end == date(2023, 12, 29)

    # 2024-01-06 is a Saturday
    # 1st saturday week of 2024 is from 12-30 to 01-05
    ws = WeekSaturday(2024, 1)
    assert str(ws) == "2024SatW1"
    assert ws.start == date(2023, 12, 30)
    assert ws.end == date(2024, 1, 5)


def test_week_sunday_properties():
    # 2023-12-31 is a Sunday
    # 52nd sunday week of 2023 is from 12-24 to 12-30
    wsun = WeekSunday(2023, 52)
    assert str(wsun) == "2023SunW52"
    assert wsun.start == date(2023, 12, 24)
    assert wsun.end == date(2023, 12, 30)

    # 2024-01-07 is a Sunday
    # 1st sunday week of 2024 is from 12-31 to 01-06
    wsun = WeekSunday(2024, 1)
    assert str(wsun) == "2024SunW1"
    assert wsun.start == date(2023, 12, 31)
    assert wsun.end == date(2024, 1, 6)
