import pytest
from datetime import datetime

from openhexa.toolbox.dhis2.periods import Day, Week, Month, Year, Quarter, SixMonth, period_from_string


def test_day():
    d1 = Day("20220101")
    d2 = Day(datetime(2022, 1, 1))
    assert d1 == d2

    with pytest.raises(ValueError):
        Day("2022-01-01")


def test_day_range():
    d1 = Day("20220130")
    d2 = Day("20220202")
    drange = d1.get_range(d2)
    assert drange == [Day("20220130"), Day("20220131"), Day("20220201"), Day("20220202")]

    d2 = Day("20220120")
    with pytest.raises(ValueError):
        d1.get_range(d2)


def test_week():
    w1 = Week("2022W4")
    w2 = Week(datetime(2022, 1, 25))
    assert w1 == w2

    with pytest.raises(ValueError):
        Week("2022W04")


def test_week_range():
    w1 = Week("2022W4")
    w2 = Week("2022W6")
    wrange = w1.get_range(w2)
    assert wrange == [Week("2022W4"), Week("2022W5"), Week("2022W6")]


def test_month():
    m1 = Month("202201")
    m2 = Month(datetime(2022, 1, 1))
    assert m1 == m2

    with pytest.raises(ValueError):
        Month("2022-01")


def test_month_range():
    m1 = Month("202201")
    m2 = Month("202204")
    mrange = m1.get_range(m2)
    assert mrange == [Month("202201"), Month("202202"), Month("202203"), Month("202204")]


def test_year():
    y1 = Year("2022")
    y2 = Year(datetime(2022, 1, 1))
    assert y1 == y2


def test_year_range():
    y1 = Year("2022")
    y2 = Year("2025")
    yrange = y1.get_range(y2)
    assert yrange == [Year("2022"), Year("2023"), Year("2024"), Year("2025")]


def test_quarter():
    q1 = Quarter("2021Q4")
    q2 = Quarter(datetime(2021, 12, 1))
    assert q1 == q2


def test_quarter_range():
    q1 = Quarter("2021Q4")
    q2 = Quarter("2022Q2")
    qrange = q1.get_range(q2)
    assert qrange == [Quarter("2021Q4"), Quarter("2022Q1"), Quarter("2022Q2")]


def test_six_month():
    sm1 = SixMonth("2022S2")
    sm2 = SixMonth(datetime(2022, 12, 1))
    assert sm1 == sm2


def test_six_month_range():
    sm1 = SixMonth("2022S2")
    sm2 = SixMonth("2023S2")
    smrange = sm1.get_range(sm2)
    assert smrange == [SixMonth("2022S2"), SixMonth("2023S1"), SixMonth("2023S2")]


def test_period_from_string():
    p1 = period_from_string("20220101")
    p2 = Day("20220101")
    assert p1 == p2

    p1 = period_from_string("202201")
    p2 = Month("202201")
    assert p1 == p2

    p1 = period_from_string("2022W1")
    p2 = Week("2022W1")
    assert p1 == p2

    p1 = period_from_string("2022")
    p2 = Year("2022")
    assert p1 == p2

    p1 = period_from_string("2022Q1")
    p2 = Quarter("2022Q1")
    assert p1 == p2

    p1 = period_from_string("2022S1")
    p2 = SixMonth("2022S1")
    assert p1 == p2
