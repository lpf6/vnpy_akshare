import datetime as dt
from collections import Iterable

import numpy as np
from chinese_calendar import is_holiday


def to_date(d: str or int or dt.datetime or dt.date):
    if type(d) is str or type(d) is np.str:
        return dt.datetime.strptime(d, "%Y-%m-%d")

    if isinstance(d, Iterable):
        ret = []
        for dd in d:
            ret.append(to_date(dd))
        return ret

    if type(d) is int or type(d) is np.int64 or type(d) is np.int32:
        if d < 10000 * 10000:
            return dt.datetime(year=d // 10000, month=d // 100 % 100, day=d % 100)
        else:
            day = d / 10000
            return dt.datetime(year=day // 10000, month=day // 100 % 100, day=day % 100,
                               hour=d // 100 % 100, minute=d % 100)
    if isinstance(d, dt.date):
        return dt.datetime(year=d.year, month=d.month, day=d.day)
    return d


def to_delta(delta: dt.timedelta or int or str):
    if type(delta) is dt.timedelta:
        return delta
    if type(delta) is str:
        delta = int(delta)

    return dt.timedelta(delta)


def to_str(d: dt.datetime or str or int):
    d = to_date(d)
    return d.strftime("%Y-%m-%d")


def to_num(d: dt.datetime or str or int, long: bool = False):
    d = to_date(d)
    if long:
        return (d.year * 10000 + d.month * 100 + d.day) * 10000 + d.hour * 100 + d.minute
    return d.year * 10000 + d.month * 100 + d.day


def is_trade_date(date: dt.datetime or str or int):
    d = to_date(date)
    return not is_holiday(d) and d.weekday() <= 4


def drange(start: str or dt.datetime, end: str or dt.datetime = None, step: int or dt.timedelta = None):
    if end is None:
        return [start]
    if start is None:
        return [end]
    if step is None:
        step = 1 if end > start else -1
    start = to_date(start)
    end = to_date(end)
    step = to_delta(step)

    ret = []
    if step > dt.timedelta(0) and end < start:
        return ret
    if step < dt.timedelta(0) and end > start:
        return ret
    if step == dt.timedelta(0) and end == start:
        return ret
    if step == dt.timedelta(0):
        raise ValueError("step cannot be zero!")
    while start <= end:
        yield start
        start += step
    yield from ()


def trade_range(start: str or dt.datetime, end: str or dt.datetime = None, step: int or dt.timedelta = 1):
    for d in drange(start, end, step):
        if not is_holiday(d) and d.weekday() <= 4:
            yield d

    yield from ()


def next_trade_day(day, count):
    if count == 0:
        return day
    day = to_date(day)
    step = 1 if count > 0 else -1
    step = to_delta(step)
    num = 0
    while True:
        if is_trade_date(day):
            if num >= abs(count):
                return day
            num += 1
        day += step


def find_trade_day(delta_days=0, date=None):
    """
    find the workday after {delta_days} days.

    :type delta_days: int
    :param delta_days: 0 means next workday (includes today), -1 means previous workday.
    :type date: datetime.date | datetime.datetime
    :param: the start point
    :rtype: datetime.date
    """
    date = to_date(date or dt.date.today())
    if delta_days >= 0:
        delta_days += 1
    sign = 1 if delta_days >= 0 else -1
    sign = to_delta(sign)
    for i in range(abs(delta_days)):
        if delta_days < 0 or i:
            date += sign
        while not is_trade_date(date):
            date += sign
    return date


def trader_today():
    today = dt.datetime.today()
    if today.time() < dt.time(9, 31) or not is_trade_date(today):
        today = next_trade_day(today, -1)
    return today
