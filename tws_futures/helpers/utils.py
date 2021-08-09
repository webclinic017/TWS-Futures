from datetime import datetime as dt
from datetime import timedelta
from numpy import busday_count


def make_datetime(target_date, date_format='%Y%m%d'):
    return dt.strptime(target_date, date_format)


def find_date(target_date, number_of_days):
    assert isinstance(target_date, dt), f'Target date must be a datetime object'
    return target_date - timedelta(days=number_of_days)


def get_weekmask(weekends=False):
    return [1, 1, 1, 1, 1, 1, 1] if weekends else [1, 1, 1, 1, 1, 0, 0]


def get_holidays(year='2021'):
    return [f'{year}-01-01']


def get_business_days(start, end, weekends=False):
    return int(busday_count(start.date(), end.date(),
                            weekmask=get_weekmask(weekends),
                            holidays=get_holidays(start.date().year)))
