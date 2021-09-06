import json
from datetime import datetime as dt
from datetime import timedelta
from numpy import busday_count
from os import makedirs
from os.path import isdir
from os.path import join
from os import listdir
import pandas as pd
from tws_futures.settings import CACHE_DIR
from tws_futures.settings import HISTORICAL_DATA_STORAGE
from tws_futures.settings import MONTH_MAP


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
                            holidays=[f'{start.year}']))


def make_dirs(path):
    if not(isdir(path)):
        makedirs(path)


def save_json(data, file_path):
    with open(file_path, 'w') as f:
        f.write(json.dumps(data, indent=1, sort_keys=True))


def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.loads(f.read())


def setup(end_date):
    year, month = end_date[:4], int(end_date[4:6])
    target_dir = join(HISTORICAL_DATA_STORAGE, year, MONTH_MAP[month])
    make_dirs(target_dir)
    return target_dir


def generate_csv(end_date):
    target_dir = setup(end_date)
    columns = ['symbol', 'expiry', 'end_date', 'end_time', 'open', 'high',
               'low', 'close', 'average', 'volume', 'bar_count']
    data = pd.DataFrame(columns=columns)
    data_dir = join(CACHE_DIR, end_date)
    files = list(filter(lambda x: x.endswith('json'), listdir(data_dir)))
    for file in files:
        file_path = join(data_dir, file)
        temp = load_json(file_path)
        _bars, _meta = temp['bar_data'], temp['meta_data']
        temp = pd.DataFrame(_bars)
        temp['symbol'], temp['expiry'] = _meta['symbol'], _meta['expiry']
        data = data.append(temp)
    data.to_csv(join(target_dir, f'{end_date}.csv'), index=False)


def _test():
    generate_csv('20210902')


if __name__ == '__main__':
    _test()
