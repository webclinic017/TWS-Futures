# -*- coding: utf-8 -*-

# from alive_progress import alive_bar
import json
from tws_futures.tws_clients.base import *
from tws_futures.tws_clients.data_extractor import HistoricalDataExtractor
from tws_futures.helpers.utils import make_dirs
from os.path import join
from logging import getLogger
from tws_futures.settings import CACHE_DIR

_CACHE_THRESHOLD = 10
_BATCH_SIZE = 30
_BAR_CONFIG = {
                    'title': '=> Status∶',
                    'calibrate': 5,
                    'force_tty': True,
                    'spinner': 'dots_reverse',
                    'bar': 'smooth'
              }
logger = getLogger(__name__)


def cache(data):
    _meta = data['meta_data']
    end_date = _meta['end_date']
    storage_dir = join(CACHE_DIR, end_date)
    make_dirs(storage_dir)
    file_name = join(storage_dir, f'{_meta["symbol"]}_{_meta["expiry"]}.json')
    with open(file_name, 'w') as f:
        f.write(json.dumps(data, indent=1, sort_keys=True))


def extract_historical_data(expiry_input):
    # logger.info(f'Attempt: {run_counter} | max attempts: {max_attempts}')
    client = HistoricalDataExtractor()
    for i in expiry_input:
        client.extract_historical_data(i)
        cache(client.data)


if __name__ == '__main__':
    expiry_input = [
                        {
                            "bar_size": "1 min",
                            "currency": "JPY",
                            "duration": 7,
                            "end_date": "20210902",
                            "exchange": "OSE.JPN",
                            "expiry": "20210909",
                            "include_expired": True,
                            "multiplier": 1000,
                            "security_type": "FUT",
                            "symbol": "N225"
                        },
                        {
                            "bar_size": "1 min",
                            "currency": "JPY",
                            "duration": 15,
                            "end_date": "20210902",
                            "exchange": "OSE.JPN",
                            "expiry": "20211209",
                            "include_expired": True,
                            "multiplier": 1000,
                            "security_type": "FUT",
                            "symbol": "N225"
                        }
                    ]
    extract_historical_data(expiry_input)
