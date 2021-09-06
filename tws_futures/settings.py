from os.path import join
from pathlib import Path


# directories
BASE_DIR = Path(__file__).parent.parent
PROJECT_DIR = join(BASE_DIR, 'tws_futures')
DATA_FILES = join(PROJECT_DIR, 'data_files')
CACHE_DIR = join(BASE_DIR, '.cache')
HISTORICAL_DATA_STORAGE = join(BASE_DIR, 'historical_data')
FUTURES_INPUT = join(DATA_FILES, 'futures_input.csv')
FUTURES_EXPIRIES = join(DATA_FILES, 'future_contracts', 'futures_expiry.csv')

# contracts
_N225 = dict(symbol='N225', security_type='FUT', exchange='OSE.JPN',
             currency='JPY', multiplier=1000, include_expired=True)

_TOPX = dict(symbol='TOPX', security_type='FUT', exchange='OSE.JPN',
             currency='JPY', multiplier=10000, include_expired=True)
_JGB = dict(symbol='JGB', security_type='FUT', exchange='OSE.JPN',
             currency='JPY', multiplier=1000000, include_expired=True)
JGB,FUT,OSE.JPN,JPY,1000000,165120001,20201012

# month map, used to convert integer values to string for months
MONTH_MAP = {
    1: 'january',
    2: 'february',
    3: 'march',
    4: 'april',
    5: 'may',
    6: 'june',
    7: 'july',
    8: 'august',
    9: 'september',
    10: 'october',
    11: 'november',
    12: 'december'
}

CONTRACTS = dict(N225=_N225, TOPX=_TOPX)
