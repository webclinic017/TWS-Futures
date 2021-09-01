from os.path import join
from pathlib import Path


# directories
BASE_DIR = Path(__file__).parent.parent
PROJECT_DIR = join(BASE_DIR, 'tws_futures')
DATA_FILES = join(PROJECT_DIR, 'data_files')
FUTURES_INPUT = join(DATA_FILES, 'futures_input.csv')
FUTURES_EXPIRIES = join(DATA_FILES, 'future_contracts', 'futures_expiry.csv')

# contracts
_N225 = dict(symbol='N225', security_type='FUT', exchange='OSE.JPN',
             currency='JPY', multiplier=1000, include_expired=True)

_TOPX = dict(symbol='TOPX', security_type='FUT', exchange='OSE.JPN',
             currency='JPY', multiplier=10000, include_expired=True)

CONTRACTS = dict(N225=_N225, TOPX=_TOPX)
