from os.path import join
from pathlib import Path


# directories
BASE_DIR = Path(__file__).parent.parent
PROJECT_DIR = join(BASE_DIR, 'tws_futures')
DATA_FILES = join(PROJECT_DIR, 'data_files')
FUTURES_INPUT = join(DATA_FILES, 'futures_input.csv')
FUTURES_EXPIRIES = join(DATA_FILES, 'future_contracts', 'futures_expiry.csv')

# contracts
_N225 = dict(symbol='N225', sectype='FUT', exchange='OSE.JPN',
             currency='JPY', multiplier=1000, includeexpired=True)

CONTRACTS = dict(N225=_N225)
