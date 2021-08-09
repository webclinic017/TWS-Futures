from os.path import join
from pathlib import Path


BASE_DIR = Path(__file__).parent.parent
PROJECT_DIR = join(BASE_DIR, 'tws_futures')
DATA_FILES = join(PROJECT_DIR, 'data_files')
FUTURES_INPUT = join(DATA_FILES, 'futures_input.csv')
FUTURES_EXPIRY = join(DATA_FILES, 'future_contracts', 'futures_expiry.csv')
# print(f'B: {BASE_DIR}')
