from os.path import isfile
import pandas as pd
from tws_futures.settings import FUTURES_INPUT as _FUTURES_INPUT


def futures_input(file_path=_FUTURES_INPUT):
    assert type(file_path) == str, 'File path must be string'
    assert isfile(file_path), 'File does not exist'
    assert file_path.endswith('.csv'), 'File must be a CSV'
    return pd.read_csv(file_path)
