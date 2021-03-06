from os.path import isfile
from datetime import datetime as dt
from tws_futures.settings import CONTRACTS


def _file_validator(file_path):
    assert isfile(file_path), f'File does not exist: {file_path}'


def _date_validator(target_date):
    try:
        dt.strptime(target_date, '%Y%m%d')
    except Exception as e:
        raise e


def _duration_validator(duration):
    # value, unit = duration.split(' ')
    # units = ('D',)
    # assert value.isdigit(), f'Duration value must be a number: {duration}'
    # assert unit in units, f'Duration unit must be in: {units}'
    assert isinstance(duration, int), f'Duration must be an integer'


def _bar_validator(bar_size):
    value, unit = bar_size.split(' ')
    units = ('min',)
    assert value.isdigit(), f'Bar size value must be a number: {bar_size}'
    assert unit in units, f'Bar size unit must be in: {units}'


def _symbol_validator(symbol):
    supported_symbols = CONTRACTS.keys()
    assert symbol in supported_symbols, f'Symbol: {symbol} is not supported'


VALIDATION_MAP = dict(symbol=_symbol_validator,
                      date=_date_validator,
                      duration=_duration_validator,
                      bar_size=_bar_validator)
