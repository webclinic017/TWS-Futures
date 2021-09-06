import pandas as pd
from datetime import timedelta

from tws_futures.helpers.utils import make_datetime
from tws_futures.helpers.utils import find_date
from tws_futures.helpers.utils import get_business_days
from tws_futures.helpers.validators import VALIDATION_MAP
from tws_futures.settings import *
import logging


logger = logging.getLogger(__name__)


def generate_expiries_input(symbol, start_date, end_date, bar_size, expiries):
    labels = ['symbol', 'expiry', 'end_date', 'duration', 'bar_size']
    data = []
    for i in range(len(expiries)):
        business_days = 83  # contract has a maturity period of ~65 days + two weeks of
        # overlap
        expiry = expiries[i]
        if i == 0:
            business_days = get_business_days(start_date, expiry)
            end_date_ = str(min(end_date, expiry).date()).replace('-', '')
            expiry_ = str(expiry.date()).replace('-', '')
        elif i+1 == len(expiries):
            business_days = get_business_days(expiries[i-1], end_date) + 20
            end_date_ = str(end_date.date()).replace('-', '')
            expiry_ = str(expiry.date()).replace('-', '')
        else:
            expiry_ = str(expiry.date()).replace('-', '')
            end_date_ = str(expiry.date()).replace('-', '')
        values = [symbol, expiry_, end_date_, business_days, bar_size]
        temp = dict(zip(labels, values))
        temp.update(CONTRACTS[symbol])
        data.append(temp)

    return data


def find_target_expiries(start_date, end_date, expiries):
    # TODO: optimize
    target_expiries = []
    for expiry in expiries:
        expiry = make_datetime(expiry)
        # if expiry not in target_expiries:
        if expiry >= start_date:
            if expiry <= end_date:
                target_expiries.append(expiry)
            else:
                business_days = get_business_days(end_date, expiry)
                if business_days <= 90:
                    target_expiries.append(expiry)
                else:
                    break
    return target_expiries


def find_start_date(end_date, duration, weekends=False):
    """
        Find starting date from a given end date and duration
        :param end_date: date from which start date is to be computed
        :param duration: number of days to go back from end_date
        :param weekends: True if weekends are to be counted as business days
        :return: datetime object for start date
    """
    with_weekdays = round(duration + ((duration / 7) * 2))
    start_date = find_date(end_date, with_weekdays)
    while True:
        business_days = get_business_days(start_date, end_date, weekends=weekends)
        start_date -= timedelta(days=1)
        if business_days >= duration:
            break
    return start_date


def _validator(**kwargs):
    for key in kwargs:
        if key in VALIDATION_MAP:
            VALIDATION_MAP[key](kwargs[key])


def validate_target_dates(start, end, expiries):
    first_expiry = make_datetime(expiries[0])
    last_expiry = make_datetime(expiries[-1])
    if start < first_expiry:
        raise ValueError(f'Start date can not be less than: {first_expiry.date()}')
    if end > last_expiry:
        raise ValueError(f'End date can not be greater than: {last_expiry.date()}')


def get_expiries(symbol):
    expiries = pd.read_csv(FUTURES_EXPIRIES)
    assert symbol in expiries, f'Can not find expiries for symbol: {symbol}'
    return expiries[symbol].sort_values().astype(str).tolist()


def generate_expiry_input(symbol, end_date, duration=1, bar_size='1 min'):
    """
        :param end_date: end date --> %Y%m%d
        :param duration: number of days to go back from target date
        :param bar_size: size of the bar for data extraction
        :param symbol: target symbol
        :return: #TODO
    """
    _validator(symbol=symbol, date=end_date, duration=duration,
               bar_size=bar_size)
    end_date = make_datetime(end_date)
    start_date = find_start_date(end_date, duration)
    all_expiries = get_expiries(symbol)
    validate_target_dates(start_date, end_date, all_expiries)
    target_expiries = find_target_expiries(start_date, end_date, all_expiries)
    return generate_expiries_input(symbol, start_date, end_date, bar_size,
                                   target_expiries)


def load_expiry_input(symbol=None, end_date=None, duration=1):
    logger.info('Generating expiry input')
    expiry_input = []
    if symbol and symbol in CONTRACTS:
        expiry_input.extend(generate_expiry_input(symbol, end_date, duration))
    else:
        for contract in CONTRACTS:
            expiry_input.extend(generate_expiry_input(contract, end_date, duration))
    logger.debug(f'Expiry input: {expiry_input}')
    return expiry_input


def _test():
    # TODO: end date constraint
    from json import dumps
    expiry_input = generate_expiry_input(symbol='N225',
                                         end_date='20210901',
                                         duration=1,
                                         bar_size='1 min')
    print(dumps(expiry_input, indent=2))


if __name__ == '__main__':
    _test()
