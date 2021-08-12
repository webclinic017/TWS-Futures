import pandas as pd
from datetime import timedelta
from json import dumps

from tws_futures.helpers.utils import make_datetime
from tws_futures.helpers.utils import find_date
from tws_futures.helpers.utils import get_business_days
from tws_futures.helpers.validators import VALIDATION_MAP
from tws_futures.settings import *


def calculate_days_of_first_expiry(start, expiry, end, check=None):
    r1 = get_business_days(start, expiry) if check == 'first' else get_business_days(
        end, expiry)
    r2 = min(end, expiry).date() if check == 'first' else expiry.date()
    return r1, r2


def generate_expiries_input(symbol, start_date, end_date, bar_size, expiries):
    expiry_list = []
    labels = ['symbol', 'lasttradedateorcontractmonth', 'end_date', 'days', 'minbar']
    data = []
    for i in range(len(expiries)):
        expiry = expiries[i]
        if i == 0:
            business_days = get_business_days(start_date, expiry)
            end_date_ = str(min(end_date, expiry).date()).replace('-', '')
            expiry_ = str(expiry.date()).replace('-', '')
            first_out = str(symbol) + ',' + str(expiry).replace('-', '') + ',' + str(
                end_date_).replace('-', '') + ',' + str(business_days) + ',' + bar_size
            expiry_list.append(first_out)
            values = [symbol, expiry_, end_date_, business_days, bar_size]
            temp = dict(zip(labels, values))
            temp.update(CONTRACTS[symbol])
            data.append(temp)
        elif i == (len(expiries) - 1):
            business_days = get_business_days(start_date, end_date)
            end_date__ = str(end_date.date()).replace('-', '')
            expiry_ = str(expiry.date()).replace('-', '')
            last, end_date_ = calculate_days_of_first_expiry(start=expiries[i - 1],
                                                             expiry=end_date,
                                                             end=start_date,
                                                             check='last')
            last_out = str(symbol) + ',' + str(expiries[i]).replace('-', '') + ',' + str(
                end_date_).replace('-', '') + ',' + str(last) + ',' + bar_size
            expiry_list.append(last_out)
            values = [symbol, expiry_, end_date__, business_days, bar_size]
            temp = dict(zip(labels, values))
            temp.update(CONTRACTS[symbol])
            data.append(temp)
        else:
            all_other = 83
            all_other_out = str(symbol) + ',' + str(expiries[i]).replace('-',
                                                                         '') + ',' + str(
                expiries[i]).replace('-', '') + ',' + str(all_other) + ',' + bar_size
            business_days = 83
            expiry_ = str(expiry.date()).replace('-', '')
            end_date_ = str(expiry.date()).replace('-', '')
            expiry_list.append(all_other_out)
            values = [symbol, expiry_, end_date_, business_days, bar_size]
            temp = dict(zip(labels, values))
            temp.update(CONTRACTS[symbol])
            data.append(temp)

    print(f'EL: {expiry_list}')
    with open('expiries_input.csv', 'a') as file:
        line = "symbol,lasttradedateorcontractmonth,end_date,days,minbar"
        file.write(line)
        file.write('\n')
        for i in expiry_list:
            file.write(i)
            file.write('\n')

    print('========= ======== ==============')
    print(dumps(data, indent=2))


def find_target_expiries(start_date, end_date, expiries):
    # TODO: optimize
    target_expiries = []
    for expiry in expiries:
        expiry = make_datetime(expiry)
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
        if business_days < duration:
            break
        start_date = start_date - timedelta(days=1)
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


def generate_expiry_input(symbol, end_date, duration, bar_size='1 min'):
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
    print(f'Start: {start_date} | End: {end_date}')
    all_expiries = get_expiries(symbol)
    validate_target_dates(start_date, end_date, all_expiries)
    target_expiries = find_target_expiries(start_date, end_date, all_expiries)
    generate_expiries_input(symbol, start_date, end_date, bar_size, target_expiries)
    return 0
    # read_fut_contract = pd.read_csv(FUTURES_INPUT)
    # read_expiry = pd.read_csv('expiries_input.csv')
    # expiry_output = read_expiry.merge(read_fut_contract, how='inner')
    # print('Input file feeding to API')
    # return [symbol, '_', target_date, '_', duration, 'ay_', bar_size], expiry_output


def _test():
    # TODO: end date constraint
    expiry_input = generate_expiry_input(symbol='N225',
                                         end_date='20210726',
                                         duration=150,
                                         bar_size='1 min')
    print(expiry_input)


if __name__ == '__main__':
    _test()
