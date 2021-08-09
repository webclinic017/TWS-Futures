import os
import pandas as pd
from numpy import busday_count
from numpy import where
from datetime import timedelta, datetime

#
# from tws_futures.data_files import futures_input as read_futures_input
from tws_futures.helpers.utils import make_datetime
from tws_futures.helpers.utils import find_date
from tws_futures.helpers.utils import get_business_days

from tws_futures.helpers.validators import VALIDATION_MAP

from tws_futures.settings import *


def calculate_days_of_first_expiry(start, expiry, end, check=None):
    # print(f'FCC -> S: {start} | E: {expiry} | O: {end} | C: {check}')
    final_check_date = 0
    e_date = 0
    if check == 'first':
        # check1 = make_datetime(expiry, '%Y-%m-%d')
        check1 = expiry
        # check2 = make_datetime(str(end.date()), '%Y-%m-%d')
        check2 = end
        out = where(check2 < check1, 1, 2)
        print(f'OOO: {out}')
        if check2 < check1:
            print('herrrrrrrrrr`')
            expiry = check2
        # if out == 1:
        #     print(111111111)
        #     expiry = check2
        # else:
        #     print(22222222222)
        #     expiry = check1
    if check == 'last':
        start = make_datetime(str(end.date()), '%Y-%m-%d')
        l_date = int(
            busday_count(start.date(), expiry.date(), weekmask=[1, 1, 1, 1, 1, 0, 0],
                         holidays=['2020-01-01']))
        if l_date < 83:
            final_check_date = l_date
            e_date = expiry.date()
    if (final_check_date > 0) and (final_check_date <= 83):
        return final_check_date, e_date
    else:
        if type(start) == str:
            start = make_datetime(start, '%Y-%m-%d')
        result = int(
            busday_count(start.date(), expiry.date(), weekmask=[1, 1, 1, 1, 1, 0, 0],
                         holidays=['2020-01-01']))
        print(f'RRR: {result}')
        return result, expiry.date()


def working_day_calculation(start_date, end_date):
    r = get_business_days(start_date, end_date, weekends=False)
    print(f'SS: {start_date} | EE: {end_date} | RR: {r}')
    return -90 < r <= 90


def generate_expiries_input(symbol, start_date, end_date, bar_size, expiries):
    expiry_list = []
    for i in range(len(expiries)):
        if i == 0:
            first, e_date = calculate_days_of_first_expiry(start=start_date,
                                                           expiry=expiries[i],
                                                           end=end_date,
                                                           check='first')
            print(f'F: {first} | ED: {e_date}')
            first_out = str(symbol) + ',' + str(expiries[i]).replace('-', '') + ',' + str(
                e_date).replace('-', '') + ',' + str(first) + ',' + bar_size
            expiry_list.append(first_out)
        elif i == (len(expiries) - 1):
            last, e_date = calculate_days_of_first_expiry(start=expiries[i - 1],
                                                          expiry=end_date,
                                                          end=start_date,
                                                          check='last')
            last_out = str(symbol) + ',' + str(expiries[i]).replace('-', '') + ',' + str(
                e_date).replace('-', '') + ',' + str(last) + ',' + bar_size
            expiry_list.append(last_out)
        else:
            all_other = 83
            all_other_out = str(symbol) + ',' + str(expiries[i]).replace('-',
                                                                         '') + ',' + str(
                expiries[i]).replace('-', '') + ',' + str(all_other) + ',' + bar_size
            expiry_list.append(all_other_out)

    print(f'EL: {expiry_list}')
    with open('expiries_input.csv', 'a') as file:
        line = "symbol,lasttradedateorcontractmonth,end_date,days,minbar"
        file.write(line)
        file.write('\n')
        for i in expiry_list:
            file.write(i)
            file.write('\n')


def find_expiries(start_date, end_date, symbol):
    # TODO: optimize
    print(f'Finding expiries...')
    print(f'S: {start_date} | E: {end_date}')
    data = pd.read_csv(FUTURES_EXPIRY)
    expiries = []
    data[symbol] = data[symbol].apply(pd.to_datetime, format='%Y%m%d')
    _end_date = str(end_date.date())
    for expiry in data[symbol]:
        print(f'Expiry: {expiry}')
        if expiry >= start_date:
            if expiry <= end_date:
                print(1)
                expiries.append(expiry)
            else:
                if len(expiries) == 0:
                    print(2)
                    if working_day_calculation(end_date, expiry):
                        print(22)
                        expiries.append(expiry)
                else:
                    print(33)
                    if working_day_calculation(end_date, expiry):
                        print(3)
                        expiries.append(expiry)
                    else:
                        break
    # expiries = list(map(lambda x: str(x.date()), expiries))
    return expiries


def end_date_constrains(start_date, end_date):
    # TODO: first check
    start_start_date = make_datetime('20000309', '%Y%m%d')
    end_end_date = make_datetime('20260312', '%Y%m%d')
    start = where(start_date < start_start_date, 1, 2)
    end = where(end_date > end_end_date, 1, 2)
    if start == 1:
        print("WARNING: Start date is beyond the duration period's start date - "
              "2000-03-09")
    elif end == 1:
        print("WARNING: End date is beyond the duration period end date - 2026-03-12")
    elif (start == 1) & (end == 1):
        print("WARNING: Start,End date durations are going beyond the start and end "
              "date's duration - 2000-03-09 to 2026-03-12")
    else:
        pass


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


def do_round_up(check_day1, check_day2):
    if check_day1 != check_day2:
        check_day2 += 1
    return check_day2


def calc_min_day(seconds):
    days = seconds / 86400
    return days


def date_calculate_to_expiry(end_date, duration, bar_size, symbol):
    # remove expiry file
    no_file_check = os.path.isfile('./expiries_input.csv')
    if no_file_check:
        os.remove('./expiries_input.csv')
    duration = duration.split()
    duration_value = int(duration[0])
    duration_unit = duration[1]
    if duration_unit == 'S':
        f_days = calc_min_day(duration_value)
        l_days = int(f_days)
        days = do_round_up(f_days, l_days)
        if days > 28:
            pass  # need to handle months for inputs go beyond month
    elif duration_unit == 'D':
        start_date = find_start_date(end_date, duration_value)
        end_date_constrains(start_date, end_date)
        expiries = find_expiries(start_date, end_date, symbol)
        print(f'Expiries: {expiries}')
        return
        generate_expiries_input(symbol, start_date, end_date, bar_size, expiries)
    elif duration_unit == 'W':
        duration_v = duration_value * 7
        end_date = make_datetime(end_date, '%Y%m%d')
        _given_dates, start_date = find_start_date(end_date,
                                                   duration_v)
        start_date = make_datetime(str(start_date), '%Y-%m-%d')
        end_date_constrains(start_date, end_date)
        expiries = find_expiries(start_date, end_date, symbol)
        generate_expiries_input(symbol, start_date, end_date, bar_size, expiries)


def _validator(**kwargs):
    for key in kwargs:
        if key in VALIDATION_MAP:
            VALIDATION_MAP[key](kwargs[key])


def load_expiry_inputs(file_path, target_date, duration, bar_size, symbol):
    """
        :param file_path: path to CSV file that contains futures input
        :param target_date: end date --> %Y%m%d
        :param duration: number of days to go back from target date
        :param bar_size: size of the bar for data extraction
        :param symbol: target symbol
        :return: #TODO
    """
    _validator(file_path=file_path, target_date=target_date, duration=duration,
               bar_size=bar_size, symbol=symbol)
    target_date = make_datetime(target_date)
    print('Generating Expiries!!!')
    date_calculate_to_expiry(target_date, duration, bar_size, symbol)
    return 0
    # print('Expiries Generated!!!')
    # print('')
    # print('Combining expiries with relevant contracts!!!')
    # read_fut_contract = pd.read_csv(FUTURES_INPUT)
    # return
    # read_expiry = pd.read_csv('expiries_input.csv')
    # expiry_output = read_expiry.merge(read_fut_contract, how='inner')
    # print('Input file feeding to API')
    # return [symbol, '_', input_date, '_', duration, 'ay_', minbar], expiry_output


def _test():
    # TODO: end date constraint
    expiry_input = load_expiry_inputs('future_input.csv',
                                      '20210726',
                                      '100 D',
                                      '1 min',
                                      'N225')
    print(expiry_input)


if __name__ == '__main__':
    _test()
