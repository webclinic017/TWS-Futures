''' utility functions for main class'''

__author__ = 'Kiran Kumar S'

import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import property as prop


def load_data_to_csv(reqId, bar, vcount, fname):
    if vcount == 0 and (prop.file_exist == False):
        fname = 'historicaldatas'
        with open(str(fname)+'.csv', 'a') as file:
            if fname == 'historicaldatas':
                line = "ecode,date,reqId,session,open,high,low,close,volume,barcount,average"
            else:
                line = 'No header'
            file.write(line)
            file.write('\n')
    else:
        with open(str(fname)+'.csv','a') as file:
            bar_date = str(bar.date)
            bar_date = bar_date.split(' ')[-1]
            bar_date = pd.to_timedelta(bar_date)
            check_date = pd.to_timedelta('12:00:00')
            session = np.where(bar_date < check_date, 1, 2)
            reqno = 1
            line = str(reqId) + str(',') + str(bar.date)+ str(',')+ str(reqno)+str(',')+str(session)+str(',')+ str(bar.open) + str(',') + str(bar.high) + str(',') + str(bar.low) + str(',') + str(bar.close) + str(',') + str(bar.volume) + str(',') + str(bar.barCount) + str(',') + str(bar.average)
            file.write(line)
            file.write('\n')


def no_historical_data(reqId, no_hist_count, fname):
    if no_hist_count == 0 and (not prop.hist_file_check):
        with open(str(fname)+'.csv', 'a') as file:
            if fname == 'nohistdata':
                line1 = "nohist"
                line2 = str(reqId)
            else:
                line1 = fname
                line2 = str(reqId)
            file.write(line1)
            file.write('\n')
            file.write(line2)
            file.write('\n')

    else:
        with open(str(fname)+'.csv','a') as file:
            line = str(reqId)
            file.write(line)
            file.write('\n')


def load_2nd_attempt_historical_data(flist):
    f_fail_list = pd.DataFrame(flist,columns=['ftickers'])
    f_fail_list.to_csv('tickers_after_2nd_attempt.csv',index=False)


def ticker_difference():
    processed = pd.read_csv('historicaldata.csv')
    processed = list(processed['reqId'].unique())
    original = pd.read_csv('Tickers.csv')
    original = original['Code'].to_list()
    diff = list(set(original)-set(processed))
    return diff, len(diff)


def write_csv(p_stocks):
    processed_stocks = pd.DataFrame(p_stocks)
    processed_stocks.to_csv('historicaldatas.csv', mode='a', header=False,index=False)


def session_req_no(bar):
    bar_date = str(bar.date)
    bar_date = bar_date.split(' ')[-1]
    bar_date = pd.to_timedelta(bar_date)
    check_date = pd.to_timedelta('12:00:00')
    session = np.where(bar_date < check_date, 1, 2)
    reqno = 1
    return session, reqno


def load_datas_to_csv(reqId, bar):
    if reqId not in prop.reqlist:
        with open(str(reqId)+'.csv', 'a') as file:
            line = "expiry,date,open,high,low,close,volume,barcount"
            file.write(line)
            file.write('\n')
            prop.reqlist.append(reqId)

    else:
        with open(str(reqId)+'.csv','a') as file:
            line = str(reqId) + str(',') + str(bar.date)+str(',')+ str(bar.open) + str(',') + str(bar.high) + str(',') + str(bar.low) + str(',') + str(bar.close) + str(',') + str(bar.volume) + str(',') + str(bar.barCount)
            file.write(line)
            file.write('\n')


def tick_by_tick(time, stock_value, size):
    with open(str('tickbytick')+'.csv', 'a') as file:
            file.write(str(time)+','+str(stock_value)+','+str(size))
            file.write('\n')


def calc_min_day(seconds):
    days = seconds/86400
    return days


def do_round_up(check_day1, check_day2):
    if check_day1 != check_day2:
        check_day2 += 1
    return check_day2


def date_finder(given_date,number_of_days):
    given_d = datetime.datetime.strptime(given_date, '%Y%m%d')
    date2 = given_d - relativedelta(days = int(number_of_days))
    return date2.date()


def working_day_calculation(start_date,end_date):
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    result = int(np.busday_count(start_date.date(), end_date.date(), weekmask=[1,1,1,1,1,0,0], holidays=['2020-01-01']))
    if result <= 20 and result >= 0 :
        return True
    elif (result < 0) and (result > -90):
        return True
    else:
        return False


def end_date_constrains(start_date, end_date):
    start_start_date = datetime.datetime.strptime('20000309','%Y%m%d')
    end_end_date = datetime.datetime.strptime('20260312','%Y%m%d')
    start = np.where(start_date < start_start_date, 1, 2)
    end = np.where(end_date > end_end_date, 1, 2)
    if start == 1:
        print("WARNING: Start date is beyond the duration period's start date - 2000-03-09")
    elif end == 1:
        print("WARNING: End date is beyond the duration period end date - 2026-03-12")
    elif (start == 1) & (end == 1):
        print("WARNING: Start,End date durations are going beyond the start and end date's duration - 2000-03-09 to 2026-03-12")
    else:
        pass


def calculate_days_of_first_expiry(start_date, end_date, o_date, check=None):
    final_check_date = 0
    e_date = 0
    if check == 'first':
        check1 = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        check2 = datetime.datetime.strptime(str(o_date.date()), '%Y-%m-%d')
        out = np.where(check2 < check1, 1, 2)
        if out == 1:
            end_date = check2
        else:
            end_date = check1
    if check == 'last':
        start = datetime.datetime.strptime(str(o_date.date()), '%Y-%m-%d')
        l_date = int(np.busday_count(start.date(), end_date.date(), weekmask=[1,1,1,1,1,0,0], holidays=['2020-01-01']))
        if l_date < 83:
            final_check_date = l_date
            e_date = end_date.date()
    if (final_check_date > 0) and (final_check_date <= 83):
        return final_check_date,e_date
    else:
        if type(start_date) == str:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        result = int(np.busday_count(start_date.date(), end_date.date(), weekmask=[1,1,1,1,1,0,0], holidays=['2020-01-01']))
        return result,end_date.date()


def generate_each_input(symbol, start_date, end_date, minbar,expiries):
    expiry_list = []
    for expiry in range(len(expiries)):
        if expiry == 0:
            first,e_date = calculate_days_of_first_expiry(start_date, expiries[expiry], end_date, check='first')
            print(first)
            first_out = str(symbol)+','+str(expiries[expiry]).replace('-','')+','+str(e_date).replace('-','')+','+str(first)+','+minbar
            expiry_list.append(first_out)
        elif expiry == (len(expiries)-1):
            last,e_date = calculate_days_of_first_expiry(expiries[expiry-1], end_date, start_date, check='last')
            print(last)
            last_out = str(symbol)+','+str(expiries[expiry]).replace('-','')+','+str(e_date).replace('-','')+','+str(last)+','+minbar
            expiry_list.append(last_out)
        else:
            all_other = 83
            all_other_out = str(symbol)+','+str(expiries[expiry]).replace('-','')+','+str(expiries[expiry]).replace('-','')+','+str(all_other)+','+minbar
            expiry_list.append(all_other_out)
    with open('expiries_input.csv', 'a') as file:
        line = "symbol,lasttradedateorcontractmonth,end_date,days,minbar"
        file.write(line)
        file.write('\n')
        for i in expiry_list:
            file.write(i)
            file.write('\n')


def find_end_date_without_weekend(duration_value,input_date):
    import numpy as np
    with_weekdays = round(duration_value+((duration_value/7)*2))
    f_result = {}
    while True:
        np_input_date = datetime.datetime.strptime(input_date,'%Y%m%d')
        start_date = date_finder(input_date, with_weekdays)
        #print(start_date)
        if 'start_date' not in f_result:
            f_result['start_date'] = start_date
        result = int(np.busday_count(f_result['start_date'],
                                     np_input_date.date(),
                                     weekmask=[1, 1, 1, 1, 1, 0, 0],
                                     holidays=['2020-01-01']))
        if 'result' not in f_result:
            f_result['result'] = result
        if f_result['result'] >= duration_value:
            return f_result['result'],f_result['start_date']
        else:
            f_result['result'] = result
            f_result['start_date'] = f_result['start_date'] - datetime.timedelta(days=1)


def tick_by_tick(expiry,time, stock_value, size):
    with open(str('tickbytick')+'.csv', 'a') as file:
            file.write(str(expiry)+','+str(time)+','+str(stock_value)+','+str(size))
            file.write('\n')


def find_expiry(start_date, end_date, symbol):
    #start_date = datetime.datetime.strptime('20160311','%Y%m%d')
    #end_date = datetime.datetime.strptime('20200728','%Y%m%d')
    # day_name = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    future_expiry = pd.read_excel('future_contracts.xlsx', skiprows=1, engine='openpyxl')
    future_list = future_expiry[symbol].to_list()
    print(f'FL: {future_list}')
    last_count = 0
    count = 0
    expiries = []
    for i in range(len(future_list)-1):
        expiry_date = datetime.datetime.strptime(str(future_list[i]), '%Y%m%d')
        start = np.where(expiry_date < start_date, 1, 2)
        end = np.where(expiry_date > end_date, 1, 2)
        if start == end:
            count += 1
            expiries.append(str(expiry_date.date()))
        if not expiries:
            if (end == 1) and (last_count == 0):
                last_expiry_date_check = datetime.datetime.strptime(str(future_list[i]),'%Y%m%d')
                last_expiry_date_before_check = datetime.datetime.strptime(str(future_list[i-1]),'%Y%m%d')
                if (end_date < last_expiry_date_check) and (end_date > last_expiry_date_before_check):
                    expiries.append(str(last_expiry_date_check.date()))
        elif (end == 1) and (count > 1) and (last_count == 0):
            last_expiry_date_check = datetime.datetime.strptime(str(future_list[i]),'%Y%m%d')
            last_expiry_date_before_check = datetime.datetime.strptime(str(future_list[i-1]),'%Y%m%d')
            if (end_date < last_expiry_date_check) and (end_date > last_expiry_date_before_check):
                expiries.append(str(last_expiry_date_check.date()))
            last_count += 1
    if working_day_calculation(str(end_date.date()), expiries[-1]):
        value = expiries[-1].replace('-', '')
        if len(future_list) != future_list.index(int(value)):
            sub_string = str(future_list[future_list.index(int(value))+1])
            sub_string = sub_string[:4]+'-'+sub_string[4:6]+'-'+sub_string[6:]
            expiries.append(sub_string)
    return expiries


def _test():
    s = datetime.datetime.strptime('2020-11-10', '%Y-%m-%d')
    e = datetime.datetime.strptime('2020-11-25', '%Y-%m-%d')
    ex = find_expiry(s, e, 'N225')
    print(f'Ex: {ex}')


if __name__ == '__main__':
    _test()
