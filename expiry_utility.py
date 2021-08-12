import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd

def calc_min_day(seconds):
    days = seconds/86400
    return days

def do_round_up(check_day1, check_day2):
    if check_day1 != check_day2:
        check_day2 += 1
    return check_day2

def date_finder(given_date,number_of_days):
    given_d = datetime.datetime.strptime(given_date,'%Y%m%d')
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

def find_expiry(start_date,end_date,symbol):
    #start_date = datetime.datetime.strptime('20160311','%Y%m%d')
    #end_date = datetime.datetime.strptime('20200728','%Y%m%d')
    day_name= ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday','Sunday']
    future_expiry = pd.read_excel('future_contracts.xlsx')
    future_list = future_expiry[symbol].to_list()
    last_count = 0
    count = 0
    expiries = []
    for i in range(len(future_list)-1):
        expiry_date = datetime.datetime.strptime(str(future_list[i]),'%Y%m%d')
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
        value = expiries[-1].replace('-','')
        if len(future_list) != future_list.index(int(value)):
            sub_string = str(future_list[future_list.index(int(value))+1])
            sub_string = sub_string[:4]+'-'+sub_string[4:6]+'-'+sub_string[6:]
            expiries.append(sub_string)
    return expiries

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

def calculate_days_of_first_expiry(start_date, end_date, check=None):
    if check == 'first':
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    if check == 'last':
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    result = int(np.busday_count(start_date.date(), end_date.date(), weekmask=[1,1,1,1,1,0,0], holidays=['2020-01-01']))
    return result

def generate_each_input(start_date, end_date, expiries):
    expiry_list = []
    for expiry in range(len(expiries)):
        if expiry == 0:
            first = calculate_days_of_first_expiry(start_date, expiries[expiry], check='first')
            first_out = str(expiries[expiry])+','+str(start_date.date())+','+str(first)
            expiry_list.append(first_out)
        elif expiry == (len(expiries)-1):
            last = calculate_days_of_first_expiry(expiries[expiry-1], end_date,check='last')
            last = last + 21 #increasing one month days
            last_out = str(expiries[expiry])+','+str(end_date.date())+','+str(last)
            expiry_list.append(last_out)
        else:
            all_other = 112
            all_other_out = str(expiries[expiry])+','+str(expiries[expiry])+','+str(all_other)
            expiry_list.append(all_other_out)
    with open('expiries_input1.csv', 'a') as file:
        line = "expiry,end_date,days"
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
            f_result['start_date'] =  start_date
        result = int(np.busday_count(f_result['start_date'],np_input_date.date(),weekmask=[1,1,1,1,1,0,0],holidays=['2020-01-01']))
        if 'result' not in f_result:
            f_result['result'] = result
        if f_result['result'] >= duration_value:
            return f_result['result'],f_result['start_date']
        else:
            f_result['result'] = result
            f_result['start_date'] = f_result['start_date'] - datetime.timedelta(days=1)
