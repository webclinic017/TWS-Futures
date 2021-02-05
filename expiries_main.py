from expiry_utility import *
import datetime
def date_calculate_to_expiry(input_date, duration, symbol):
    duration = duration.split()
    duration_value = int(duration[0])
    duration_period = duration[1]
    if duration_period == 'S':
        f_days = calc_min_day(duration_value)
        l_days = int(f_days)
        days = do_round_up(f_days,l_days)
        if days > 28:
            pass #need to handle months for inputs go beyond month
    elif duration_period == 'D':
        end_date = datetime.datetime.strptime(input_date,'%Y%m%d')
        _given_dates, start_date = find_end_date_without_weekend(duration_value, input_date)
        start_date = datetime.datetime.strptime(str(start_date),'%Y-%m-%d')
        end_date_constrains(start_date, end_date)
        expiries = find_expiry(start_date, end_date, symbol)
        generate_each_input(start_date, end_date, expiries)
    elif duration_period == 'W':
        duration_v = duration_value * 7
        end_date = datetime.datetime.strptime(input_date,'%Y%m%d')
        _given_dates, start_date = find_end_date_without_weekend(duration_v, input_date)
        start_date = datetime.datetime.strptime(str(start_date),'%Y-%m-%d')
        end_date_constrains(start_date, end_date)
        expiries = find_expiry(start_date, end_date, symbol)
        generate_each_input(start_date, end_date, expiries)

if __name__ == '__main__':
    print(date_calculate_to_expiry('20201012','100 W','N225'))
