import os
import time
from utility import *


def calc_min_day(seconds):
    days = seconds/86400
    return days


def date_calculate_to_expiry(input_date, duration, minbar, symbol):
    no_file_check = os.path.isfile('expiries_input1.csv')
    if no_file_check:
        os.remove('expiries_input1.csv')
    duration = duration.split()
    duration_value = int(duration[0])
    duration_period = duration[1]
    if duration_period == 'S':
        f_days = calc_min_day(duration_value)
        l_days = int(f_days)
        days = do_round_up(f_days,l_days)
        if days > 28:
            pass  # need to handle months for inputs go beyond month
    elif duration_period == 'D':
        end_date = datetime.datetime.strptime(input_date, '%Y%m%d')
        _given_dates, start_date = find_end_date_without_weekend(duration_value, input_date)
        start_date = datetime.datetime.strptime(str(start_date),'%Y-%m-%d')
        print(f'Start: {start_date} | End: {end_date}')
        end_date_constrains(start_date, end_date)
        expiries = find_expiry(start_date, end_date, symbol)
        generate_each_input(symbol, start_date, end_date, minbar, expiries)
    elif duration_period == 'W':
        duration_v = duration_value * 7
        end_date = datetime.datetime.strptime(input_date,'%Y%m%d')
        _given_dates, start_date = find_end_date_without_weekend(duration_v, input_date)
        start_date = datetime.datetime.strptime(str(start_date),'%Y-%m-%d')
        end_date_constrains(start_date, end_date)
        expiries = find_expiry(start_date, end_date, symbol)
        generate_each_input(symbol, start_date, end_date,minbar, expiries)


def load_tickers(fname):
    print('Loading input files!!!')
    ticker_file = pd.read_csv(fname)
    all_tickers = ticker_file['symbol'].to_list()
    file_check = os.path.isfile('./historicaldatas.csv')
    no_file_check = os.path.isfile('./nohistdata.csv')
    if no_file_check:
        prop.hist_file_check = True
    if file_check:
        prop.file_exist = True
        hist_file = pd.read_csv('historicaldatas.csv')
        tickers = hist_file['ecode'].unique()
        processed_tickers = list(tickers)
        processed_tickers.sort()
        diff = set(all_tickers)-set(processed_tickers) #no historical data too should removed from diff tickers
        no_hist_file = set()
        if no_file_check:
            no_hist_file = pd.read_csv('nohistdata.csv')
            no_hist_file_list = no_hist_file['nohist'].unique()
            no_hist_file_list = list(no_hist_file_list)
            if 'nohist' in no_hist_file_list:
                del no_hist_file_list[0]
            no_hist_file_list = (int(float(i)) for i in no_hist_file_list)
        diff = set(diff) - set(no_hist_file_list)
        diff = list(diff)
        if diff:
            diff.sort(reverse=True)
            return diff
        else:
            print('Historical process has been done and moving old file to different folder!')
            present_dir = os.getcwd()
            dest_dir = present_dir[:-12]
            source_file = os.path.join(present_dir,'historicaldatas.csv')
            first_path_time = time.strftime("%Y%m%d")
            dest_path_exist = os.path.join(dest_dir, 'Historicaldata-' + first_path_time)
            if not os.path.exists(dest_path_exist):
                os.makedirs(dest_path_exist)
            time_now = time.strftime("%H-%M-%S")
            dest_path_exist1 = os.path.join(dest_path_exist, time_now)
            if not os.path.exists(dest_path_exist1):
                os.makedirs(dest_path_exist1)
            dest_dir = os.path.join(dest_path_exist1,'historicaldatas.csv')
            hist_file.sort_values(by=['ecode','date'],inplace=True)
            hist_file.drop_duplicates(inplace=True)
            hist_file.to_csv('historicaldatas.csv',header=False, index=False)
            os.rename(source_file, dest_dir)
            if no_file_check:
                no_hist_source_file = os.path.join(present_dir,'nohistdata.csv')
                no_hist_dest_dir = os.path.join(dest_path_exist1,'nohistdata.csv')
                no_hist_file.sort_values(by=['nohist'],inplace=True)
                no_hist_file.drop_duplicates(inplace=True)
                no_hist_file.to_csv('nohistdata.csv',header=False, index=False)
                os.rename(no_hist_source_file, no_hist_dest_dir)
            print('File has been moved to different folder!!!')
            print('Continue with given input')
            all_tickers.sort(reverse=True)
            return all_tickers
    else:
        all_tickers.sort(reverse=True)
        return all_tickers


def load_expiry_inputs(fname, input_date, duration, minbar, symbol):
    date_calculate_to_expiry(input_date, duration, minbar, symbol)
    # print('Expiries Generated!!!')
    # print('')
    # print('Combining expiries with relevant contracts!!!')
    read_fut_contract = pd.read_csv(fname)
    read_expiry = pd.read_csv('expiries_input.csv')
    expiry_output = read_expiry.merge(read_fut_contract, how='inner')
    print('Input file feeding to API')
    return [symbol, '_', input_date, '_', duration, 'ay_', minbar], expiry_output


if __name__ == "__main__":
    from json import dumps
    res = load_tickers('future_input.csv')
    print(f'Type: {type(res)}')
    print(dumps(res, indent=1, sort_keys=True))

    # print(date_calculate_to_expiry('20201012','75 D','15 mins','N225'))
