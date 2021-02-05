from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from datetime import datetime
import pytz
import math
from utility import no_historical_data, write_csv, session_req_no,load_datas_to_csv,tick_by_tick
from logger import logging
import property as prop
import time
from load_tickers import load_expiry_inputs
import sys
import pandas as pd
from copy import deepcopy
import numpy as np
import os

def contract_tick_object(expiry):
    contract = Contract()
    contract.symbol = 'N225'
    contract.secType = 'FUT'
    contract.exchange = 'OSE.JPN'
    contract.currency = 'JPY'
    contract.multiplier = 1000
    contract.lastTradeDateOrContractMonth = expiry
    contract.includeExpired = True
    return contract

def contract_object(input_record):
    #symbol,sectype,exchange,currency,multiplier,localsymbol,conid,lasttradedateorcontractmonth,strike,right,primaryexchange,tradingclass,includeexpired,secidtype,secid
    contract = Contract()
    """contract.symbol = 'N225'
    contract.secType = 'FUT'
    contract.exchange = 'OSE.JPN'
    contract.currency = 'JPY'
    contract.multiplier = 1000
    contract.lastTradeDat
    eOrContractMonth = 20200910
    contract.includeExpired =  True
    """
    contract.symbol = input_record['symbol'] if ('symbol' in input_record.keys()) and (input_record['symbol'] != 'nan') else ""
    contract.secType = input_record['sectype'] if ('sectype' in input_record.keys()) and (input_record['sectype'] != 'nan') else ""
    contract.exchange = input_record['exchange'] if ('exchange' in input_record.keys()) and (input_record['exchange'] != 'nan') else ""
    contract.currency = input_record['currency'] if ('currency' in input_record.keys()) and (input_record['currency'] != 'nan') else ""
    contract.multiplier = int(input_record['multiplier']) if ('multiplier' in input_record.keys()) and (input_record['multiplier'] != 'nan') else ""
    #contract.localSymbol = input_record['localsymbol'] if ('localsymbol' in input_record.keys()) and (input_record['localsymbol'] != 'nan') else ""
    #contract.conId = input_record['conid'] if ('conid' in input_record.keys()) and (input_record['conid'] != 'nan') else ""
    contract.lastTradeDateOrContractMonth = int(input_record['lasttradedateorcontractmonth']) if ('lasttradedateorcontractmonth' in input_record.keys()) and (input_record['lasttradedateorcontractmonth'] != 'nan') else ""
    #contract.strike = input_record['strike'] if ('strike' in input_record.keys()) and (input_record['strike'] != 'nan') else 0.0 #Float
    #contract.right = input_record['right'] if ('right' in input_record.keys()) and (input_record['right'] != 'nan') else ""
    #contract.primaryExchange = input_record['primaryexchange'] if ('primaryexchange' in input_record.keys()) and (input_record['primaryexchange'] != 'nan') else "" # pick an actual (ie non-aggregate) exchange that the contract trades on.  DO NOT SET TO SMART.
    #contract.tradingClass = input_record['tradingclass'] if ('tradingclass' in input_record.keys()) and (input_record['tradingclass'] != 'nan') else ""
    contract.includeExpired = bool(input_record['includeexpired']) if ('includeexpired' in input_record.keys()) and (input_record['includeexpired'] != 'nan') else False #Boolean
    #contract.secIdType = input_record['secidtype'] if ('secidtype' in input_record.keys()) and (input_record['secidtype'] != 'nan') else ""  #CUSIP;SEDOL;ISIN;RIC
    #contract.secId = input_record['secid'] if ('secid' in input_record.keys()) and (input_record['secid'] != 'nan') else ""
    #print(contract.secId)
    return contract

class TestApp(EWrapper, EClient):
    def __init__(self, host, port, clientId, inputs):
        EClient.__init__(self, self)
        self.connect(host, port, clientId)
        print('API program starts now !!!')
        self.file_name, load_ticker = inputs
        self.file_name = ''.join(map(str,self.file_name))
        self.check_loader = deepcopy(load_ticker)#[3841, 7426, 1795, 7175, 7559, 7689, 9355, 4875, 4621, 7435, 8208, 4496, 4497, 4624, 5395, 7446, 8729, 5918, 3490, 7460, 2986, 4781, 6065, 7857, 8885, 7351, 7873, 6599, 2761, 5962, 4685, 9679, 5969, 5971, 4052, 7891, 6998, 9049, 3802, 7515, 7901, 9311, 3424, 7265, 3171, 3686, 5355, 9967, 9073, 7413, 6518, 9976, 3578]
        self.loaded_ticker = load_ticker #[3841, 7426, 1795, 7175, 7559, 7689, 9355, 4875, 4621, 7435, 8208, 4496, 4497, 4624, 5395, 7446, 8729, 5918, 3490, 7460, 2986, 4781, 6065, 7857, 8885, 7351, 7873, 6599, 2761, 5962, 4685, 9679, 5969, 5971, 4052, 7891, 6998, 9049, 3802, 7515, 7901, 9311, 3424, 7265, 3171, 3686, 5355, 9967, 9073, 7413, 6518, 9976, 3578]
        self.p_stocks = []
        self.diff_ip_address = False
        self.diff_ip_address_count = 0
        self.no_hist_data = []
        self.received_tickers = []
        self.file_write_count = 0
        self.csv_write_count = 0
        self.req_res = [] #how many requested and received count balance
        self.no_of_expiry = self.calculate_expiry_len()
        #self.create_output_file()
        self.p_input = None
        self.reqCount = 0
        self.last_tick_update = 0
        self.first_request()
        self.create_file()

    def create_file(self):
        no_file_check = os.path.isfile('tickbytick.csv')
        if not no_file_check:
            with open('tickbytick.csv', 'a') as file:
                line = "expiry,date,price,volume"
                file.write(line)
                file.write('\n')

    def input_pop(self):
        self.p_input, last_row = self.p_input.drop(self.p_input.tail(1).index), self.p_input.tail(1)
        last_row = last_row.astype(str)
        input_record = last_row.to_dict(orient='records')
        date = input_record[0]['date']
        expiry = input_record[0]['expiry']
        #print('popping',date,expiry)
        #print(self.p_input)
        return date, expiry

    def fut_request(self):
        if not self.p_input.empty:
            date,expiry = self.input_pop()
            self.reqHistoricalTicks(expiry, contract_tick_object(expiry),"", date+" 15:16:00", 1000, "TRADES", 1, True, [])
            self.reqCount += 1
        else:
            #need to feed again the data when fails
            print('tick by tick data adding has been begin')
            if self.last_tick_update == 0:
                self.check_and_filter()
                self.last_tick_update += 1

    def check_and_filter(self):
        last_bar = pd.read_csv('tickbytick.csv')
        last_bar['barcount'] =  1
        last_bar['date'] = pd.to_datetime(last_bar['date'])
        last_bar['date'] = last_bar['date'].dt.floor('Min')
        last_bar = last_bar.groupby(['expiry','date']).sum()
        last_bar.reset_index(['expiry','date'],inplace=True)
        last_bar['date'] = last_bar['date'].apply(lambda x: str(x))
        last_bar = last_bar[last_bar['date'].str.endswith('15:14:00')]
        last_bar['price'] = last_bar['price']/last_bar['barcount']
        last_bar['volume'] = last_bar['volume']/2
        last_bar['volume'] = last_bar['volume'].astype(int)
        last_bar['open'] = last_bar['price']
        last_bar['high'] = last_bar['price']
        last_bar['low'] = last_bar['price']
        last_bar['close'] = last_bar['price']
        del last_bar['price']
        last_bar = last_bar[['expiry','date','open','high','low','close','volume','barcount']]
        last_bar[['date','time']] = last_bar['date'].str.split(' ',expand=True)
        last_bar['time'] = '15:15:00'
        last_bar['date'] = last_bar.apply(lambda x: x.date+' '+x.time,axis=1)
        last_bar['date'] = last_bar['date'].str.replace('-','')
        del last_bar['time']
        last_bar.reset_index(drop=True,inplace=True)
        self.tick_append(last_bar)

    def tick_append(self,last_bar):
        all_files = os.listdir()
        all_files = [file for file in all_files if file.startswith('combined_')]
        c_file = pd.read_csv(all_files[0])
        c_file = c_file.append(last_bar,ignore_index=True)
        c_file['date'] = pd.to_datetime(c_file['date'])
        c_file.sort_values(by=['expiry','date'],inplace=True)
        c_file.reset_index(drop=True,inplace=True)
        c_file.to_csv('final_'+all_files[0]+'.csv')
        print('tick by tick adding :done!')

    def input_prep(self):
        all_files = os.listdir()
        all_files = [file for file in all_files if file.startswith('20') ]
        df = []
        for e_file in all_files:
            e_file = pd.read_csv(e_file)
            e_file[['date','time']] = e_file.date.str.split(expand=True)
            e_file = e_file[['expiry','date']]
            e_file.drop_duplicates(keep='last',inplace=True)
            #print('one',e_file)
            e_file['date'] = pd.to_datetime(e_file['date'])
            e_file.set_index('date',inplace=True)
            e_file = e_file[e_file.index.dayofweek < 5]
            e_file.reset_index(inplace=True)
            e_file['date'] = e_file['date'].astype(str).apply(lambda x:x.replace('-',''))
            df.append(e_file)
        self.p_input = pd.concat(df)
        self.p_input.reset_index(drop=True,inplace=True)
        print(self.p_input)

    def historicalTicksLast(self, reqId, ticks, done):
        for tick in ticks:
            e_time = datetime.utcfromtimestamp(tick.time).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Tokyo'))
            #print(datetime.utcfromtimestamp(tick.time).strftime('%Y-%m-%d %H:%M:%S'))
            print("HistoricalTick. ReqId:", reqId,'Time', e_time.strftime("%Y-%m-%d %H:%M:%S"), 'Price', tick.price, 'Size', tick.size)
            tick_by_tick(reqId, e_time.strftime("%Y-%m-%d %H:%M:%S"), tick.price, tick.size)
            #new_source['date'] = new_source['date'].dt.tz_localize("Asia/Dubai").dt.tz_convert('Asia/Tokyo').dt.tz_localize(None)
        self.reqCount -= 1
        self.fut_request()

    def calculate_expiry_len(self):
        expiry_f = pd.read_csv('expiries_input.csv')
        return len(expiry_f.index)

    def do_combine_output_csv(self):
        out_put_file = [file for file in os.listdir() if file.startswith('20')]
        combined_file = []
        for each_file in out_put_file:
            e_file = pd.read_csv(each_file)
            combined_file.append(e_file)
        final_out_file = pd.concat(combined_file)
        final_out_file.to_csv('combined_'+self.file_name+'.csv',index=False)
        print('Expiries has been combined!!!')

    def df_pop(self):
        self.loaded_ticker, last_row = self.loaded_ticker.drop(self.loaded_ticker.tail(1).index), self.loaded_ticker.tail(1)
        last_row = last_row.astype(str)
        input_record = last_row.to_dict(orient='records')
        dict_input_record = input_record[0]
        tick = input_record[0]['lasttradedateorcontractmonth']
        days = input_record[0]['days']
        input_date = input_record[0]['end_date']
        min_bar = input_record[0]['minbar']
        return tick, days, input_date, min_bar, dict_input_record

    def first_request(self):
        if self.loaded_ticker.empty and (len(self.loaded_ticker) >= 10):
            for i in range(10):
                tick, days, input_date, min_bar, input_record = self.df_pop()
                print(tick,days,input_date,min_bar,input_record)
                self.reqHistoricalData(tick, contract_object(input_record), str(input_date)+" 15:16:00", str(days)+" D",
                                       min_bar, "TRADES", 0,
                                       1, False, [])
            prop.hist_data_req_count += 10
        else:
            for i in range(len(self.loaded_ticker)):
                tick, days, input_date, min_bar, input_record = self.df_pop()
                self.reqHistoricalData(tick, contract_object(input_record), str(input_date)+" 15:16:00", str(days)+" D",
                                       min_bar, "TRADES", 0,
                                       1, False, [])
                prop.hist_data_req_count += 1

        """if (len(self.loaded_ticker) < 10) and self.p_stocks:
            self.csv_write_count += 1
            write_csv(self.p_stocks)
            self.p_stocks.clear()
            logging.info('Data has been append to CSV, Append Count:{}'.format(self.csv_write_count))
            self.csv_write_count += 1"""

    def second_request(self):
        if self.loaded_ticker.empty:
            prop.hist_data_req_count += 130
            tick, days, input_date, min_bar, input_record = self.df_pop()
            self.reqHistoricalData(tick, contract_object(input_record), str(input_date)+" 16:30:00", str(days)+" D",
                                       min_bar, "TRADES", 0,
                                       1, False, [])
        """else:
            prop.hist_data_req_count -= 1
            self.security_define_failed()
            if (prop.hist_data_req_count < 40) and (not prop.done):
                write_csv(self.p_stocks)
                self.p_stocks.clear()
                logging.info('Data has been append to CSV, Append Count:{}'.format(self.csv_write_count))
                self.csv_write_count += 1
            if not self.loaded_ticker:
                prop.done = True
                self.update_tickers_loaded_ticker()
            if len(self.check_loader) == len(set(self.received_tickers)):
                prop.connection = False
                print('Historical Process request has been done!!!!')
                prop.done = True
                time.sleep(20)
                return
        if self.file_write_count >= 50:
            write_csv(self.p_stocks)
            self.p_stocks.clear()
            logging.info('Data has been append to CSV, Append Count:{}'.format(self.csv_write_count))
            self.csv_write_count += 1"""

    def error(self, reqId, errorCode, errorString):
        print('Error: ',reqId," ",errorCode,"Error String: ",errorString)

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        if reqId not in self.req_res:
            self.req_res.append(reqId)
        if len(self.req_res) == self.no_of_expiry:
            self.do_combine_output_csv()
            self.input_prep()
            self.fut_request()

    def historicalData(self, reqId, bar):
        print('HistoricalData', reqId, 'Date: ', bar.date, 'Open: ', bar.open, 'High: ', bar.high, 'Low: ', bar.low, 'Close: ', bar.close, 'Volume: ', bar.volume, 'Count: ', bar.barCount)
        #load_datas_to_csv(self.input_date, bar)
        load_datas_to_csv(reqId, bar)

    #with open('historicaldata.csv','wb') as file:
    #    file.write(str(reqId, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average))
    #    file.write('\n')

def main():
    app = TestApp("127.0.0.1", 7497, 10, load_expiry_inputs('future_input.csv','20201125','1 D','1 min','N225'))
    app.run()

if __name__ == '__main__':
    main()
