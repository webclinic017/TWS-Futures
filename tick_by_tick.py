from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
import os
import pandas as pd

def contract_object(expiry):
    contract = Contract()
    contract.symbol = 'N225'
    contract.secType = 'FUT'
    contract.exchange = 'OSE.JPN'
    contract.currency = 'JPY'
    contract.multiplier = 1000
    contract.lastTradeDateOrContractMonth = expiry
    contract.includeExpired = True
    return contract

class TestApp(EWrapper, EClient):
    def __init__(self, host, port, clientId):
        EClient.__init__(self, self)
        self.connect(host, port, clientId)
        self.p_input = None
        self.reqCount = 0
        print('input prepartion started!!!')
        self.input_prep()
        print('input prepartion done!!!')
        print('input request triggering!!!')
        self.fut_request()
        print('input request triggered!!!')

    def fut_request(self):
        if not self.p_input.empty:
            date,expiry = self.input_pop()
            print(date,expiry)
            self.reqHistoricalTicks(1, contract_object(expiry),"", date+" 15:31:00", 1000, "TRADES", 1, True, [])
            self.reqCount += 1
        else:
            self.check_and_update()
    def error(self, reqId, errorCode, errorString):
        print('Error: ',reqId," ",errorCode,"Error String: ",errorString)

    def historicalTicksLast(self, reqId, ticks, done):
        from datetime import datetime
        import pytz
        for tick in ticks:
            e_time = datetime.utcfromtimestamp(tick.time).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Tokyo'))
            #print(datetime.utcfromtimestamp(tick.time).strftime('%Y-%m-%d %H:%M:%S'))
            print("HistoricalTick. ReqId:", reqId,'Time', e_time.strftime("%Y-%m-%d %H:%M:%S"), 'Price', tick.price, 'Size', tick.size)
            tick_by_tick(e_time.strftime("%Y-%m-%d %H:%M:%S"), tick.price, tick.size)
            #new_source['date'] = new_source['date'].dt.tz_localize("Asia/Dubai").dt.tz_convert('Asia/Tokyo').dt.tz_localize(None)
        self.reqCount -= 1
        self.fut_request()

    def input_pop(self):
        self.p_input, last_row = self.p_input.drop(self.p_input.tail(1).index), self.p_input.tail(1)
        last_row = last_row.astype(str)
        input_record = last_row.to_dict(orient='records')
        date = input_record[0]['date']
        expiry = input_record[0]['expiry']
        return date, expiry

    def check_and_update(self):
        pass

    def input_prep(self):
        all_files = os.listdir()
        all_files = [file for file in all_files if file.startswith('20') ]
        df = []
        for e_file in all_files:
            e_file = pd.read_csv(e_file)
            e_file[['date','time']] = e_file.date.str.split(expand=True)
            e_file = e_file[['expiry','date']]
            e_file.drop_duplicates(keep='last',inplace=True)
            e_file['date'] = pd.to_datetime(e_file['date'])
            e_file.set_index('date',inplace=True)
            e_file = e_file[e_file.index.dayofweek < 5]
            e_file.reset_index(inplace=True)
            e_file['date'] = e_file['date'].astype(str).apply(lambda x:x.replace('-',''))
            df.append(e_file)
        self.p_input = pd.concat(df)

        """for index, e_row in e_file.iterrows():
            date = e_row['date']
            expiry = e_row['expiry']
            self.req_send(date,expiry)"""

def tick_by_tick(time, stock_value, size):
    with open(str('tickbytick')+'.csv', 'a') as file:
            file.write(str(time)+','+str(stock_value)+','+str(size))
            file.write('\n')

def main():
    app = TestApp("127.0.0.1", 7497, 10)
    app.run()

if __name__ == '__main__':
    main()

from itertools import product

"""
1. End of day we getting two fold of volume.
2. start of the day tick data available for only on monday and cant find for rest of the weekdays.

"""
