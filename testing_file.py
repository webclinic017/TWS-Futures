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
    def __init__(self, host, port, clientId, expiry, date):
        EClient.__init__(self, self)
        self.connect(host, port, clientId)
        self.lexpiry = expiry
        self.idate = date
        print(1)
        self.fut_request()

    def fut_request(self):
        # self.reqHistoricalTicks(1, contract_object(self.lexpiry),"", self.idate+" 05:31:00 JST", 1000,
        #                         "TRADES", 1, True, [])
        print(2)
        self.reqHistoricalData(1,
                               contract_object(self.lexpiry),
                               "20200810 15:30:00",
                               "20 D",
                               "1 min",
                                "TRADES", 0, 1, False, [])
        print(3)

    def error(self, reqId, errorCode, errorString):
        print('Error: ',reqId," ",errorCode,"Error String: ",errorString)
        # self.fut_request()

    def historicalData(self, reqId, bar):
        print(f'ID: {reqId} | Bar: {bar}')

    def historicalTicksLast(self, reqId, ticks, done):
        print(f'Tick: {reqId} | {done}')
        print(f'Ticks: {ticks[0]}')
        exit()
        # from datetime import datetime
        # import pytz
        # for tick in ticks:
        #     e_time = datetime.utcfromtimestamp(tick.time).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Tokyo'))
        #     print("HistoricalTick. ReqId:", reqId,'Time', e_time.strftime("%Y-%m-%d %H:%M:%S"), 'Price', tick.price, 'Size', tick.size)
            #new_source['date'] = new_source['date'].dt.tz_localize("Asia/Dubai").dt.tz_convert('Asia/Tokyo').dt.tz_localize(None)

def main():
    app = TestApp("127.0.0.1", 7497, 10, '20201210', '20201019')
    app.run()

if __name__ == '__main__':
    main()
