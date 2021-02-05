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
        self.fut_request()

    def fut_request(self):
        self.reqHistoricalTicks(1, contract_object(self.lexpiry),"", self.idate+" 05:31:00 JST", 1000, "TRADES", 1, True, [])
    def error(self, reqId, errorCode, errorString):
        print('Error: ',reqId," ",errorCode,"Error String: ",errorString)

    def historicalTicksLast(self, reqId, ticks, done):
        from datetime import datetime
        import pytz
        for tick in ticks:
            e_time = datetime.utcfromtimestamp(tick.time).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Tokyo'))
            #print(datetime.utcfromtimestamp(tick.time).strftime('%Y-%m-%d %H:%M:%S'))
            print("HistoricalTick. ReqId:", reqId,'Time', e_time.strftime("%Y-%m-%d %H:%M:%S"), 'Price', tick.price, 'Size', tick.size)
            #new_source['date'] = new_source['date'].dt.tz_localize("Asia/Dubai").dt.tz_convert('Asia/Tokyo').dt.tz_localize(None)

def main():
    app = TestApp("127.0.0.1", 7497, 10, '20201210', '20201019')
    app.run()

if __name__ == '__main__':
    main()
