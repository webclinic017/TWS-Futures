from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract
from utility import no_historical_data, write_csv, session_req_no
from logger import logging
import property as prop
import time
from load_tickers import load_tickers
import sys
import pandas as pd
from copy import deepcopy


def create_stock(tick):
    contract = Contract()
    contract.symbol = tick
    contract.secType = "STK"
    contract.exchange = "TSEJ"
    contract.currency = "JPY"
    contract.primaryExchange = 'TSEJ'
    return contract


class TestApp(EWrapper, EClient):
    def __init__(self, host, port, clientId, load_ticker):
        EClient.__init__(self, self)
        self.connect(host, port, clientId)
        print('API program starts now !!!')
        self.check_loader = deepcopy(load_ticker)  # [3841, 7426, 1795]
        self.loaded_ticker = load_ticker  # [3841, 7426, 1795]
        self.p_stocks = []
        self.diff_ip_address = False
        self.diff_ip_address_count = 0
        self.no_hist_data = []
        self.received_tickers = []
        self.file_write_count = 0
        self.csv_write_count = 0
        self.create_output_file()  # create historicaldatas.csv & nohistdata.csv
        self.first_request()

    def create_output_file(self):
        if not prop.file_exist:
            with open('historicaldatas' + '.csv', 'a') as file:
                line = "date,session,ecode,open,high,low,close,volume"
                file.write(line)
                file.write('\n')
        if not prop.hist_file_check:
            with open('nohistdata.csv','a') as file:
                line = "nohist"
                file.write(line)
                file.write('\n')

    def first_request(self):
        print('first request...')
        print(f'loaded tickers: {self.loaded_ticker}')
        if self.loaded_ticker and (len(self.loaded_ticker) >= 10):
            for i in range(10):
                tick = self.loaded_ticker.pop()
                self.reqHistoricalData(tick, create_stock(tick), "20200810 15:30:00", "365 D",
                                       "1 min", "TRADES", 0,
                                       1, False, [])
            prop.hist_data_req_count += 10
        else:
            for i in range(len(self.loaded_ticker)):
                tick = self.loaded_ticker.pop()
                self.reqHistoricalData(tick, create_stock(tick), "20200810 15:30:00", "365 D",
                                       "1 min", "TRADES", 0,
                                       1, False, [])
                prop.hist_data_req_count += 1

        if (len(self.loaded_ticker) < 10) and self.p_stocks:
            self.csv_write_count += 1
            write_csv(self.p_stocks)
            self.p_stocks.clear()
            logging.info('Data has been append to CSV, Append Count:{}'.format(self.csv_write_count))
            self.csv_write_count += 1

    def second_request(self):
        if self.loaded_ticker:
            prop.hist_data_req_count += 1
            tick = self.loaded_ticker.pop()
            self.reqHistoricalData(tick, create_stock(tick), "20200810 15:30:00", "365 D",
                                   "1 min", "TRADES", 0,
                                   1, False, [])
        else:
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
            self.csv_write_count += 1

    def diff_id_request(self, tick):
        self.reqHistoricalData(tick, create_stock(tick), "20200807 15:30:00", "1 D",
                               "30 mins", "TRADES", 0,
                               1, False, [])

    def waiting(self):
        time.sleep(60)
        self.second_request()

    def update_tickers_loaded_ticker(self):
        ticker_file = pd.read_csv('Tickers.csv')
        all_tickers = ticker_file['msymbol'].to_list()
        hist_file = pd.read_csv('historicaldatas.csv')
        hist_tickers = hist_file['ecode'].to_list()
        no_hist_tickers = pd.read_csv('nohistdata.csv')
        no_hist_tickers = no_hist_tickers['nohist'].to_list()
        if 'nohist' in no_hist_tickers:
            del no_hist_tickers[0]
        no_hist_tickers = (int(float(i)) for i in no_hist_tickers)
        processed_tickers = hist_tickers
        processed_tickers.sort()
        diff = set(all_tickers) - set(processed_tickers)
        diff = set(diff) - set(no_hist_tickers)
        diff = list(diff)
        if diff:
            diff.sort(reverse=True)
            self.loaded_ticker = diff
            self.check_loader = deepcopy(diff)
            self.received_tickers.clear()
            self.second_request()
        else:
            prop.done = True  # no difference tickers found.

    def security_define_failed(self):
        for key, value in prop.error_dict.items():
            if value == 2:
                self.received_tickers.append(key)
                no_historical_data(key, prop.no_hist_data, 'nohistdata')
                prop.no_hist_data += 1

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        logging.info('HistoricalDataEnd: Historical Data Received for Req Id: {}'.format(reqId))
        self.received_tickers.append(reqId)
        # self.cancelHistoricalData(reqId)
        prop.hist_data_req_count -= 1
        self.file_write_count += 1
        print('dataend', prop.hist_data_req_count)
        if (prop.hist_data_req_count < 20) and (not prop.done):
            if prop.done:
                pass
            elif len(self.loaded_ticker) <= 10:
                self.second_request()
            else:
                for i in range(10):
                    self.second_request()
                    self.second_request()
        else:
            pass

    def nextValidId(self, orderId: int):
        """Method of EWrapper.
        Sets the order_id class variable.
        This method is called from after connection completion, so
        provides an entry point into the class.
        """
        super().nextValidId(orderId)
        self.order_id = orderId
        # print(orderId)
        return self

    def error(self, reqId, errorCode, errorString):
        print('Error: ', reqId, " ", errorCode, "Error String: ", errorString)
        # prop.hist_data_req_count += 1
        if prop.hist_data_req_count < 40 and self.p_stocks:
            write_csv(self.p_stocks)
            self.p_stocks.clear()
            logging.info('Data has been append to CSV, Append Count:{}'.format(self.csv_write_count))
            self.csv_write_count += 1

        if reqId in self.received_tickers:
            pass

        else:
            if errorCode == 162:
                e_string = 'HMDS query returned no data:'
                if errorString == "Historical Market Data Service error message:Trading TWS session is connected from a different IP address":
                    time.sleep(10)
                    self.cancelHistoricalData(reqId)
                    self.diff_ip_address = True
                    self.diff_id_request(reqId)
                    self.diff_ip_address_count += 1
                    if self.diff_ip_address_count >= 10:
                        logging.warning('API connected from different IP Address Req Id: {}'.format(reqId))
                        self.waiting()
                if e_string in errorString:
                    prop.hist_data_req_count -= 1
                    self.received_tickers.append(reqId)
                    no_historical_data(reqId, prop.no_hist_data, 'nohistdata')
                    prop.no_hist_data += 1
                    self.second_request()
                else:
                    logging.info('Difference scenario need to see Req Id: {} ErrorString: {}'.format(reqId, errorString))
                    prop.hist_data_req_count -= 1
                    print(prop.hist_data_req_count)
                    self.cancelHistoricalData(reqId)
                    if (prop.hist_data_req_count < 40) and (not prop.done) and self.p_stocks:
                        self.second_request()

            elif errorCode == 366:
                # self.no_hist_list_data.append(reqId)
                # no_historical_data(reqId,prop.no_hist_data,'nohistdata') #writting tickers of no hist data
                # prop.no_hist_data += 1 #file write purpose
                prop.hist_data_req_count -= 1
                if (prop.hist_data_req_count < 40) and (not prop.done) and self.loaded_ticker:
                    self.second_request()  # No historical data found
            elif errorCode == 322:
                prop.hist_data_req_count -= 1
                print('50 Exceed reqId', reqId)
                self.cancelHistoricalData(reqId)
                print(prop.hist_data_req_count)
                logging.warning('More than 50 request to API Req Id:{}'.format(reqId))
                if (prop.hist_data_req_count < 40) and (not prop.done) and self.loaded_ticker:
                    self.second_request()
            elif errorCode == 100:
                prop.hist_data_req_count -= 1
                if (prop.hist_data_req_count < 40) and (not prop.done) and self.loaded_ticker:
                    self.second_request()
            elif errorCode == 504:  # No Connection
                prop.hist_data_req_count -= 1
                if (prop.hist_data_req_count < 40) and (not prop.done) and self.loaded_ticker:
                    self.second_request()
                self.cancelHistoricalData(reqId)
            elif errorCode == -1:
                prop.hist_data_req_count -= 1
                if (prop.hist_data_req_count < 40) and (not prop.done) and self.loaded_ticker:
                    self.second_request()
            elif errorCode == 200:  # security details missing on historical data
                prop.hist_data_req_count -= 1
                err_val = 'No security definition has been found'
                if err_val in errorString:
                    print(err_val)
                    if reqId in prop.error_dict:
                        prop.error_dict[reqId] += 1
                        if prop.error_dict[reqId] >= 3:
                            self.received_tickers.append(reqId)
                    else:
                        prop.error_dict[reqId] = 1
                        self.diff_id_request(reqId)
                else:
                    print('Unhandled exception from 200', reqId)

                if (prop.hist_data_req_count < 40) and (not prop.done) and self.loaded_ticker:
                    self.second_request()
            elif (errorCode == 2104) and (errorCode == 2106) and (errorCode == 2158):  # connected message
                print('2104 errors', prop.hist_data_req_count)
                prop.hist_data_req_count -= 1
                print(prop.hist_data_req_count)
                if (prop.hist_data_req_count < 40) and (not prop.done) and self.loaded_ticker:
                    self.second_request()
            elif prop.hist_data_req_count > 40 and (not prop.done) and self.loaded_ticker:
                time.sleep(10)
                self.second_request()
            else:
                print('exception not captured yet')
                self.second_request()

        """if errorCode == 322:
            self.disconnect()
            time.sleep(10)"""
        """    self.reqHistoricalData(reqId, contract, "", "1 D", "1 min", "TRADES", 0, 1, False, [])
        elif errorCode == 100:
            time.sleep(1)"""

    def historicalData(self, reqId, bar):
        print('Date: ', bar.date, 'HistoricalData', reqId, 'Open: ', bar.open, 'High: ', bar.high, 'Low: ', bar.low,
              'Close: ', bar.close,
              'Volume: ', bar.volume, 'Count: ')
        session, reqno = session_req_no(bar)
        # load_data_to_csv(reqId, bar, vcount = prop.histdata, fname = 'historicaldatas') #fname can be date and time, now manual name used
        e_stock = [str(bar.date), str(session), str(reqId), str(bar.open), str(bar.high), str(bar.low), str(bar.close),
                   str(bar.volume)]
        self.p_stocks.append(e_stock)

        prop.histdata += 1  # file write purpose
        if self.diff_ip_address:
            self.diff_ip_address = False
        """if reqId not in prop.ctickers:
            prop.ctickers.append(reqId)
            if len(prop.ctickers) == 50:
                time.sleep(15)
                if len(prop.tickers) > 50:
                    prop.tickers = prop.tickers[50:]
                    main()"""


def main():
    # load_tickers = load_tickers('future_input.csv')
    # print(f'Result Type: {type(results)}')
    # print(f'Results: {results}')
    # exit()
    app = TestApp("127.0.0.1", 7497, 10, load_tickers('future_input.csv'))
    """lrange = prop.stop - prop.start
    for tick in range(lrange):
        app.reqHistoricalData(prop.tickers[prop.eachticker], create_stock(prop.tickers[prop.eachticker]), "", "1 D",
                               "1 min", "TRADES", 0,
                               1, True, [])
        prop.eachticker += 1"""

    """for tick in range(len(prop.tickers)):
        print(tick)
        app.reqHistoricalData(prop.tickers[tick], create_stock(prop.tickers[tick]), "", "1 D", "1 min", "TRADES", 0,
                               1, False, [])
    if len(prop.tickers) > 50:
        prop.tickers = prop.tickers[50:]
        print('Done!!!!!')"""
    app.run()
    app.disconnect()


if __name__ == '__main__':
    try:
        sys.setrecursionlimit(10000000)
        while True:
            main()
            if prop.connection:
                break
        print('API request and response has been done!!!!')
    except KeyboardInterrupt:
        # do nothing here
        pass

"""list1 = []
    con = True
    count = 1
    length_check = len(prop.tickers) / 50
    length = int(len(prop.tickers) / 50)
    if length_check > length:
        diff = length_check - length
        actual_diff = diff * 50
        length += 1
    if count == 1:
            prop.stop += 50
            subtickers = prop.tickers[prop.start:prop.stop]
            main()
            count += 1
            list1.append('one')
        if count == length:
            prop.start += 50
            prop.stop += len(prop.tickers)-prop.start
            subtickers = prop.tickers[prop.start:prop.stop]
            main()
            count += 1
            if length <= count:
                list1.append('three')
                con = False
                print('All Process done!!!!!!')
                print('List is - ',list1)
                print(prop.eachticker)

        else:
            print('counting happening',count)
            prop.start += 50
            prop.stop += 50
            subtickers = prop.tickers[prop.start:prop.stop]
            main()
            if length <= count:
                con = False
                print('Process done!!!!!!')
                print('List is - ',list1)
            count += 1
            list1.append('two')"""

# HMDS query returned no data: 5962.T@TSEJ Trades, put in list and then go for all exception, handle in first if
