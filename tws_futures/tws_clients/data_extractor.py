#! TWS-Futures/venv/bin/python3.9
# -*- coding: utf-8 -*-

"""
    Historical data extractor for futures, written around TWS API(reqHistoricalData)
"""

from tws_futures.tws_clients import TWSWrapper
from tws_futures.tws_clients import TWSClient
from tws_futures.tws_clients import create_futures_contract
from os import name as os_name
import logging
import signal


logging.basicConfig()
_logger = logging.getLogger(__name__)
OS_IS_UNIX = os_name == 'posix'


class HistoricalDataExtractor(TWSWrapper, TWSClient):

    MARKET_END_TIME = '15:16:00'

    def __init__(self, duration=1, bar_size='1 min',
                 what_to_show='TRADES', use_rth=0, date_format=1,
                 keep_upto_date=False, chart_options=(), logger=None,
                 timeout=30, max_attempts=3, debug=False):
        TWSWrapper.__init__(self)
        TWSClient.__init__(self, wrapper=self)
        self.is_connected = False
        # TODO: implement broken connection handler
        self.connection_is_broken = False
        self.handshake_completed = False
        # self.end_date = end_date
        # self.end_time = end_time
        self.duration = duration
        self.bar_size = bar_size
        self.what_to_show = what_to_show
        self.use_rth = use_rth
        self.date_format = date_format
        self.keep_upto_date = keep_upto_date
        self.chart_options = chart_options
        self.timeout = timeout
        self.logger = logger or _logger
        self.max_attempts = max_attempts
        self.debug = debug
        self.data = None
        self.expiry_input = None
        self.symbol = None
        self.end_date = None
        self.expiry = None
        if self.debug:
            self.logger.setLevel(logging.DEBUG)
        self.logger.info(f'Initialised extractor object')

    def _init_data_tracker(self):
        """
            Initializes the data tracker.
            Should be invoked for every new ticker.
        """
        _meta_data = self.expiry_input
        _meta_data['_error_stack'] = []
        _meta_data['total_bars'] = 0
        _meta_data['attempts'] = 0
        _meta_data['status'] = False
        _initial_data = {'meta_data': _meta_data, 'bar_data': []}
        self.data = _initial_data
        self.logger.info(f'Initialized data tracker for symbol: '
                         f'{self.symbol}')

    def _reset_attrs(self, **kwargs):
        """
            Resets the value of the given attributes
            :param kwargs: keyword argument to reset the attribute
        """
        for attr, value in kwargs.items():
            if value is None:
                raise ValueError(f'Attribute {attr} can not be None.')
            setattr(self, attr, value)
            self.logger.debug(f'Attribute: {attr} was reset to: {value}')
        self.logger.info('Extractor was reset')

    def _set_timeout(self, symbol):
        """
            Break the call running longer than timeout threshold.
            Call error method with code=-1 on timeout.
            NOTE: Not supported on Windows OS yet.
        """
        # noinspection PyUnusedLocal
        def _handle_timeout(signum, frame):
            try:
                _message = f'Data request for symbol: {symbol} timed out after:' \
                           f' {self.timeout} seconds'
                self.error(symbol, -1, _message)
            except OSError as e:  # TODO: make it work for Windows OS
                self.logger.error(f'{e}')

        # TODO: final alarm
        # if OS_IS_UNIX:
        signal.signal(signal.SIGALRM, _handle_timeout)
        signal.alarm(self.timeout)

    def _request_historical_data(self):
        """
            Sends request to TWS API
        """
        self.logger.info(f'Requesting historical data for symbol: {self.symbol}')
        try:
            # if OS_IS_UNIX:
            #     self._set_timeout(self.symbol)
            contract = create_futures_contract(self.expiry_input)
            end_date_time = f'{self.end_date} {HistoricalDataExtractor.MARKET_END_TIME}'
            duration = str(self.duration) + ' D'
            self.data['meta_data']['attempts'] += 1
            self.logger.debug(f'Request parameters: {contract}')
            self.reqHistoricalData(self.expiry, contract, end_date_time, duration,
                                   self.bar_size, self.what_to_show, self.use_rth,
                                   self.date_format, self.keep_upto_date,
                                   self.chart_options)
        except OSError as e:  # TODO: temporary piece, find a fix for Windows
            self.logger.critical(f'{e}')

    def _extraction_check(self):
        """
            Returns True if bar data has been fully extracted
        """
        self.logger.info(f'Running extraction check for symbol: {self.symbol}')
        meta_data = self.data['meta_data']
        status = meta_data['status']
        max_attempts_reached = meta_data['attempts'] >= self.max_attempts
        status_ = status or max_attempts_reached
        self.logger.debug(f'Symbol: {self.symbol} | Extraction Status: {status_}')
        return status_

    def connect(self, host='127.0.0.1', port=7497, client_id=10):
        """
            Establishes a connection to TWS API
            Sets 'is_connected' to True after a successful connection
            :param host: IP Address of the machine hosting the TWS app
            :param port: Port number on which TWS is listening to new connections
            :param client_id: Client ID using which connection will be made
        """
        self.logger.info('Connecting to TWS API server')
        if not self.is_connected:
            super().connect(host, port, client_id)
            self.is_connected = self.isConnected()
            self.logger.debug(f'Connection status: {self.is_connected}')
            self.run()

    def disconnect(self):
        """
            Disconnects the client from TWS API
        """
        self.logger.info('Extraction completed, terminating main loop')
        self._reset_attrs(is_connected=False, handshake_completed=False)
        super().disconnect()

    def run(self):
        """
            Triggers the infinite message loop defined within parent class(EClient):
                - Completes the initial handshake
                - Triggers data extraction from error method
            Note: User must connect to TWS API prior to calling this method.
        """
        self.logger.info('Initiating main loop')
        if not self.is_connected:
            raise ConnectionError(f'Not connected to TWS API.')
        super().run()

    def extract_historical_data(self, expiry_input=None):
        """
            Performs historical data extraction on tickers provided as input.
        """
        if expiry_input is not None:
            self.expiry_input = expiry_input
            self._reset_attrs(**expiry_input)
            self._init_data_tracker()
        if not self.is_connected:
            self.connect()
        if not self._extraction_check():
            self._request_historical_data()
        else:
            self.disconnect()

    def historicalData(self, expiry, bar):
        """
            This method receives data from TWS API,
            invoked automatically after "reqHistoricalData".
            :param expiry: represents contract expiry
            :param bar: a bar object that contains OHLCV data
        """
        end_date, end_time = bar.date.split()
        data = dict(end_date=end_date, end_time=end_time, open=bar.open,
                    high=bar.high, low=bar.low, close=bar.close,
                    volume=bar.volume, average=bar.average,
                    bar_count=bar.barCount)
        self.data['bar_data'].append(data)
        self.logger.info(f'Bar-data received for expiry: {expiry}')
        self.logger.debug(f'Bar: {bar}')

    def historicalDataEnd(self, expiry, start, end):
        """
            This method is called automatically after all the bars have been received
            Marks the completion of historical data
            :param expiry: contract expiry date
            :param start: starting timestamp
            :param end: ending timestamp
        """
        self.logger.info(f'Data extraction completed for expiry: {expiry}')
        self.data['meta_data']['start'] = start
        self.data['meta_data']['end'] = end
        self.data['meta_data']['status'] = True
        self.data['meta_data']['total_bars'] = len(self.data['bar_data'])
        self.extract_historical_data()

    def error(self, id_, code, message):
        """
            Error handler for all API calls, invoked directly by EClient methods
            :param id_: error ID
            :param code: error code, defines error type
            :param message: error message, information about error
        """
        # -1 is not a true error, but only an informational message
        # initial call to run invokes this method with error ID = -1
        if id == -1:
            self.logger.info(f'Market data connection initiated')
            if code in [502, 2103, 2105, 2157]:
                self.logger.critical(f'Connection issue: {message}')
                if self.debug:
                    raise ConnectionError(f'Can\'t connect to TWS API.')
        if code == 2158:  # marks the completetion of connection handshake
            self.extract_historical_data()


if __name__ == '__main__':
    expiry_input = {
                        "symbol": "N225",
                        "expiry": "20210909",
                        "end_date": "20210901",
                        "duration": 1,
                        "bar_size": "1 min",
                        "security_type": "FUT",
                        "exchange": "OSE.JPN",
                        "currency": "JPY",
                        "multiplier": 1000,
                        "include_expired": True
                }
    extractor = HistoricalDataExtractor()
    extractor.extract_historical_data(expiry_input)
    import json
    print(json.dumps(extractor.data, indent=1, sort_keys=True))
