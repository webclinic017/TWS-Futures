from unittest import TestCase
from unittest import main
from tws_futures import load_expiry_input
from tws_futures.tws_clients import HistoricalDataExtractor


class TWSFuturesTester(TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    def test_load_expiry(self):
        expected_keys = ['symbol', 'expiry', 'end_date', 'duration', 'bar_size',
                         'security_type', 'exchange', 'currency', 'multiplier',
                         'include_expired']
        expiry_input = load_expiry_input(symbol='N225', end_date='20210906', duration=1)
        # expiry input must be a list
        self.assertIsInstance(expiry_input, list)
        # it must contain only dictionary objects
        for i in expiry_input:
            self.assertIsInstance(i, dict)
            self.assertCountEqual(i.keys(), expected_keys)
        # print(f'Expiry Input: {expiry_input}')
        self.expiry_input = expiry_input

    def test_data_extraction(self):
        expiry_input = {
                            'symbol': 'N225',
                            'expiry': '20210909',
                            'end_date': '20210906',
                            'duration': 1,
                            'bar_size': '1 min',
                            'security_type': 'FUT',
                            'exchange': 'OSE.JPN',
                            'currency': 'JPY',
                            'multiplier': 1000,
                            'include_expired': True
                        }
        client = HistoricalDataExtractor()
        client.extract_historical_data(expiry_input)
        print(client.data)

    def test_csv_generation(self):
        pass


if __name__ == '__main__':
    main()
