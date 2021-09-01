from argparse import ArgumentParser
from datetime import datetime as dt
from tws_futures.helpers._input_types import INPUT_TYPES


_DATE_FORMAT = '%Y%m%d'
_CURRENT_DATE = dt.today().date().strftime(_DATE_FORMAT)


def parse_user_args():
    parser = ArgumentParser(prog='tws_futures',
                            description='A Python CLI built to download bar-data for'
                                        'Future Contracts from TWS API.',
                            )
    parser.add_argument('--end-date', '-ed', type=INPUT_TYPES['date'],
                        default=_CURRENT_DATE, dest='end_date',
                        help='End date for data extraction, default is current date. '
                             '(Expected format: "YYYYMMDD")')
    parser.add_argument('--duration', '-d', type=INPUT_TYPES['duration'],
                        default=1, dest='duration',
                        help='Number of days for which data is being requested.')
