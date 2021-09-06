# from tws_futures.settings import *
from tws_futures.helpers.parsers import parse_user_args
from tws_futures.helpers.input_loader import load_expiry_input
from tws_futures.helpers.utils import generate_csv
from tws_futures.tws_clients import extract_historical_data
from tws_futures.helpers.logger_setup import get_logger
import json

args = parse_user_args()
logger = get_logger(__name__, debug=args.debug)


def main():
    try:
        expiry_input = load_expiry_input(end_date=args.end_date,
                                         duration=args.duration)
        extract_historical_data(expiry_input)
        generate_csv(args.end_date)
    except Exception as e:
        logger.critical(f'Program crashed: {e}')
        if args.debug:
            raise e


if __name__ == '__main__':
    main()
