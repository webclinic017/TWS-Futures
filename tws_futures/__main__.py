from tws_futures import get_logger
from tws_futures import parse_user_args
from tws_futures import load_expiry_input
from tws_futures import extract_historical_data
from tws_futures import generate_csv
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
