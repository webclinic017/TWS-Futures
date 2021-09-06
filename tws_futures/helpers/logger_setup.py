# -*- coding: utf-8 -*-

from logging.config import dictConfig
from logging import getLogger
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler

from os.path import isdir
from os.path import dirname
from os.path import join
from os import makedirs

_PROJECT_ROOT = dirname(dirname(dirname(__file__)))
LOG_FORMAT = '%(asctime)s | %(name)s:%(levelname)s | %(module)s:%(funcName)s:%(lineno)d | %(message)s'
LOG_LOCATION = join(_PROJECT_ROOT, 'logs')
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'file': {
            'format': LOG_FORMAT,
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
        'console': {
            'format': '=> %(levelname)s | %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'level': 'CRITICAL',
            'formatter': 'console',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',  # Default is stderr
        },
        # 'file': {
        #     'level': 'ERROR',
        #     'formatter': 'file',
        #     'class': 'logging.FileHandler',
        #     'encoding': 'utf-8',
        #     'filename': 'app.log'
        # }
    },
    'loggers': {
        'root': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False
        },
        'child': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}


def get_log_file():
    """
        Sets up log directory , creates a log file for current run and return full path for the same.
    """
    log_location = LOG_LOCATION
    if not(isdir(log_location)):
        makedirs(log_location)
    log_file_name = join(log_location, 'app.log')
    return log_file_name


def get_logger(name, debug=False):
    name = 'root' if name == '__main__' else 'child'
    if name == 'root':  # TODO: find a better way to do this
        level = 'DEBUG' if debug else 'INFO'
        for handler in LOG_CONFIG['handlers']:
            LOG_CONFIG['handlers'][handler]['level'] = level

        for logger in LOG_CONFIG['loggers']:
            LOG_CONFIG['loggers'][logger]['level'] = level

        dictConfig(LOG_CONFIG)

    logger = getLogger(name)
    logger.setLevel(LOG_CONFIG['loggers']['root']['level'])

    if debug:
        formatter = Formatter(LOG_CONFIG['formatters']['file']['format'])
        file_name = get_log_file()
        handler = TimedRotatingFileHandler(file_name, when='H', interval=3, backupCount=5, delay=True)
        handler.setLevel(LOG_CONFIG['loggers'][name]['level'])
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


if __name__ == '__main__':
    logger = get_logger(__name__, debug=True)
    logger.debug('root logger test debug')
    logger.info('root logger test info')
    logger.warning('root logger test warning')
    logger.error('root logger test error')
    logger.critical('root logger test critical')

    logger = get_logger('child', debug=False)
    logger.debug('child logger test debug')
    logger.info('child logger test info')
    logger.warning('child logger test warning')
    logger.error('child logger test error')
    logger.critical('child logger test critical')
