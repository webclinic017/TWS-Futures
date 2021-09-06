# -*- coding: utf-8 -*-

from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.client import Contract


class TWSWrapper(EWrapper):

    def __init__(self):
        EWrapper.__init__(self)


class TWSClient(EClient):

    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)


def create_stock(symbol, security_type='STK', exchange='SMART', currency='JPY'):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = security_type
    contract.exchange = exchange
    contract.currency = currency
    return contract


def create_futures_contract(expiry_input):
    contract = Contract()
    """
        contract.symbol = 'N225'
        contract.secType = 'FUT'
        contract.exchange = 'OSE.JPN'
        contract.currency = 'JPY'
        contract.multiplier = 1000
        contract.lastTradeDat
        eOrContractMonth = 20200910
        contract.includeExpired =  True
    """
    i = expiry_input
    symbol, security_type, exchange = i['symbol'], i['security_type'], i['exchange']
    currency, multiplier, expiry = i['currency'], i['multiplier'], i['expiry']
    include_expired = i['include_expired']
    contract.symbol = symbol
    contract.secType = security_type
    contract.exchange = exchange
    contract.currency = currency
    contract.multiplier = multiplier
    contract.lastTradeDateOrContractMonth = expiry
    contract.includeExpired = include_expired
    return contract
