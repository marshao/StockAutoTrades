#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import datetime, pandas, time, progressbar
from sqlalchemy import create_engine, Table, Column, MetaData
from sqlalchemy.sql import select, and_, or_, not_


class C_Algorithems_BestPattern(object):
    def __init__(self):
        self._input_dir = '/home/marshao/DataMiningProjects/Input/'
        self._output_dir = '/home/marshao/DataMiningProjects/Output/'
        self._stock_name_file = self._output_dir + 'StockNames.csv'
        self._stock_codes = []
        self._stock_code = "300226"
        self._stock_market = ""
        self._window = '10'
        self._engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')
        self._metadata = MetaData(self._engine)
        self._log_mesg = ''
        self._op_log = 'operLog.txt'
        self._my_columns = []
        self._my_DF = pandas.DataFrame(columns=self._my_columns)
        self._x_min = ['m5', 'm15', 'm30', 'm60']
        self._x_period = ['day', 'week']


class C_BestMACDPattern(C_Algorithems_BestPattern):
    '''
    Step1: decide what analysis period will be used (5min, 30min, daily)
    Step2: use MACD_Pattern_lists to calculate all trading signals of all patterns.
    Step3: use trading signals of all pattern to calculate ending profits of all pattern
    Step4: Analysis ending profits of all pattern to select the best pattern
    '''

    def _par(self, period):
        self._tb_MACDIndex = Table('tb_MACDIndex', self._metadata, autoload=True)
        self._tb_StockCodes = Table('tb_StockCodes', self._metadata, autoload=True)
        if period in self._x_min:
            self._tb_TradeSignal = Table('tb_StockIndex_MACD', self._metadata, autoload=True)
            self._tb_StockRecords = Table('tb_StockXMinRecords', self._metadata, autoload=True)
            self._tb_Trades = Table('tb_MACD_Trades', self._metadata, autoload=True)
        elif period in self._x_period:
            self._tb_TradeSignal = Table('tb_StockIndex_MACD_HalfHour', self._metadata, autoload=True)
            self._tb_StockRecords = Table('tb_StockXPeriodRecords', self._metadata, autoload=True)
            self._tb_Trades = Table('tb_MACD_Trades_HalfHour', self._metadata, autoload=True)

    def calculate_MACD_trading_signals(self, period):
        # use MACD_Pattern_lists to calculate all trading signals of all patterns
        pass

    def calculate_MACD_ending_profits(self):
        pass

    def select_best_pattern(self):
        pass


class C_BestSARPattern(C_Algorithems_BestPattern):
    pass


def main():
    pass


if __name__ == '__main__':
    main()
