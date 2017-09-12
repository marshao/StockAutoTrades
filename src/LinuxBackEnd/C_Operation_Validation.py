#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

from sqlalchemy.sql import select
from sqlalchemy import create_engine
import pandas as pd
import datetime, time


class C_Operation_Validation(object):
    '''
    Change: 2017-06-22 in _send_trading_command: modified the received signal to 1 and -1 to match the signal calculation result.
            2017-06-22 in _MACD_Singal_calcualtion_cross use MACD_short = 999 to represent real life stock operation and save them in DB.
    '''

    def __init__(self):
        self._input_dir = '/home/marshao/DataMiningProjects/Input/'
        self._output_dir = '/home/marshao/DataMiningProjects/Output/'
        self._operation_log = self._output_dir + 'operLog.txt'
        self._validation_log = self._output_dir + 'validateLog.txt'
        self._engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return time_stamp

    def _time_tag_dateonly(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return only_date

    def _write_log(self, log_mesg, logPath='validateLog.txt'):
        # logPath = str(self._time_tag_dateonly()) + logPath
        fullPath = self._output_dir + logPath
        if isinstance(log_mesg, str):
            with open(fullPath, 'a') as log:
                log.writelines(log_mesg)
        else:
            for message in log_mesg:
                with open(fullPath, 'a') as log:
                    log.writelines(message)
        self._log_mesg = ''

    def get_last_signal_from_DB(self, stock_code):
        quote_time = self._time_tag_dateonly()
        sql_select_last = 'select * from tb_StockIndex_MACD_New where stock_code = %s and period = "M30" and ' \
                          'quote_time < %s order by quote_time DESC'
        df_signals = pd.read_sql(sql_select_last, con=self._engine, params=(stock_code, quote_time),
                                 index_col='quote_time')
        print df_signals.index[0], df_signals.Signal[0]

    def get_last_signal_from_log(self, stock_code):
        quote_time = self._time_tag_dateonly()
        f = open(self._operation_log, 'r')
        key = ['Step1', 'Signal']
        i = 0
        for line in reversed(f.readlines()):
            print line.rstrip()
            words = line.split()
            print words
            i += 1
            if i > 5:
                break

    def get_daily_signals_from_DB(self):
        pass

    def get_daily_signals_from_log(self):
        pass

    def signal_validation(self):
        pass


def main():
    ov = C_Operation_Validation()
    # ov.get_last_signal_from_DB('sz002310')
    ov.get_last_signal_from_log('sz002310')


if __name__ == '__main__':
    main()
