#!/usr/local/bin/python
# coding: GBK

__metclass__ = type

import os, time, pandas
from datetime import datetime
from sqlalchemy import create_engine


class C_GetDataFromWeb:
    '''
    This is the class to monitor the stock real time price from various resources: (Sina, SnowBall, etc)
    '''

    def __init__(self):
        self._output_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\'
        self._input_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._install_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\'

        self._data_source = {'sina': 'http://hq.sinajs.cn/list=', 'snowball': ''}
        self._period = ['15min', '30min', '60min']
        self._stock_code = ['300226', '600887']
        self._url = ''
        self._log_mesg = ''
        self._op_log = 'operLog.txt'
        self._stock_code = ''
        self._engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')


    def download_historical_period_data(self, period, stock_code, start_time, end_time):
        pass

    def clean_data(self, stock_code, period):
        pass

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local

    def _time_tag_dateonly(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.now()
        only_date = time_stamp.date()
        return only_date

    def _write_log(self, log_mesg, logPath):
        fullPath = self._output_dir + logPath
        with open(fullPath, 'a') as log:
            log.writelines(log_mesg)

    def _convert_encoding(self, lines, new_coding='UTF-8'):
        try:
            encoding = 'GB2312'
            data = lines.decode(encoding)
            data = data.encode(new_coding)
        except:
            data = 'DecodeError'
        return data


def main():
    pass


if __name__ == '__main__':
    main()
