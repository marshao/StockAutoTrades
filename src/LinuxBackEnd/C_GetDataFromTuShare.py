#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import datetime, time
import pandas as pd
import tushare as ts
import C_GlobalVariable as glb


class C_GetDataFromTuShare(object):
    '''
    Change: 2017-06-22 in _send_trading_command: modified the received signal to 1 and -1 to match the signal calculation result.
            2017-06-22 in _MACD_Singal_calcualtion_cross use MACD_short = 999 to represent real life stock operation and save them in DB.
    '''

    def __init__(self):
        gv = glb.C_GlobalVariable()
        self._emailobj = gv.get_emailobj()

    def get_m30_data(self, code=None):
        if code is None:
            code = '002310'
        df = ts.get_hist_data(code, ktype='30')
        print df.tail(5)


def main():
    gt = C_GetDataFromTuShare()
    gt.get_m30_data()


if __name__ == '__main__':
    main()
