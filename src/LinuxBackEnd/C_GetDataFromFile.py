#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import datetime, time, os
import pandas as pd
import C_GlobalVariable as glb


class C_GetDataFromFile(object):
    '''
    Change: 2017-06-22 in _send_trading_command: modified the received signal to 1 and -1 to match the signal calculation result.
            2017-06-22 in _MACD_Singal_calcualtion_cross use MACD_short = 999 to represent real life stock operation and save them in DB.
    '''

    def __init__(self):
        gv = glb.C_GlobalVariable()
        self._emailobj = gv.get_emailobj()
        self._master_config = gv.get_master_config()
        self._input_path = self._master_config['ubuntu_file_input_dir']

    def _get_data(self):
        print "fuck that"
        for rt, dirs, files in os.walk('/home'):
            for file in files:
                print os.path.join(rt, file)


def main():
    gf = C_GetDataFromFile()
    gf._get_data()
