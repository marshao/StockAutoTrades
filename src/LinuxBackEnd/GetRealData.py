#!/usr/local/bin/python
# coding: utf-8

from C_GetDataFromWeb import *


def get_data_qq(stock_code, period):
    gd = C_GettingData()
    gd.get_data_qq(stock_code, period)
    done = True
    return done
