#!/usr/local/bin/python
# coding: utf-8

from C_Algorithms_BestMACDPattern import *


def apply_pattern(period=None, stock_code=None):
    if period is None:
        period = 'm30'
    if stock_code is None:
        stock_code = 'sz002310'
    bMACD = C_BestMACDPattern()
    # for each_stock_code in stock_codes:
    bMACD.apply_best_MACD_pattern_to_data(period=period, stock_code=stock_code, quo=0.7, ga=0.3)





def best_pattern_daily_calculate():
    bMACD = C_BestMACDPattern()
    bMACD.best_pattern_daily_calculate('m30')
    bSAR = C_BestSARPattern()
    bSAR.SAR_patterns_exams()
    bSAR.SAR_ending_profit_all_patterns('sz300226')
