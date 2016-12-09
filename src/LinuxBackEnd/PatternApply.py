#!/usr/local/bin/python
# coding: utf-8

from C_Algorithms_BestMACDPattern import *


def apply_pattern(pattern_number=17, period='m30', stock_code='sz300226'):
    bMACD = C_BestMACDPattern()
    # for each_stock_code in stock_codes:
    bMACD.apply_best_MACD_pattern_to_data(pattern_number=pattern_number, period=period, stock_code=stock_code)

    bSAR = C_BestSARPattern()
    bSAR.SAR_ending_profit_all_patterns('sz300226')



def best_pattern_daily_calculate():
    bMACD = C_BestMACDPattern()
    bMACD.best_pattern_daily_calculate()
