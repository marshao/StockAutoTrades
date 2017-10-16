#!/usr/local/bin/python
# coding: utf-8

from C_Algorithms_BestMACDPattern import *
from src import C_GlobalVariable as glb

def apply_pattern(period=None, stock_code=None):

    if period is None:
        period = 'm30'
    if stock_code is None:
        stock_code = 'sz002310'

    bMACD = C_BestMACDPattern()
    # for each_stock_code in stock_codes:
    bMACD.apply_best_MACD_pattern_to_data(period=period, stock_code=stock_code, quo=0.7, ga=0.3, beta=0.2)


def best_pattern_daily_calculate():
    bMACD = C_BestMACDPattern()
    bMACD.best_pattern_daily_calculate('m30')
    bSAR = C_BestSARPattern()
    bSAR.SAR_patterns_exams()
    bSAR.SAR_ending_profit_all_patterns('sz300226')


def apply_multi_patterns(period=None, stock_codes=None):
    gv = glb.C_GlobalVariable()
    stock_pars_m30 = gv.get_stock_config()['stock_m30_config']
    '''
    stock_pars_m30 = {'sz002310': [0.7, 0.3, 0.2],
                      'sh600867': [0.8, 0.6, 0.2],
                      'sz300146': [0.7, 0.3, 0.2],
                      'sh600271': [0.9, 0.4, 0.2]
                      }
    '''
    if period is None:
        period = 'm30'
    if stock_codes is None:
        stock_codes = ['sz002310']

    bMACD = C_BestMACDPattern()
    # for each_stock_code in stock_codes:
    for each_stock in stock_codes:
        bMACD.apply_best_MACD_pattern_to_data(period=period, stock_code=each_stock, quo=stock_pars_m30[each_stock][0],
                                              ga=stock_pars_m30[each_stock][1], beta=stock_pars_m30[each_stock][2])

def best_multi_patterns_daily_calculate(stock_codes=None):
    if stock_codes is None:
        stock_codes = ['sz002310']
    bMACD = C_BestMACDPattern()

    for each_stock in stock_codes:
        bMACD.best_pattern_daily_calculate('m30', each_stock)

