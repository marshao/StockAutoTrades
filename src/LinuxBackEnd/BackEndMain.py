#!/usr/local/bin/python
# coding: utf-8


from C_GetDataFromWeb import *
import sys

sys.path.append('/home/marshao/DataMiningProjects/Project_StockAutoTrade_LinuxBackEnd/StockAutoTrades/src')

def main():
    single_stock()

def single_stock():
    period = 'm30'
    stock_code = 'sz002310'
    gd = C_GettingData()
    gd.job_schedule(period, stock_code)

def multi_stocks():
    period = 'm30'
    stock_code = 'sz002310'
    gd = C_GettingData()
    gd.job_schedule(period, stock_code)

if __name__ == '__main__':
    main()
