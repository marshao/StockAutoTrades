#!/usr/local/bin/python
# coding: utf-8


from C_GetDataFromWeb import *

def main():
    period = 'm30'
    stock_code = 'sz002310'
    gd = C_GettingData()
    gd.job_schedule(period, stock_code)


if __name__ == '__main__':
    main()
