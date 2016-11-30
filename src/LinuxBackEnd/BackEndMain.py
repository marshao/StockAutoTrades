#!/usr/local/bin/python
# coding: utf-8

import socket
from C_GetDataFromWeb import *
from C_Algorithms_BestMACDPattern import *


def commu(cmd='1'):
    '''
                :param cmd: '1': update stock in hand information
                             '2': Buy with Meg
                             '3': Sale with Meg
                             '4, stock_code': Get stock current price
                             '5, stock_code': Get stock asset information.
                        recevie: x.1: command was executed successfully
                                x.2 : command was not executed successfully
                :return:
                '''
    s = socket.socket()
    host = socket.gethostname()
    port = 32768
    host = 'ghuan02-d.inovageo.com'
    l = "Buy 300226 1000 at 50.13 "
    s.connect((host, port))
    s.send(cmd)
    receive = s.recv(1024)
    print receive
    return receive


def apply_pattern(pattern_number=17, period='m30', stock_code='sz300226'):
    bMACD = C_BestMACDPattern()
    # for each_stock_code in stock_codes:
    bMACD.apply_best_MACD_pattern_to_data(pattern_number=pattern_number, period=period, stock_code=stock_code)


def best_pattern_daily_calculate():
    bMACD = C_BestMACDPattern()
    bMACD.best_pattern_daily_calculate()


def main():
    gd = C_GettingData()
    gd.service_getting_data()


if __name__ == '__main__':
    main()
