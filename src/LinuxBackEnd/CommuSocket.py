#!/usr/local/bin/python
# coding: utf-8

import socket
from sqlalchemy import create_engine


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
    try:
        s = socket.socket()
        host = socket.gethostname()
        port = 32768
        host = '10.175.10.99'
        l = "Buy 300226 1000 at 50.13 "
        s.connect((host, port))
        s.send(cmd)
        receive = s.recv(1024)
        print receive
        return receive
    except:
        print "The front end server error"


def db_engine():
    eng1 = create_engine('mysql+mysqldb://marshao:123@10.176.50.233/DB_StockDataBackTest')
    eng2 = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')
    return eng1