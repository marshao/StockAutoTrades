#!/usr/local/bin/python
# coding: utf-8

import socket


class C_BackEndMain():
    def __init__(self):
        pass


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

def main():
    pass


if __name__ == '__main__':
    main()
