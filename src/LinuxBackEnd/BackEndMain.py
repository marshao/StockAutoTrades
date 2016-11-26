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
                             '4': Get stock current price
                :return:
                '''
    s = socket.socket()
    host = socket.gethostname()
    port = 32768
    host = 'ghuan02-d.inovageo.com'
    l = "Buy 300226 1000 at 50.13 "
    print l
    # s.connect((host, port))
    # s.send(l)
    # s.recv(1024)


def main():
    pass


if __name__ == '__main__':
    main()
