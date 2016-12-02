#!/usr/local/bin/python
# coding: utf-8

import socket, time, datetime
from C_StockWindowControl import *



class C_FrontEndSockets:
    def __init__(self):
        self._swc = C_StockWindowControl()
        self._swc._get_handles()


    def _refresh_window_control(self):
        print "refresh started"
        last = time.time()
        while True:
            current = time.time()
            if current - last > 30:
                self._swc._buy_page()
                sleep(0.5)
                self._swc._stock_withdraw_page()
                last = current
                print "Trading software is refreshed and actived."



def main():
    soc = C_FrontEndSockets()
    soc._refresh_window_control()


if __name__ == '__main__':
    main()
