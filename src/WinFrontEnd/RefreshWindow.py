#!/usr/local/bin/python
# coding: utf-8

import time, random
import win32api
from C_StockWindowControl import *



class C_FrontEndSockets:
    def __init__(self):
        pass


    def _refresh_window_control(self):
        print "refresh started"
        last = time.time()
        cx = 60
        cy = 327
        while True:
            current = time.time()
            if current - last > 20:
                cx = cx + random.randint(50,500)
                cy = cy + random.randint(50,500)
                #win32api.SetCursorPos((cx, cy))
                win32api.keybd_event(39, 0, 0, 0)
                win32api.keybd_event(39, 0, win32con.KEYEVENTF_KEYUP, 0)
                last = current
                print "Trading software is refreshed and actived."



def main():
    soc = C_FrontEndSockets()
    soc._refresh_window_control()


if __name__ == '__main__':
    main()
