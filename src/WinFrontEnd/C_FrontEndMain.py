#!/usr/local/bin/python
# coding: utf-8

import socket, time, datetime
from C_StockWindowControl import *
from threading import Thread, Timer


class C_FrontEndSockets:
    def __init__(self):
        self._swc = C_StockWindowControl()
        self._swc._get_handles()
        self._swc._get_various_data()


    def _listen(self):
        s = socket.socket()
        host = 'Bei1Python'
        port = 32768
        s.bind((host, port))
        s.listen(5)
        print "Port Listening is started"
        # Can be invoked 30 secs before applying the pattern calculation, and make the listen survive for 120 secs.
        while True:
            c, addr = s.accept()
            print 'Got connection from', addr
            mesg = c.recv(1024)
            back_mesg = self._prcess_message(mesg)
            c.send(back_mesg)
            c.close()

    def _prcess_message(self, from_mesg):
        back_mesg = ''
        print from_mesg
        items = from_mesg.split()
        if items[0] == '5':
            print "go to get cash avalible information"
            cash_avaliable = self._swc.update_asset()
            if cash_avaliable != '':
                back_mesg = '5.1 ' + cash_avaliable
            else:
                back_mesg = '5.2'
        elif items[0] == '1':
            print 'Go to get stock avalible information'
            done = self._swc._save_stock_infor_to_file()
            if done:
                back_mesg = '1.1'
            else:
                back_mesg = '1.2'
        elif items[0] == '2':
            print "issue a buy command"
            stockTrades = from_mesg.split()
            print "stockTrades is %s" % stockTrades
            done = self._swc.buy_stock(stockTrades)
            # done = True
            if done:
                back_mesg = '2.1'
            else:
                back_mesg = '2.2'
        elif items[0] == '3':
            print "issue a sales command"
            stockTrades = from_mesg.split()
            done = self._swc.sale_stock(stockTrades)
            #done = True
            if done:
                back_mesg = '3.1'
            else:
                back_mesg = '3.2'
        else:
            print "unknow command"
        print back_mesg + " at " + self._time_tag()
        return back_mesg

    def _refresh_window_control(self):
        print "refresh started"
        last = time.time()
        while True:
            current = time.time()
            if current - last > 30:
                self._swc._buy_page()
                sleep(0.3)
                self._swc._stock_withdraw_page()
                last = current
                print "Trading software is refreshed and actived."

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local


def main():
    soc = C_FrontEndSockets()
    soc._listen()
    #refresh_window = Timer(20, soc._refresh_window_control())
    #port_listen = Thread(target=soc._listen())
    #refresh_window.start()
    #port_listen.start()
    #refresh_window.join()
    #port_listen.join()

if __name__ == '__main__':
    main()
