#!/usr/local/bin/python
# coding: utf-8

import socket, time, datetime
from C_StockWindowControl import *

class C_FrontEndSockets:

    def __init__(self):
        s = socket.socket()
        host = 'ghuan02-d.inovageo.com'
        port = 32768
        s.bind((host, port))
        self._listen(s)


    def _listen(self, s):
        s.listen(5)
        while True:
            c, addr = s.accept()
            print 'Got connection from', addr
            mesg = c.recv(1024)
            back_mesg = self._prcess_message(mesg)
            c.send(back_mesg)
            c.close()

    def _prcess_message(self, from_mesg):
        back_mesg = ''
        items = from_mesg.split()
        if items[0] == '5':
            print "go to get cash avalible information"
            swc = C_StockWindowControl()
            back_mesg = '5.1 ' + swc.update_asset()
        else:
            back_mesg = '5.2'
        print back_mesg + " at " + self._time_tag()
        return back_mesg

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local

def main():
    soc = C_FrontEndSockets()


if __name__ == '__main__':
    main()



