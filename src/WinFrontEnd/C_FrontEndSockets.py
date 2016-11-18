#!/usr/local/bin/python
# coding: utf-8

import socket, time, datetime

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
            c.send('Thank you for connecting')
            mesg = c.recv(1024)
            self._prcess_message(mesg)
            c.close()

    def _prcess_message(self, mesg):
        print mesg + " at " + self._time_tag()
        return mesg

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local

def main():
    soc = C_FrontEndSockets()

if __name__ == '__main__':
    main()



