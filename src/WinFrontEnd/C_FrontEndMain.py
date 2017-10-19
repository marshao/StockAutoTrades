#!/usr/local/bin/python
# coding: utf-8

import socket, time
from C_StockWindowControl import *
import logging
from src import C_GlobalVariable as glb

class C_FrontEndSockets:
    def __init__(self):
        self._swc = C_StockWindowControl()
        self._swc._get_handles()
        self._swc._get_various_data()
        logging.basicConfig()

    def _listen(self):
        '''
        This is the port listening function, once it goes into listen mode, it will not exit unless meet a exception
        :return:
        '''
        gv = glb.C_GlobalVariable()
        alive = True
        s = socket.socket()
        host = gv.get_master_config()['pro_front_name']
        # host = 'Bei1Python'
        port = gv.get_master_config()['win_port']
        # port = 32768
        s.bind((host, port))
        s.listen(5)
        print "Start Listening"
        while alive:
            # 进入监听模式,只有收到数据才会进行下一步
            c, addr = s.accept()
            mesg = c.recv(1024)
            back_mesg = self._prcess_message(mesg)
            c.send(back_mesg)
            c.close()
        s.close()


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

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local


def main():
    soc = C_FrontEndSockets()
    soc._listen()


if __name__ == '__main__':
    main()
