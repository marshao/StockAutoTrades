#!/usr/local/bin/python
# coding: utf-8

import socket, time, datetime, random
from C_StockWindowControl import *
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler

class C_FrontEndSockets:
    def __init__(self):
        self._swc = C_StockWindowControl()
        self._swc._get_handles()
        self._swc._get_various_data()
        pass

    def Tasks(self):
        # scheduler = BlockingScheduler()
        scheduler = BackgroundScheduler()
        # scheduler.add_job(self._listen, 'cron', day_of_week='mon-fri', hour='9-15', minute='2/30', second='30',
        #                  id='SocketListen')
        scheduler.add_job(self._listen, 'cron', day_of_week='mon-fri', hour='9-15', minute='1/1',
                          id='SocketListen')
        scheduler.add_job(self._refresh_window_control, 'interval', seconds='20', id='RefreshWindow')
        scheduler.start()
        scheduler.print_jobs()
        while True:
            scheduler.print_jobs()
            sleep(20)
            # self._refresh_window_control()

    def _listen(self):
        '''
        The port listening function will be activated 30 seconds before the pattern apply. The port will stay alive for
        120 seconds.
        The port listening job will also reset the StockWindowsControl
        :return:
        '''
        # self.__init__()
        swc = C_StockWindowControl()
        swc._get_handles()
        swc._get_various_data()
        last = time.time()
        alive = True
        s = socket.socket()
        host = 'Bei1Python'
        port = 32768
        s.bind((host, port))
        s.listen(5)
        print "Port Listening is started"
        while alive:
            current = time.time()
            if current - last > 30:
                alive = False
            c, addr = s.accept()
            print 'Got connection from', addr
            mesg = c.recv(1024)
            back_mesg = self._prcess_message(mesg, swc)
            c.send(back_mesg)
            c.close()
            print "listening"
            sleep(1)

    def _prcess_message(self, from_mesg, swc):
        back_mesg = ''
        print from_mesg
        items = from_mesg.split()
        if items[0] == '5':
            print "go to get cash avalible information"
            cash_avaliable = swc.update_asset()
            if cash_avaliable != '':
                back_mesg = '5.1 ' + cash_avaliable
            else:
                back_mesg = '5.2'
        elif items[0] == '1':
            print 'Go to get stock avalible information'
            done = swc._save_stock_infor_to_file()
            if done:
                back_mesg = '1.1'
            else:
                back_mesg = '1.2'
        elif items[0] == '2':
            print "issue a buy command"
            stockTrades = from_mesg.split()
            print "stockTrades is %s" % stockTrades
            done = swc.buy_stock(stockTrades)
            # done = True
            if done:
                back_mesg = '2.1'
            else:
                back_mesg = '2.2'
        elif items[0] == '3':
            print "issue a sales command"
            stockTrades = from_mesg.split()
            done = swc.sale_stock(stockTrades)
            #done = True
            if done:
                back_mesg = '3.1'
            else:
                back_mesg = '3.2'
        else:
            print "unknow command"
        print back_mesg + " at " + self._time_tag()
        return back_mesg

    def _refresh_window_control_old(self):
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

    def _refresh_window_control(self):
        print "refresh started"
        last = time.time()
        # cx = 60
        # cy = 327
        '''
        while True:
            current = time.time()
            if current - last > 20:
                # cx = cx + random.randint(50,500)
                # cy = cy + random.randint(50,500)
                # win32api.SetCursorPos((cx, cy))
                win32api.keybd_event(39, 0, 0, 0)
                win32api.keybd_event(39, 0, win32con.KEYEVENTF_KEYUP, 0)
                last = current
                print "Trading software is refreshed and actived."
            sleep(3)
        '''
        #cx = cx + random.randint(50,500)
        #cy = cy + random.randint(50,500)
        #win32api.SetCursorPos((cx, cy))
        win32api.keybd_event(39, 0, 0, 0)
        win32api.keybd_event(39, 0, win32con.KEYEVENTF_KEYUP, 0)
        print "Trading software is refreshed and actived."


    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local


def main():
    soc = C_FrontEndSockets()
    soc.Tasks()
    #soc._listen()
    #refresh_window = Timer(20, soc._refresh_window_control())
    #port_listen = Thread(target=soc._listen())
    #refresh_window.start()
    #port_listen.start()
    #refresh_window.join()
    #port_listen.join()

if __name__ == '__main__':
    main()
