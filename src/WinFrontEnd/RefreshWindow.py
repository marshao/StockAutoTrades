#!/usr/local/bin/python
# coding: utf-8

import time
from C_StockWindowControl import *
from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import logging


class C_FrontEndRefresh:
    def __init__(self):
        self._swc = C_StockWindowControl()
        self._swc._get_handles()
        self._swc._get_various_data()
        logging.basicConfig()

    def Tasks(self):
        # scheduler = BlockingScheduler()
        executors = {'default': ThreadPoolExecutor(10),
                     'processpool': ProcessPoolExecutor(3)}
        job_defaults = {'coalesce': False, 'max_instances': 3}
        # scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults)
        scheduler = BackgroundScheduler()
        scheduler.add_job(self._reActive_Platform, 'cron', day_of_week='mon-fri', hour='9-15', minute='2/30',
                          second='30', id='PlatformReActive')
        scheduler.add_job(self._reActive_Platform, 'cron', day_of_week='mon-fri', hour='8', minute='*/5',
                          id='PlatformDailyActive')
        scheduler.add_job(self._refresh_window_control, 'interval', seconds=20, id='RefreshWindow')
        scheduler.start()
        scheduler.print_jobs()
        while True:
            scheduler.print_jobs()
            sleep(20)
            # self._refresh_window_control()

    def _reActive_Platform(self):
        self._swc._stock_holding_page()
        sleep(5)
        self._swc._buy_page()
        # self._swc.update_asset()

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
        # cx = cx + random.randint(50,500)
        # cy = cy + random.randint(50,500)
        # win32api.SetCursorPos((cx, cy))
        win32api.keybd_event(39, 0, 0, 0)
        win32api.keybd_event(39, 0, win32con.KEYEVENTF_KEYUP, 0)
        print "Trading software is refreshed and actived."

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local


def main():
    soc = C_FrontEndRefresh()
    soc.Tasks()


if __name__ == '__main__':
    main()
