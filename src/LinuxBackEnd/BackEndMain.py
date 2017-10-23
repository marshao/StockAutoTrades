#!/usr/local/bin/python
# coding: utf-8



import sys, datetime, time

sys.path.append('/home/marshao/DataMiningProjects/Project_StockAutoTrade_LinuxBackEnd/StockAutoTrades/src')
import multiprocessing as mp
from apscheduler.schedulers.background import BackgroundScheduler
from PatternApply import apply_pattern, best_pattern_daily_calculate, update_stock_inhand
from C_GetDataFromWeb import C_GettingData
import C_GlobalVariable as glb

def main():
    print "This is the production enviroment!!!"
    single_stock()

def single_stock():
    period = 'm30'
    stock_code = 'sz002310'
    # gd = C_GettingData()
    # gd.job_schedule(period, stock_code)
    job_schedule(period, stock_code)

def multi_stocks():
    period = 'm30'
    stock_code = 'sz002310'
    # gd = C_GettingData()
    # gd.job_schedule(period, stock_code)
    job_schedule(period, stock_code)


def job_schedule(period=None, stock_code=None):
    # job_stores = {'default': MemoryJobStore()}
    # executor = {'processpool': ThreadPoolExecutor(8)}
    # job_default = {'coalesce': False, 'max_instances': 12}
    # scheduler_1 = BackgroundScheduler(jobstores=job_stores, executors=executor, job_defaults=job_default)

    if period is None:
        period = 'm30'
    if stock_code is None:
        stock_code = 'sz002310'
    scheduler_1 = BackgroundScheduler()
    scheduler_2 = BackgroundScheduler()

    scheduler_1.add_job(data_service, 'cron', day_of_week='mon-fri', hour='9-15', minute='5/15',
                        args=['m1'])
    scheduler_1.add_job(data_service, 'cron', day_of_week='mon-fri', hour='9-15', minute='7/15',
                        args=['m15'])
    scheduler_1.add_job(data_service, 'cron', day_of_week='mon-fri', hour='9-15', minute='1/30',
                        args=['m30'])
    scheduler_1.add_job(data_service, 'cron', day_of_week='mon-fri', hour='9-15', minute='10/30',
                        args=['m60'])
    scheduler_1.add_job(apply_pattern, 'cron', day_of_week='mon-fri', hour='9-15', minute='3/30',
                        args=[period, stock_code])
    scheduler_1.add_job(update_stock_inhand, 'cron', day_of_week='mon-fri', hour='9-15', minute='1/30')

    scheduler_2.add_job(best_pattern_daily_calculate, 'cron', day_of_week='fri', hour='22')

    scheduler_1.start()
    scheduler_2.start()

    # The switch of scheduler_1
    while True:
        scheduler_switch(scheduler_1, scheduler_2)


def scheduler_switch(scheduler_1, scheduler_2):
    current_time = datetime.datetime.now().time()
    master_config = glb.C_GlobalVariable().get_master_config()
    if (current_time > master_config['start_morning'] and current_time < master_config['end_morning']) or (
                    current_time > master_config['start_afternoon'] and current_time < master_config['end_afternoon']):
        scheduler_2.pause()
        scheduler_1.resume()
        scheduler_1.print_jobs()
        scheduler_2.print_jobs()
        print "scheduler_1 back to work"
        # time.sleep(600)
    else:
        print "out of the time of getting data"
        scheduler_1.pause()
        scheduler_2.resume()
        scheduler_2.print_jobs()

    time.sleep(60)


def data_service(period):
    gd = C_GettingData()
    stock_config = glb.C_GlobalVariable().get_stock_config()
    stock_codes = stock_config['stock_codes']
    processes = []
    for stock in stock_codes:
        p = mp.Process(target=gd.get_data_qq, args=(stock, period, 'qfq',))
        processes.append(p)

    for p in processes:
        p.start()

    for p in processes:
        p.join()

if __name__ == '__main__':
    main()
