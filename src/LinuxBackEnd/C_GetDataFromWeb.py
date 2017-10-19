#!/usr/local/bin/python
# coding: GBK

__metclass__ = type


import time, pandas, urllib, re
import datetime
from sqlalchemy import Table,  MetaData
from sqlalchemy.sql import select, and_
from PatternApply import apply_pattern, best_pattern_daily_calculate

import C_GlobalVariable as glb
# from apscheduler.schedulers import Scheduler
#import multiprocessing as mp
import logging

class C_GettingData:
    '''
    This is the class to monitor the stock real time price from various resources: (Sina, SnowBall, etc)
    #Change: 2017-07-05 Found a mistake in schedule task. Everytime before the schedular call the apply_best_pattern()
    it will also download the m30 data again. It cause there are two very timely closed m30 records in DB every time.
    Ex: m30 in 2:30 and m30 in 2:39. These closed records will set the signal calcultion and pattern selection into error.
    To fix that: Delete the unwanted rows each time.

    # Change: 2017-07-05 Added opening rows for each day by adding funcion _insert_opening_records()
    '''

    def __init__(self):
        gv = glb.C_GlobalVariable()
        self._master_config = gv.get_master_config()
        self._calcu_config = gv.get_calcu_config()
        self._stock_config = gv.get_stock_config()

        self._output_dir = self._master_config['ubuntu_output_dir']
        self._input_dir = self._master_config['win_input_dir']
        self._install_dir = self._master_config['win_install_dir']

        # self._stock_code = ['sz300226', 'sh600887', 'sz300146', 'sh600221']
        # self._stock_code = ['sz300146', 'sh600867', 'sz002310', 'sh600221']
        self._stock_code = self._stock_config['stock_codes']

        self._op_log = self._master_config['op_log']
        self._engine = self._master_config['db_engine']
        self._metadata = MetaData(self._engine)
        self._x_min = self._master_config['x_min']
        self._x_period = self._master_config['x_period']
        self._q_count = self._master_config['q_count']
        self._fq = self._master_config['fq']
        self._data_source = self._master_config['data_source']
        self._start_morning = self._master_config['start_morning']
        self._end_morning = self._master_config['end_morning']
        self._start_afternoon = self._master_config['start_afternoon']
        self._end_afternoon = self._master_config['end_afternoon']

        self._log_mesg = ''
        self._my_real_time_DF_columns_sina = ['stock_code', 'close_price', 'open_price', 'current_price', 'high_price', 'low_price', 'buy_price', 'sale_price', 'trading_volumn', 'trading_amount',
                   'buy1_apply','buy1_price','buy2_apply','buy2_price','buy3_apply','buy3_price','buy4_apply','buy4_price','buy5_apply','buy5_price',
                   'sale1_apply','sale1_price','sale2_apply','sale2_price','sale3_apply','sale3_price','sale4_apply','sale4_price','sale5_apply','sale5_price',
                   'today','totime']
        self._real_time_DF_columns_qq = ['stock_name','stock_code', 'current_price', 'close_price', 'open_price',
                                              'current_buy_amount', 'current_sale_amount',
                                              'buy1_price','buy1_apply',  'buy2_price','buy2_apply',  'buy3_price','buy3_apply',
                                                'buy4_price','buy4_apply', 'buy5_price','buy5_apply',
                                              'sale1_price','sale1_apply',  'sale2_price','sale2_apply',  'sale3_price', 'sale3_apply',
                                              'sale4_price','sale4_apply',  'sale5_price', 'sale5_apply',
                                              'time', 'net_chg','net_chg_percent','high_price','low_price','total_volumn', 'total_amount',
                                                'turnover_rate', 'PE','circulation_market_value','total_market_value','PB','limit_up','limit_down']
        self._x_min_columns = ['quote_time', 'open_price', 'high_price','low_price','close_price','trading_volumn','stock_code','period']
        self._1_min_columns = ['quote_time', 'price', 'trading_volumn','stock_code']
        self._stock_minitue_data_DF = pandas.DataFrame(columns = self._my_real_time_DF_columns_sina)
        self._x_min_data_DF = pandas.DataFrame(columns = self._x_min_columns)
        self._1_min_data_DF = pandas.DataFrame(columns = self._1_min_columns)
        self._real_time_data_DF = pandas.DataFrame(columns = self._real_time_DF_columns_qq)

        self._fun = self._empty_fun



    def __get_real_time_data_sina(self, data_source, stock_code):
        # 此函数负责拾取每60秒的数据更新
        per_real_data = self.__price_monitoring_sina(data_source, stock_code)
        # 将返回的per_real_data 增加到DF stock_real_data中
        print "new data found at ",self._time_tag()
        for row in per_real_data:
            self._stock_minitue_data_DF.loc[len(self._stock_minitue_data_DF)] = row


    def __price_monitoring_sina(self, data_source, stock_code):
        # This function is able to get real current data when it is called.
        # A list contain current data will be returned.
        if stock_code == None:
            i = 1
            stock_code = self._stock_code[0]
            count = len(self._stock_code)
            if i <= (count - 1):
                stock_code = stock_code+ ',' + self._stock_code[i]
                i += 1
        if data_source == None:
            data_source = self._data_source['sina']
        else:
            for each_key in self._data_source.keys():
                if each_key == data_source:
                    data_source = self._data_source[each_key]
                else:
                    data_source = self._data_source['sina']

        url = data_source + stock_code
        html = urllib.urlopen(url)
        real_data = html.read()
        #Send real time data to process and use the returned list set to build a Pandas DataFrame
        per_real_data = self.__process_real_time_data_sina(real_data)
        return  per_real_data


    def __process_real_time_data_sina(self, real_data):
        '''
        :param real_data: Get the real time downloaded price data from Web Site, and converted each element of the prices
         into right format.
        :return: Return a list set of the price data back to calling function.
        '''
        stock_data_set = real_data.split(';')
        #print stock_data_set
        stock_data_list = []
        stock_data_ll = []
        stock_count = 0

        while stock_count < (len(stock_data_set)-1):
            element = stock_data_set[stock_count].split(',')
            stock_count += 1
            i = 0
            #print stock_count, element
            if len(element) != 33:
                print 'Retrieved data is not completed, it has %i elements' %len(element)
                return stock_data_list
            else:
                while i < 33:
                    if i <= 31:
                        if i in (1,2,3,4,5,6,7,11,13,15,17,19,21,23,25,27,29):
                            stock_data_list.append(float(element[i]))
                            i += 1
                        elif i in (8,10,12,14,16,18,20,22,24,26,28):
                            stock_data_list.append(int(element[i]))
                            i += 1
                        elif i == 9:
                            stock_data_list.append(float(element[i]) / 10000)
                            i += 1
                        elif i in (30,31):
                            stock_data_list.append(element[i])
                            i += 1
                        elif i == 0:
                            #m = ((element[i].split('='))[0].split('_'))[2]
                            stock_data_list.append(((element[i].split('='))[0].split('_'))[2])
                            i += 1
                        else:
                            i += 1
                    else:
                        i += 1
                stock_data_ll.append(stock_data_list)
                stock_data_list = []
        return stock_data_ll
        # here is not finish yet, still need to return the list back

    def __save_real_time_data_to_db_sina(self):
        get_real_time_data = self._timer(self.__get_real_time_data_sina)
        while True:
            current_time = datetime.datetime.now().time()
            if (current_time > self._start_morning and current_time < self._end_morning) or (current_time > self._start_afternoon and current_time < self._end_afternoon):
                # Need a while true loop in here to keep hearing the real time data
                get_real_time_data()
                self._stock_minitue_data_DF.to_sql('tb_StockRealTimeData', self._engine, if_exists='append', index=False)
                time.sleep(5)
                #print "Saved data into DB"
                self._stock_minitue_data_DF.drop(self._stock_minitue_data_DF.index[:], inplace= True)
            else:
                print "Not in transaction time, wait 10 min to try again."
                time.sleep(600)



    def service_getting_data(self):
        #get_data = self._timer(self.get_data_qq)
        last_run = time.time()
        while True:
            current = time.time()
            current_time = datetime.datetime.now().time()
            lag = current - last_run
            print "Tring to save data at last_run %s and current %s  and current - last_run %s" % (
            last_run, current, str(lag))
            time.sleep(15)
            if (current_time > self._start_morning and current_time < self._end_morning) or (current_time > self._start_afternoon and current_time < self._end_afternoon):
                # Need a while true loop in here to keep hearing the real time data
                if current - last_run > 900: # read data from web site at every 15 min
                    for stock in self._stock_code:
                        self.get_data_qq(stock, period = 'm5')
                        print 'saved m5 data'
                        time.sleep(5)
                        self.get_data_qq(stock, period='m1')
                        print 'saved m1 data'
                        time.sleep(5)
                        self.get_data_qq(stock, period='m30')
                        print 'saved m30 data'
                        time.sleep(5)
                        self.get_data_qq(stock, period='m60')
                        print 'saved m1 data'
                        time.sleep(5)
                        self.get_data_qq(stock, period = 'real')
                        print 'saved real time data'
                    last_run = time.time()
                    self._log_mesg = 'Write data to DB at ', self._time_tag()
                    print self._log_mesg
                    print "Saving data process took %s seconds" % (last_run - current)
                    apply_pattern(17, 'm30', 'sz300226')
            else:
                print "Not in transaction time, wait 10 min to try again."
                st_time = datetime.time(21, 0, 0)
                ed_time = datetime.time(21, 15, 0)
                if (current_time > st_time) and (current_time < ed_time):
                    best_pattern_daily_calculate()
                time.sleep(600)

    def get_data_qq(self, stock_code=None, period=None, fq=None, q_count=None):
        logging.debug('Starting')
        if stock_code is None:
            stock_code = 'sz300226'
        if period is None:
            period = 'day'
        if fq is None:
            fq = 'qfq'
        if q_count is None:
            q_count = '320'

        self._stock_minitue_data_DF = pandas.DataFrame(columns=self._my_real_time_DF_columns_sina)
        self._x_min_data_DF = pandas.DataFrame(columns=self._x_min_columns)
        self._1_min_data_DF = pandas.DataFrame(columns=self._1_min_columns)
        got = True
        # try:
        if period in self._x_period:  # precess day/week data
            fq = ('qfq' if fq not in (self._fq) else fq)
            url = self._data_source['qq_x_period'] % (stock_code, period, q_count, fq)
            html = urllib.urlopen(url)
            data = html.read()
            self._process_x_period_data_qq(data, period, fq, stock_code)
            # self._save_data_to_db_qq(period, stock_code)
        elif period in (self._x_min):
            if period == 'm1':  # process 1 min data
                    url = self._data_source['qq_1_min'] % (stock_code, stock_code)
                    print url
                    html = urllib.urlopen(url)
                    data = html.read()
                    self._process_1_min_data_qq(data, stock_code)
                    #self._save_data_to_db_qq(period, stock_code)
            else:  # process X min data
                if period == 'm5':
                    q_count = self._q_count[1]
                    url = self._data_source['qq_x_min'] % (stock_code, period, q_count)
                elif period == 'm15':
                    q_count = self._q_count[2]
                    url = self._data_source['qq_x_min'] % (stock_code, period, q_count)
                elif period == 'm30':
                    if q_count is None:
                        q_count = self._q_count[3]
                    url = self._data_source['qq_x_min'] % (stock_code, period, q_count)
                    print "Get URL"
                elif period == 'm60':
                    q_count = self._q_count[3]
                    url = self._data_source['qq_x_min'] % (stock_code, period, q_count)
                else:
                    q_count = self._q_count[4]
                    url = self._data_source['qq_x_min'] % (stock_code, period, q_count)
                print url
                html = urllib.urlopen(url)
                data = html.read()
                self._process_x_min_data_qq(data, period, stock_code)
                #self._save_data_to_db_qq(period, stock_code)
        else:  # process real time data
            url = self._data_source['qq_realtime'] % stock_code
            html = urllib.urlopen(url)
            data = html.read()
            self._process_real_time_data_qq(data)
        self._log_mesg = self._log_mesg + "At %s Getting: Stock %s period %s data has been download.\n" % (
        self._time_tag(), stock_code, period)
        self._save_data_to_db_qq(period, stock_code)
        # except:
        #    got = False
        #    print "Can not load data for %s with Period %s"%(stock_code, period)
        logging.debug('Finished')
        self._write_log(self._log_mesg)
        return got


    def _process_x_period_data_qq(self, data, period, fq, stock_code):
        self._x_min_data_DF.drop(self._x_min_data_DF.index[:], inplace = True)
        sPos = data.find(fq+period)+len(fq+period)+4
        ePos = data.find('"qt"')
        data = data[sPos:(ePos-3)].split('],[')
        p = re.compile(r'\d+.\d+.\d+')
        for item in data:
            if item.find('{"nd"') != -1:
                item = item[:item.find('{"nd"')]
            tmp_l = p.findall(item)
            #tmp_l[0] = re.sub(r'[^\d]', '', tmp_l[0])
            tmp_l[0] = datetime.datetime.strptime(tmp_l[0], '%Y-%m-%d')
            i =1
            while i < 6:
                try:
                    if tmp_l[i] != '':
                        tmp_l[i] = float(tmp_l[i])
                except: # the except in here is to deal with the today's value, which is missing high, low, close
                    tmp_v = tmp_l[i-1]
                    tmp_l[i-1] = 0.00
                    tmp_l.append(tmp_v)
                i += 1
            tmp_l.append(stock_code)
            tmp_l.append(period)
            #print tmp_l
            self._x_min_data_DF.loc[len(self._x_min_data_DF)] = tmp_l
        #print self._x_min_data_DF
        self._x_min_data_DF.set_index('quote_time', inplace=True)
        #print self._x_min_data_DF
        print "processed %s data" %period
        return self._x_min_data_DF


    def _process_real_time_data_qq(self, data):
        self._real_time_data_DF.drop(self._real_time_data_DF.index[:], inplace=True)
        sPos = data.find('=')
        data = data[sPos+2:-3].split('~')
        tmp_l = []
        tmp_l.append(data[1]) # stock_name
        tmp_l.append(data[2])  # stock_code
        tmp_l.append(float(data[3])) # current_price
        tmp_l.append(float(data[4])) # close_price
        tmp_l.append(float(data[5])) #open_price
        tmp_l.append(int(data[7])) # buying_amount
        tmp_l.append(int(data[8])) # saling_amount
        tmp_l.append(float(data[9])) # buy1_price
        tmp_l.append(int(data[10]))  # buy1_amount
        tmp_l.append(float(data[11])) # buy2_price
        tmp_l.append(int(data[12]))  # buy2_amount
        tmp_l.append(float(data[13])) # buy3_price
        tmp_l.append(int(data[14]))  # buy3_amount
        tmp_l.append(float(data[15])) # buy4_price
        tmp_l.append(int(data[16]))  # buy4_amount
        tmp_l.append(float(data[17])) # buy5_price
        tmp_l.append(int(data[18]))  # buy5_amount
        tmp_l.append(float(data[19])) # sale1_price
        tmp_l.append(int(data[20]))  # sale1_amount
        tmp_l.append(float(data[21])) # sale2_price
        tmp_l.append(int(data[22]))  # sale2_amount
        tmp_l.append(float(data[23])) # sale3_price
        tmp_l.append(int(data[24]))  # sale3_amount
        tmp_l.append(float(data[25])) # sale4_price
        tmp_l.append(int(data[26]))  # sale4_amount
        tmp_l.append(float(data[27])) # sale5_price
        tmp_l.append(int(data[28]))  # sale5_amount
        tmp_l.append(datetime.datetime.strptime(data[30],'%Y%m%d%H%M%S')) # trade_time
        tmp_l.append(float(data[31]))  # net_chg
        tmp_l.append(float(data[32]))  # net_chg_percent
        tmp_l.append(float(data[33]))  # high_price
        tmp_l.append(float(data[34]))  # low_price
        tmp_l.append(int(data[36]))  # trading_volumn
        tmp_l.append(int(data[37]))  # trading_amount
        tmp_l.append(float(data[38]))  # turnover_rate
        if data[39] != '':
            tmp_l.append(float(data[39]))  #PE
        else:
            tmp_l.append(0.00) #PE
        tmp_l.append(float(data[44]))  # circulate_market_value
        tmp_l.append(float(data[45]))  # total_market_value
        tmp_l.append(float(data[46]))  # PB
        tmp_l.append(float(data[47]))  # limit_up
        tmp_l.append(float(data[48]))  # limit_down
        self._real_time_data_DF.loc[len(self._real_time_data_DF)] = tmp_l
        print "processed real time data"
        return self._real_time_data_DF


    def _process_1_min_data_qq(self,data, stock_code):
        self._1_min_data_DF.drop(self._1_min_data_DF.index[:], inplace=True)
        today = datetime.date.today().strftime('%Y-%m-%d')
        sPos = data.find('{"data":[')
        ePos = data.find('"date":')
        data = data[(sPos + 9):(ePos - 2)].split(',')
        p = re.compile(r'\d+\.*\d+')
        for item in data:
            tmp_l = p.findall(item)
            tmp_l[0]=today + ' ' + tmp_l[0]
            tmp_l[0]=datetime.datetime.strptime(tmp_l[0],'%Y-%m-%d %H%M')
            tmp_l[1] = float(tmp_l[1])
            tmp_l[2] = float(tmp_l[2])
            tmp_l.append(stock_code)
            self._1_min_data_DF.loc[len(self._1_min_data_DF)] = tmp_l
        self._1_min_data_DF.set_index('quote_time', inplace=True)
        print "processed 1 min data of %s" % stock_code
        return self._1_min_data_DF

    def _process_x_min_data_qq(self, data, x_min, stock_code):
        self._x_min_data_DF.drop(self._x_min_data_DF.index[:], inplace=True)
        #print self._x_min_data_DF

        nPos = data.find(x_min)
        ePos = data.find("prec")
        data = data[(nPos + 7):(ePos - 4)].split('],[')
        p = re.compile(r'\d+.\d+')

        for item in data:
            tmp_l = p.findall(item)
            tmp_l[0] = datetime.datetime.strptime(tmp_l[0],'%Y%m%d%H%M')
            i = 1
            while i < 6:
                tmp_l[i] = float(tmp_l[i])
                i += 1
            tmp_l.append(stock_code)
            tmp_l.append(x_min)
            #print len(self._x_min_data_DF)
            self._x_min_data_DF.loc[len(self._x_min_data_DF)] = tmp_l
            #print self._x_min_data_DF['quote_time']
        self._x_min_data_DF.set_index('quote_time', inplace=True)
        print "processed %s data of stock %s" % (x_min, stock_code)
        #print self._x_min_data_DF
        return self._x_min_data_DF


    def _save_data_to_db_qq(self, period, stock_code):
        # save today's data (1min, 5min, 30min), min data will be downloaded and saved into DB in every 30 min from 9:30
        # Data will be exanmed to avoid inserting duplicate rows into DB.
        # Columns: stock_code, period, quote_time will be used to exanmed the duplicated row.
        data = self._x_min_data_DF
        if period in self._x_min:
            # save x min data into DB
            data = self._remove_overlaied_rows(period, stock_code)
            if period != 'm1':
                data = self._remove_unwant_min_rows(data, stock_code)
                data.to_sql('tb_StockXMinRecords', self._engine, if_exists='append', index=True)
                self._remove_zero_trading_volumn_rows(period, stock_code)
                print "saved %s period data of stock code %s at time %s" % (period, stock_code, self._time_tag())
            else:
                data.to_sql('tb_Stock1MinRecords', self._engine, if_exists='append', index=True)
                print "saved m1 data of stock code %s at %s" % (stock_code, self._time_tag())
        elif period in self._x_period:
            # save historical data (day, week), historical data will be downloaded and saved into DB when it is required.
            data = self._remove_overlaied_rows(period, stock_code)
            data.to_sql('tb_StockXPeriodRecords', self._engine, if_exists='append', index=True)
            print "saved %s period data of stock code %s at %s" % (period, stock_code, self._time_tag())
        else:
            # save real_time data, real time data will be downloaded and saved into DB in every 2 sec.
            data = self._real_time_data_DF
            data.to_sql('tb_StockRealTimeRecords', self._engine, if_exists='append', index=False)

        self._log_mesg = self._log_mesg + "At %s Data Saving: Stock %s period %s data has been saved. \n" % (
            self._time_tag(), stock_code, period)
        self._write_log(self._log_mesg)

    def _remove_overlaied_rows(self, period, stock_code):
        '''
        Step 1: 从数据库中取出相应股票最后一个记录的时间戳
        Step 2：从data 删除早于这个时间戳的记录
        Step 3: 返回清理后的DataFrame
        :param df:
        :return:
        '''
        # print "Jump into remove duplication"
        conn = self._engine.connect()
        if period in self._x_min: # process X min data
            if period != 'm1':
                tb = Table('tb_StockXMinRecords', self._metadata, autoload=True)
                s = select([tb.c.quote_time]). \
                    where(
                    and_(tb.c.period == period,
                         tb.c.stock_code == stock_code)). \
                    order_by(tb.c.quote_time.desc()).limit(1)
                data = self._x_min_data_DF
                # Insert opening prices for x_min data except m1
                #data = self._insert_opening_record(data)
            else:# Process 1 min data
                tb = Table('tb_Stock1MinRecords', self._metadata, autoload=True)
                s = select([tb.c.quote_time]). \
                    where(tb.c.stock_code == stock_code). \
                    order_by(tb.c.quote_time.desc()).limit(1)
                data = self._1_min_data_DF
        else: # process day / week data
            tb = Table('tb_StockXPeriodRecords', self._metadata, autoload = True)
            s = select([tb.c.quote_time]). \
                where(
                and_(tb.c.period == period,
                     tb.c.stock_code == stock_code)). \
                order_by(tb.c.quote_time.desc()).limit(1)
            data = self._x_min_data_DF

        result = conn.execute(s).fetchall()
        if len(result) != 0:
            last_record_time = result[0][0]
            data = data[data.index > last_record_time]
            print "Overlaied rows has been removed."
        else:
            print "Overlaied rows has been removed."

        self._log_mesg = self._log_mesg + "At %s Overlaied: Overlaied rows for Stock %s period %s data has been removed. \n" % (
            self._time_tag(), stock_code, period)
        self._write_log(self._log_mesg)
        conn.close()
        return data

    def _remove_zero_trading_volumn_rows(self, period, stock_code):
        '''
        Some time in ShangHai stocks m30 records, duplicated M30 rows with 0 trading_volumns shows in DB, these rows
        need to be deleted.
        :param period:
        :param stock_code:
        :return:
        '''
        conn = self._engine.connect()
        tb = Table('tb_StockXMinRecords', self._metadata, autoload=True)
        d = tb.delete(and_(tb.c.stock_code == stock_code, tb.c.period == period, tb.c.trading_volumn == 0))

        # s = select(tb.c.id_tb_StockXMinRecords where (and_(tb.c.stock_code == stock_code, tb.c.period == period, tb.c.trading_volumn == 0))))
        # d1 = tb.delete(tb.c.id_tb_StockXMinRecords == s.id_tb_StockXMinRecords)
        # s = delete FROM tb where tb.c.id_tb_StockXMinRecords = (select a.id_tb_StockXMinRecords from (SELECT id_tb_StockXMinRecords FROM DB_StockDataBackTest.tb_StockXMinRecords where stock_code = 'sh600867' and period='m30' and trading_volumn = 0) as a);
        conn.execute(d)
        conn.close()
        print "Zero_trading_volumn_rows be removed"
        self._log_mesg = self._log_mesg + "At %s Zero_trading_volumn_rows: zero_trading_volumn rows for Stock %s period %s data has been removed. \n" % (
            self._time_tag(), stock_code, period)
        self._write_log(self._log_mesg)

    def _remove_unwant_min_rows(self, df, stock_code):
        '''
        As the getting data process cannot be runned at exact minitue, the system will download some unwanted data.
        Ex: want the getting m30 data was runned at 10:39:00, then beside 10:30:00 record, another 10:39:00 m30 records
        will also be downloaded and saved.
        This function is to remove stock records which are not exactly at exactly want minitues stamps.
        :param df: DF fram which has been processed by remove_duplicate_rows
        :return: a cleaner DF
        '''
        if len(df['open_price']) == 0:
            return df

        for idx, row in df.iterrows():
            # period = df.period[0]
            # download_min = df.index[0].minute
            #download_sec = df.index[0].second

            period = row.period
            # print row.name
            download_min = row.name.minute
            # print download_min
            download_sec = row.name.second

            if period == 'm5':
                if (int(download_min) % 5 != 0) or (int(download_sec) != 0):
                    df.drop(idx, inplace=True)
            elif period == 'm15':
                if (int(download_min) % 15 != 0) or (int(download_sec) != 0):
                    df.drop(idx, inplace=True)
            elif period == 'm30':
                if (int(download_min) % 30 != 0) or (int(download_sec) != 0):
                    df.drop(idx, inplace=True)
            elif period == 'm60':
                if (int(download_min) != 0) or (int(download_sec) != 0):
                    df.drop(idx, inplace=True)
            else:
                pass

        self._log_mesg = self._log_mesg + "At %s Unwanted: Unwanted rows for Stock %s period %s data has been removed. \n" % (
        self._time_tag(),stock_code, period)
        self._write_log(self._log_mesg)
        print "Unwanted Min Records have been removed."
        return df

    def _insert_opening_record(self, df):
        '''
        By observing the data records, I found there is no 9:30 opening records for each day. This missing records will
        casue the first caulation between 9:30 and 10:00 is the actually the compare of 10:00 today and 15:00 yesterday.
         The program will not be able to consider the difference of today's opening and yesterday's closing.
         So: A opening records is inserted by this function with all prices = today's opening and volumn = 0
         This function affect m5, m15, m30 and m60 records
        :param df:
        :return:
        '''
        if len(df['open_price']) == 0:
            return df

        # This part is to select different trading dates
        dates = df.ix[:, 0:0].reset_index()
        dates['date'] = 0
        for idx, row in dates.iterrows():
            dates.set_value(idx, 'date', row['quote_time'].date())
        dates.drop_duplicates('date', keep='first', inplace=True)

        # For each trading days, insert opening row
        for idx, row in dates.iterrows():
            # tday = datetime.datetime.today()
            year = row['date'].year
            month = row['date'].month
            day = row['date'].day
            open_stamp = datetime.datetime(year, month, day, 9, 30, 0, 0)

            # From the 10:00 record to retrieve open_price
            first_stamp = datetime.datetime(year, month, day, 10, 0, 0, 0)

            if first_stamp in df.index:
                first_stamp_open = df.get_value(first_stamp, 'open_price')
                stock_code = df.get_value(first_stamp, 'stock_code')
                period = df.get_value(first_stamp, 'period')
                # Insert 9:30 record for each day
                df.ix[open_stamp] = [first_stamp_open, first_stamp_open, first_stamp_open, first_stamp_open, 0,
                                     stock_code, period]
        df = df.sort_index()
        df.index.names = ['quote_time']
        print "Opening Record has been inserted."
        return df


    def _timer(self, func):
        # 定义一个计时器函数，让get_real_time_data 每60秒向数据库传送一次更新的数据。
        #定义一个内嵌的包装函数，给传入的函数加上计时功能的包装
        def wrapper():
            start = time.time()
            while (time.time() - start) < 900: # call function at every 15 min
                func(None, None, None, None)
                end = time.time()
                time.sleep(5)
                print "time spent of each run = ", (end - start)
        return wrapper

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local


    def _time_tag_dateonly(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return only_date

    '''
     def _write_log(self,log_mesg, logPath):
        fullPath = self._output_dir + logPath
        with open(fullPath, 'a') as log:
            log.writelines(log_mesg)
    '''

    def _write_log(self, log_mesg, logPath='DataLog.txt'):
        # logPath = str(self._time_tag_dateonly()) + logPath
        fullPath = self._output_dir + logPath
        if isinstance(log_mesg, str):
            with open(fullPath, 'a') as log:
                log.writelines(log_mesg)
        else:
            for message in log_mesg:
                with open(fullPath, 'a') as log:
                    log.writelines(message)
        self._log_mesg = ''

    def _convert_encoding(self, lines, new_coding='UTF-8'):
        try:
            encoding = 'GB2312'
            data = lines.decode(encoding)
            data = data.encode(new_coding)
        except:
            data = 'DecodeError'
        return data

    def _empty_fun(self, period):
        pass

    '''
    def job_schedule(self, period=None, stock_code=None):
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

        scheduler_1.add_job(self._data_service, 'cron', day_of_week='mon-fri', hour='9-15', minute='5/15',
                            args=['m1'])
        scheduler_1.add_job(self._data_service, 'cron', day_of_week='mon-fri', hour='9-15', minute='7/15',
                            args=['m15'])
        scheduler_1.add_job(self._data_service, 'cron', day_of_week='mon-fri', hour='9-15', minute='1/30',
                            args=['m30'])
        scheduler_1.add_job(self._data_service, 'cron', day_of_week='mon-fri', hour='9-15', minute='10/30',
                            args=['m60'])
        scheduler_1.add_job(apply_pattern, 'cron', day_of_week='mon-fri', hour='9-15', minute='3/30',
                            args=[period, stock_code])
        scheduler_1.add_job(update_stock_inhand, 'cron', day_of_week='mon-fri', hour='9-15', minute='1/30')

        scheduler_2.add_job(best_pattern_daily_calculate, 'cron', day_of_week='fri', hour='22')

        scheduler_1.start()
        scheduler_2.start()

        # The switch of scheduler_1
        while True:
            self._scheduler_switch(scheduler_1, scheduler_2)


    def _scheduler_switch(self, scheduler_1, scheduler_2):
        current_time = datetime.datetime.now().time()

        if (current_time > self._start_morning and current_time < self._end_morning) or (
                        current_time > self._start_afternoon and current_time < self._end_afternoon):
            scheduler_2.pause()
            scheduler_1.resume()
            scheduler_1.print_jobs()
            scheduler_2.print_jobs()
            print "scheduler_1 back to work"
            #time.sleep(600)
        else:
            print "out of the time of getting data"
            scheduler_1.pause()
            scheduler_2.resume()
            scheduler_2.print_jobs()

        time.sleep(60)


    def _data_service(self, period):
        processes = []
        for stock in self._stock_code:
            p = mp.Process(target=self.get_data_qq, args=(stock, period, 'qfq',))
            processes.append(p)

        for p in processes:
            p.start()

        for p in processes:
            p.join()
    '''


def main():
    pp = C_GettingData()
    # pp.job_schedule()
    #pp.get_real_time_data('sina', 'sz300226')
    #pp.get_real_time_data(None, None)
    #pp.save_real_time_data_to_db()
    #pp.service_getting_data()
    # pp.get_data_qq(stock_code='sz002310', period='day')
    #pp.get_data_qq(stock_code='sz002310',period='m1')
    #pp.get_data_qq(period='real')
    pp.get_data_qq(stock_code='sz002310', period='m30', q_count=800)
    #pp._data_service('m30')
    # pp.get_data_qq(stock_code='sh600221', period='day')
    #pp.get_data_qq(stock_code='sh600221',period='week')

if __name__ == '__main__':
    main()
