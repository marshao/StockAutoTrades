#!/usr/local/bin/python
# coding: GBK

__metclass__ = type


import os, time, pandas, urllib, re
import datetime
from sqlalchemy import create_engine, Table, Column, MetaData
from sqlalchemy.sql import select, and_, or_, not_
from PatternApply import apply_pattern, best_pattern_daily_calculate


class C_GettingData:
    '''
    This is the class to monitor the stock real time price from various resources: (Sina, SnowBall, etc)
    '''

    def __init__(self):
        self._output_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\'
        self._input_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._install_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\'

        self._data_source={'sina':'http://hq.sinajs.cn/list=','qq_realtime':'http://qt.gtimg.cn/q=%s',
                           'qq_1_min':'http://web.ifzq.gtimg.cn/appstock/app/minute/query?_var=min_data_%s&code=%s',
                           'qq_x_min':'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=%s,%s,,%s',
                           'qq_x_period': 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,%s,,,%s,%s'}
        self._x_min = ['m1','m5','m15','m30','m60']
        self._x_period = ['day', 'week']
        self._q_count = ['320','50','16','8','4']
        self._fq = ['qfq', 'hfq','bfq']
        self._stock_code = ['sz300226', 'sh600887', 'sz300146', 'sh600221']
        # self._stock_code = ['sz300226']
        self._log_mesg = ''
        self._op_log = 'operLog.txt'
        self._engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')
        self._metadata = MetaData(self._engine)
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
        self._start_morning = datetime.time(9,31,0)
        self._end_morning = datetime.time(11,31,0)
        self._start_afternoon = datetime.time(13, 0, 0)
        self._end_afternoon = datetime.time(18,30,0)


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

    def get_data_qq(self, stock_code='sz300226', period='day', fq='qfq', q_count='320'):
        self._stock_minitue_data_DF = pandas.DataFrame(columns=self._my_real_time_DF_columns_sina)
        self._x_min_data_DF = pandas.DataFrame(columns=self._x_min_columns)
        self._1_min_data_DF = pandas.DataFrame(columns=self._1_min_columns)
        got = True
        try:
            if period in self._x_period:  # precess day/week data
                fq = ('qfq' if fq not in (self._fq) else fq)
                url = self._data_source['qq_x_period'] % (stock_code, period, q_count, fq)
                html = urllib.urlopen(url)
                data = html.read()
                self._process_x_period_data_qq(data, period, fq, stock_code)
                self._save_data_to_db_qq(period, stock_code)
            elif period in (self._x_min):
                if period == 'm1':  # process 1 min data
                    url = self._data_source['qq_1_min'] % (stock_code, stock_code)
                    html = urllib.urlopen(url)
                    data = html.read()
                    self._process_1_min_data_qq(data, stock_code)
                    self._save_data_to_db_qq(period, stock_code)
                else:  # process X min data
                    if period == 'm5':
                        q_count = self._q_count[1]
                        url = self._data_source['qq_x_min'] % (stock_code, period, q_count)
                    elif period == 'm15':
                        q_count = self._q_count[2]
                        url = self._data_source['qq_x_min'] % (stock_code, period, q_count)
                    elif period == 'm30':
                        q_count = self._q_count[3]
                        url = self._data_source['qq_x_min'] % (stock_code, period, q_count)
                    else:
                        q_count = self._q_count[4]
                        url = self._data_source['qq_x_min'] % (stock_code, period, q_count)
                    html = urllib.urlopen(url)
                    data = html.read()
                    self._process_x_min_data_qq(data, period, stock_code)
                    self._save_data_to_db_qq(period, stock_code)
            else:  # process real time data
                url = self._data_source['qq_realtime'] % stock_code
                html = urllib.urlopen(url)
                data = html.read()
                self._process_real_time_data_qq(data)
                self._save_data_to_db_qq(period, stock_code)
        except:
            got = False
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
        print "processed 1 min data"
        return self._1_min_data_DF

    def _process_x_min_data_qq(self, data, x_min, stock_code):
        self._x_min_data_DF.drop(self._x_min_data_DF.index[:], inplace=True)
        #print self._x_min_data_DF
        nPos = data.find(x_min)
        data = data[(nPos + 7):-5].split('],[')
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
        print "processed %s data" %x_min
        return self._x_min_data_DF

    def _save_data_to_db_qq(self, period, stock_code):
        # save today's data (1min, 5min, 30min), min data will be downloaded and saved into DB in every 30 min from 9:30
        # Data will be exanmed to avoid inserting duplicate rows into DB.
        # Columns: stock_code, period, quote_time will be used to exanmed the duplicated row.
        data = self._x_min_data_DF
        if period in self._x_min:
            # save x min data into DB
            data = self._remove_duplicate_rows(period, stock_code)
            if period != 'm1':
                data.to_sql('tb_StockXMinRecords', self._engine, if_exists='append', index=True)
            else:
                data.to_sql('tb_Stock1MinRecords', self._engine, if_exists='append', index=True)
        elif period in self._x_period:
            # save historical data (day, week), historical data will be downloaded and saved into DB when it is required.
            data = self._remove_duplicate_rows(period, stock_code)
            data.to_sql('tb_StockXPeriodRecords', self._engine, if_exists='append', index=True)
        else:
            # save real_time data, real time data will be downloaded and saved into DB in every 2 sec.
            data = self._real_time_data_DF
            data.to_sql('tb_StockRealTimeRecords', self._engine, if_exists='append', index=False)

    def _remove_duplicate_rows(self, period, stock_code):
        '''
        Step 1: 从数据库中取出相应股票最后一个记录的时间戳
        Step 2：从data 删除早于这个时间戳的记录
        Step 3: 返回清理后的DataFrame
        :param df:
        :return:
        '''
        print "Jump into remove duplication"
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
            print result
            last_record_time = result[0][0]
            print last_record_time
            #data = data.loc[lambda df: df.index > last_record_time, :]
            data = data[data.index > last_record_time]
            return data
        else:
            return data

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

    def _write_log(self,log_mesg, logPath):
        fullPath = self._output_dir + logPath
        with open(fullPath, 'a') as log:
            log.writelines(log_mesg)

    def _convert_encoding(self, lines, new_coding='UTF-8'):
        try:
            encoding = 'GB2312'
            data = lines.decode(encoding)
            data = data.encode(new_coding)
        except:
            data = 'DecodeError'
        return data

def main():
    pass
    #pp = C_GettingData()
    #pp.get_real_time_data('sina', 'sz300226')
    #pp.get_real_time_data(None, None)
    #pp.save_real_time_data_to_db()
    #pp.service_getting_data()
    # pp.get_data_qq(stock_code='sz300146', period = 'm5')
    # pp.get_data_qq(stock_code='sz300146',period='m1')
    #pp.get_data_qq(period='real')
    # pp.get_data_qq(stock_code='sz300146', period='m30')
    # pp.get_data_qq(stock_code='sh600221', period='day')
    #pp.get_data_qq(stock_code='sh600221',period='week')

if __name__ == '__main__':
    main()
