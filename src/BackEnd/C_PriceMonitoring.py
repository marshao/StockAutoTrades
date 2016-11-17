#!/usr/local/bin/python
# coding: GBK

__metclass__ = type


import os, time, pandas, urllib
import datetime
from sqlalchemy import create_engine


class C_PriceMonitoring:
    '''
    This is the class to monitor the stock real time price from various resources: (Sina, SnowBall, etc)
    '''

    def __init__(self):
        self._output_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\'
        self._input_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._install_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\'

        self._data_source={'sina':'http://hq.sinajs.cn/list=','snowball':''}
        self._period = ['15min','30min','60min']
        self._stock_code = ['sz300226', 'sh600887']
        self._log_mesg = ''
        self._op_log = 'operLog.txt'
        self._engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')
        self._my_columns = ['stock_code', 'close_price', 'open_price', 'current_price', 'high_price','low_price','buy_price','sale_price','trading_volumn','trading_amount',
                   'buy1_apply','buy1_price','buy2_apply','buy2_price','buy3_apply','buy3_price','buy4_apply','buy4_price','buy5_apply','buy5_price',
                   'sale1_apply','sale1_price','sale2_apply','sale2_price','sale3_apply','sale3_price','sale4_apply','sale4_price','sale5_apply','sale5_price',
                   'today','totime']
        self._stock_minitue_data = pandas.DataFrame(columns = self._my_columns)
        self._start_morning = datetime.time(9,30,0)
        self._end_morning = datetime.time(11,30,0)
        self._start_afternoon = datetime.time(13,30,0)
        self._end_afternoon = datetime.time(15,30,0)



    def get_real_time_data(self, data_source, stock_code):
        # 此函数负责拾取每60秒的数据更新
        per_real_data = self.price_monitoring(data_source, stock_code)
        # 将返回的per_real_data 增加到DF stock_real_data中
        print "new data found at ",self._time_tag()
        for row in per_real_data:
            self._stock_minitue_data.loc[len(self._stock_minitue_data)] = row


    def price_monitoring(self, data_source, stock_code):
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
        per_real_data = self._process_real_time_data(real_data)
        return  per_real_data

    def _timer(self, func):
        # 定义一个计时器函数，让get_real_time_data 每60秒向数据库传送一次更新的数据。

        #定义一个内嵌的包装函数，给传入的函数加上计时功能的包装
        def wrapper():
            start = time.clock()
            while (time.clock() - start) < 60:
                func(None, None)
                end = time.clock()
                time.sleep(5)
                print "time spent of each run = ", (end - start)
        return wrapper

    def _process_real_time_data(self, real_data):
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



    def save_real_time_data_to_db(self):
        get_real_time_data = self._timer(self.get_real_time_data)
        while True:
            current_time = datetime.datetime.now().time()
            if (current_time > self._start_morning and current_time < self._end_morning) or (current_time > self._start_afternoon and current_time < self._end_afternoon):
                # Need a while true loop in here to keep hearing the real time data
                get_real_time_data()
                self._stock_minitue_data.to_sql('tb_StockRealTimeData', self._engine, if_exists='append', index=False)
                time.sleep(5)
                #print "Saved data into DB"
                self._stock_minitue_data.drop(self._stock_minitue_data.index[:], inplace= True)
            else:
                print "Not in transaction time, wait 10 min to try again."
                time.sleep(600)

    def _calculate_period_data_from_real_time(self):
        pass

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
    pp = C_PriceMonitoring()
    #pp.get_real_time_data('sina', 'sz300226')
    #pp.get_real_time_data(None, None)
    pp.save_real_time_data_to_db()

if __name__ == '__main__':
    main()
