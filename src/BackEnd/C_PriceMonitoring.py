#!/usr/local/bin/python
# coding: GBK

__metclass__ = type


import os, time, pandas, urllib
from datetime import datetime
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



    def get_real_time_data(self, data_source, stock_code):
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
        real_date = html.read()
        #print real_date
        self._process_real_time_data(real_date)

    def price_monitoring(self):
        pass

    def _process_real_time_data(self, real_data):
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
                            stock_data_list.append('stock_code')
                            i += 1
                        else:
                            i += 1
                    else:
                        i += 1
                stock_data_ll.append(stock_data_list)
                stock_data_list = []
        print stock_data_ll
        # here is not finish yet, still need to return the list back



    def _save_real_time_data_to_db(self, stock_code, period):
        pass

    def _calculate_period_data_from_real_time(self):
        pass

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local


    def _time_tag_dateonly(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.now()
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
    pp.get_real_time_data(None, None)

if __name__ == '__main__':
    main()
