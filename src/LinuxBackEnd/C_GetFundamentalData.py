#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import time, urllib, re, chardet
import pandas as pd
import datetime
from sqlalchemy import Table, MetaData
from sqlalchemy.sql import select, and_
from sqlalchemy.orm import sessionmaker
import C_GlobalVariable as glb
import logging



class C_GetFundamentalData:
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
        self._my_real_time_DF_columns_sina = ['stock_code', 'close_price', 'open_price', 'current_price', 'high_price',
                                              'low_price', 'buy_price', 'sale_price', 'trading_volumn',
                                              'trading_amount',
                                              'buy1_apply', 'buy1_price', 'buy2_apply', 'buy2_price', 'buy3_apply',
                                              'buy3_price', 'buy4_apply', 'buy4_price', 'buy5_apply', 'buy5_price',
                                              'sale1_apply', 'sale1_price', 'sale2_apply', 'sale2_price', 'sale3_apply',
                                              'sale3_price', 'sale4_apply', 'sale4_price', 'sale5_apply', 'sale5_price',
                                              'today', 'totime']
        self._real_time_DF_columns_qq = ['stock_name', 'stock_code', 'current_price', 'close_price', 'open_price',
                                         'current_buy_amount', 'current_sale_amount',
                                         'buy1_price', 'buy1_apply', 'buy2_price', 'buy2_apply', 'buy3_price',
                                         'buy3_apply',
                                         'buy4_price', 'buy4_apply', 'buy5_price', 'buy5_apply',
                                         'sale1_price', 'sale1_apply', 'sale2_price', 'sale2_apply', 'sale3_price',
                                         'sale3_apply',
                                         'sale4_price', 'sale4_apply', 'sale5_price', 'sale5_apply',
                                         'time', 'net_chg', 'net_chg_percent', 'high_price', 'low_price',
                                         'total_volumn', 'total_amount',
                                         'turnover_rate', 'PE', 'circulation_market_value', 'total_market_value', 'PB',
                                         'limit_up', 'limit_down']
        self._x_min_columns = ['quote_time', 'open_price', 'high_price', 'low_price', 'close_price', 'trading_volumn',
                               'stock_code', 'period']
        self._1_min_columns = ['quote_time', 'price', 'trading_volumn', 'stock_code']
        self._stock_minitue_data_DF = pd.DataFrame(columns=self._my_real_time_DF_columns_sina)
        self._x_min_data_DF = pd.DataFrame(columns=self._x_min_columns)
        self._1_min_data_DF = pd.DataFrame(columns=self._1_min_columns)
        self._real_time_data_DF = pd.DataFrame(columns=self._real_time_DF_columns_qq)

        self._fun = self._empty_fun

    def get_single_stock_main_financail_indicators(self, stock_code_m=None):
        if stock_code_m is None:
            stock_code_m = 'sz300226'
        stock_code = stock_code_m[2:]

        # Read data from Web
        url_principle_financial_index = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=report' % stock_code
        html = urllib.urlopen(url_principle_financial_index)
        data = html.read()
        empty = re.compile('--')
        data = empty.sub('0', data)
        ul1 = re.compile('\n')
        data = ul1.sub('', data)

        # Process Web data into a DF
        rows = []
        endata = data.decode('GB2312').encode('utf-8').split('\r')[0:20]
        for line in endata:
            line = line.split(',')
            rows.append(line)
            # columns.append(line[0])
        df = pd.DataFrame(rows).T
        df.columns = df.iloc[0]
        df = df.iloc[1:, :]
        df['报告日期'] = df['报告日期'].astype('datetime64[ns]')

        for column in df.columns:
            if column != '报告日期':
                df[column] = pd.to_numeric(df[column], errors='raise')

        df = df.dropna()
        df['stock_code'] = stock_code_m

        # Prepare DB Sessions
        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        main_financail_indicators = Table('tb_StockMainFinancialIndicators', meta, autoload=True)
        # As the SQLAlchemy reading the table columns as unicode, so have to decode the columns names of DF

        fc = ['stock_code', 'stock_name', '报告日期'.decode('utf-8'), '基本每股收益(元)'.decode('utf-8'),
              '每股净资产(元)'.decode('utf-8'), '每股经营活动产生的现金流量净额(元)'.decode('utf-8'),
              '主营业务收入(万元)'.decode('utf-8'), '主营业务利润(万元)'.decode('utf-8'), '营业利润(万元)'.decode('utf-8'),
              '投资收益(万元)'.decode('utf-8'), '营业外收支净额(万元)'.decode('utf-8'), '利润总额(万元)'.decode('utf-8'),
              '净利润(万元)'.decode('utf-8'), '净利润(扣除非经常性损益后)(万元)'.decode('utf-8'),
              '经营活动产生的现金流量净额(万元)'.decode('utf-8'), '现金及现金等价物净增加额(万元)'.decode('utf-8'),
              '总资产(万元)'.decode('utf-8'), '流动资产(万元)'.decode('utf-8'), '总负债(万元)'.decode('utf-8'),
              '流动负债(万元)'.decode('utf-8'), '股东权益不含少数股东权益(万元)'.decode('utf-8'), '净资产收益率加权'.decode('utf-8')]

        session.execute(
            main_financail_indicators.insert(),
            [{fc[0]: stock_code_m,
              fc[1]: '',
              fc[2]: row['报告日期'],
              fc[3]: row['基本每股收益(元)'],
              fc[4]: row['每股净资产(元)'],
              fc[5]: row['每股经营活动产生的现金流量净额(元)'],
              fc[6]: row['主营业务收入(万元)'],
              fc[7]: row['主营业务利润(万元)'],
              fc[8]: row['营业利润(万元)'],
              fc[9]: row['投资收益(万元)'],
              fc[10]: row['营业外收支净额(万元)'],
              fc[11]: row['利润总额(万元)'],
              fc[12]: row['净利润(万元)'],
              fc[13]: row['净利润(扣除非经常性损益后)(万元)'],
              fc[14]: row['经营活动产生的现金流量净额(万元)'],
              fc[15]: row['现金及现金等价物净增加额(万元)'],
              fc[16]: row['总资产(万元)'],
              fc[17]: row['流动资产(万元)'],
              fc[18]: row['总负债(万元)'],
              fc[19]: row['流动负债(万元)'],
              fc[20]: row['股东权益不含少数股东权益(万元)'],
              fc[21]: row['净资产收益率加权(%)']
              } for idx, row in df.iterrows()]
        )
        session.commit()
        session.close()

    def get_stock_running_indicators(self, stock_code_m=None):
        if stock_code_m is None:
            stock_code_m = 'sz300226'
        stock_code = stock_code_m[2:]

        # Read data from Web
        url_ylnl_indicators = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=ylnl' % stock_code
        url_chnl_indicators = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=chnl' % stock_code
        url_cznl_indicators = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=cznl' % stock_code
        url_yynl_indicators = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=yynl' % stock_code

        html = urllib.urlopen(url_ylnl_indicators)
        ylnl = html.read()
        empty = re.compile('--')
        ylnl = empty.sub('0', ylnl)
        ul1 = re.compile('\n')
        data = ul1.sub('', ylnl)

        # Process Web data into a DF
        rows = []
        endata = data.decode('GB2312').encode('utf-8').split('\r')[0:20]
        # print endata
        for line in endata:
            line = line.split(',')
            rows.append(line)
            # columns.append(line[0])
        df = pd.DataFrame(rows).T
        df.columns = df.iloc[0]
        df = df.iloc[1:, :]
        df['报告日期'] = df['报告日期'].astype('datetime64[ns]')
        print df
        for column in df.columns:
            if column != '报告日期':
                pass
                # df[column] = pd.to_numeric(df[column], errors='raise')

        df = df.dropna()
        df['stock_code'] = stock_code_m
        # print df

        # Prepare DB Sessions
        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        main_financail_indicators = Table('tb_StockMainFinancialIndicators', meta, autoload=True)
        # As the SQLAlchemy reading the table columns as unicode, so have to decode the columns names of DF

        fc = ['stock_code', 'stock_name', '报告日期'.decode('utf-8'), '基本每股收益(元)'.decode('utf-8'),
              '每股净资产(元)'.decode('utf-8'), '每股经营活动产生的现金流量净额(元)'.decode('utf-8'),
              '主营业务收入(万元)'.decode('utf-8'), '主营业务利润(万元)'.decode('utf-8'), '营业利润(万元)'.decode('utf-8'),
              '投资收益(万元)'.decode('utf-8'), '营业外收支净额(万元)'.decode('utf-8'), '利润总额(万元)'.decode('utf-8'),
              '净利润(万元)'.decode('utf-8'), '净利润(扣除非经常性损益后)(万元)'.decode('utf-8'),
              '经营活动产生的现金流量净额(万元)'.decode('utf-8'), '现金及现金等价物净增加额(万元)'.decode('utf-8'),
              '总资产(万元)'.decode('utf-8'), '流动资产(万元)'.decode('utf-8'), '总负债(万元)'.decode('utf-8'),
              '流动负债(万元)'.decode('utf-8'), '股东权益不含少数股东权益(万元)'.decode('utf-8'), '净资产收益率加权'.decode('utf-8')]

        '''
        session.execute(
            main_financail_indicators.insert(),
            [{fc[0]: stock_code_m,
              fc[1]: '',
              fc[2]: row['报告日期'],
              fc[3]: row['基本每股收益(元)'],
              fc[4]: row['每股净资产(元)'],
              fc[5]: row['每股经营活动产生的现金流量净额(元)'],
              fc[6]: row['主营业务收入(万元)'],
              fc[7]: row['主营业务利润(万元)'],
              fc[8]: row['营业利润(万元)'],
              fc[9]: row['投资收益(万元)'],
              fc[10]: row['营业外收支净额(万元)'],
              fc[11]: row['利润总额(万元)'],
              fc[12]: row['净利润(万元)'],
              fc[13]: row['净利润(扣除非经常性损益后)(万元)'],
              fc[14]: row['经营活动产生的现金流量净额(万元)'],
              fc[15]: row['现金及现金等价物净增加额(万元)'],
              fc[16]: row['总资产(万元)'],
              fc[17]: row['流动资产(万元)'],
              fc[18]: row['总负债(万元)'],
              fc[19]: row['流动负债(万元)'],
              fc[20]: row['股东权益不含少数股东权益(万元)'],
              fc[21]: row['净资产收益率加权(%)']
              } for idx, row in df.iterrows()]
        )
        session.commit()
        session.close()
        '''


    def _timer(self, func):
        def wrapper():
            start = time.time()
            while (time.time() - start) < 900:  # call function at every 15 min
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

    def _char_detect(self, data):
        print chardet.detect(data)

    def _empty_fun(self, period):
        pass


def main():
    pp = C_GetFundamentalData()
    # pp.get_single_stock_main_financail_indicators()
    pp.get_stock_running_indicators()

if __name__ == '__main__':
    main()
