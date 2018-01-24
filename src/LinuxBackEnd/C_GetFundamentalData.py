#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import sys

sys.path.append('/home/marshao/DataMiningProjects/Project_StockAutoTrade_LinuxBackEnd/StockAutoTrades/src')

import time, urllib, re, chardet
import pandas as pd
import datetime
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import sessionmaker
import C_GlobalVariable as glb




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

    def get_stock_main_financail_indicators(self, stock_code_m=None):
        '''
        This function is to get the financial indicators of specific stock from web
        :param stock_code_m:
        :return:
        '''
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

    def get_stock_operation_indicators(self, stock_code_m=None):
        if stock_code_m is None:
            stock_code_m = 'sz300226'
        stock_code = stock_code_m[2:]

        # Defining Web URL
        url_ylnl_indicators = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=ylnl' % stock_code
        url_chnl_indicators = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=chnl' % stock_code
        url_cznl_indicators = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=cznl' % stock_code
        url_yynl_indicators = 'http://quotes.money.163.com/service/zycwzb_%s.html?type=report&part=yynl' % stock_code
        url = [url_ylnl_indicators, url_chnl_indicators, url_cznl_indicators, url_yynl_indicators]
        columns = ['报告日期'.decode('utf-8'), ]
        indicator_data = pd.DataFrame()

        # Getting web data
        for each_url in url:
            html = urllib.urlopen(each_url)
            data = html.read()
            no_value = re.compile('--')
            data = no_value.sub('0', data)
            ul1 = re.compile('\n')
            data = ul1.sub('', data)

            # Process Web data into a DF
            rows = []
            endata = data.decode('GB2312').encode('utf-8').split('\r')

            # print endata
            for line in endata:
                line = line.split(',')
                if (line[0] != '\t\t') & (line[0] != ''):
                    rows.append(line)
                    if line[0] != '报告日期':
                        # create column name list
                        percent = re.compile('\(%\)')
                        col_name = percent.sub('', line[0]).decode('utf-8')
                        columns.append(col_name)

            df = pd.DataFrame(rows).T
            df.columns = df.iloc[0]
            df = df.iloc[1:, :]
            df['报告日期'] = df['报告日期'].astype('datetime64[ns]')
            df.set_index('报告日期', inplace=True)
            for column in df.columns:
                if column != '报告日期':
                    df = df.dropna()
                    df[column] = pd.to_numeric(df[column], errors='raise')

            if indicator_data.shape[1] == 0:
                indicator_data = df
            else:
                indicator_data = pd.concat([indicator_data, df], axis=1, join_axes=[indicator_data.index])

        indicator_data['stock_code'] = stock_code_m

        # Prepare DB Sessions
        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        main_financail_indicators = Table('tb_StockOperateIndicators', meta, autoload=True)
        # As the SQLAlchemy reading the table columns as unicode, so have to decode the columns names of DF
        # print indicator_data.iloc[25:33, 5:11]
        session.execute(
            main_financail_indicators.insert(),
            [{'stock_code': stock_code_m,
              columns[0]: idx,
              columns[1]: row['总资产利润率(%)'],
              columns[2]: row['主营业务利润率(%)'],
              columns[3]: row['总资产净利润率(%)'],
              columns[4]: row['成本费用利润率(%)'],
              columns[5]: row['营业利润率(%)'],
              columns[6]: row['主营业务成本率(%)'],
              columns[7]: row['销售净利率(%)'],
              columns[8]: row['净资产收益率(%)'],
              columns[9]: row['股本报酬率(%)'],
              columns[10]: row['净资产报酬率(%)'],
              columns[11]: row['资产报酬率(%)'],
              columns[12]: row['销售毛利率(%)'],
              columns[13]: row['三项费用比重(%)'],
              columns[14]: row['非主营比重(%)'],
              columns[15]: row['主营利润比重(%)'],
              columns[16]: row['流动比率(%)'],
              columns[17]: row['速动比率(%)'],
              columns[18]: row['现金比率(%)'],
              columns[19]: row['利息支付倍数(%)'],
              columns[20]: row['资产负债率(%)'],
              columns[21]: row['长期债务与营运资金比率(%)'],
              columns[22]: row['股东权益比率(%)'],
              columns[23]: row['长期负债比率(%)'],
              columns[24]: row['股东权益与固定资产比率(%)'],
              columns[25]: row['负债与所有者权益比率(%)'],
              columns[26]: row['长期资产与长期资金比率(%)'],
              columns[27]: row['资本化比率(%)'],
              columns[28]: row['固定资产净值率(%)'],
              columns[29]: row['资本固定化比率(%)'],
              columns[30]: row['产权比率(%)'],
              columns[31]: row['清算价值比率(%)'],
              columns[32]: row['固定资产比重(%)'],
              columns[33]: row['主营业务收入增长率(%)'],
              columns[34]: row['净利润增长率(%)'],
              columns[35]: row['净资产增长率(%)'],
              columns[36]: row['总资产增长率(%)'],
              columns[37]: row['应收账款周转率(次)'],
              columns[38]: row['应收账款周转天数(天)'],
              columns[39]: row['存货周转率(次)'],
              columns[40]: row['固定资产周转率(次)'],
              columns[41]: row['总资产周转率(次)'],
              columns[42]: row['存货周转天数(天)'],
              columns[43]: row['总资产周转天数(天)'],
              columns[44]: row['流动资产周转率(次)'],
              columns[45]: row['流动资产周转天数(天)'],
              columns[46]: row['经营现金净流量对销售收入比率(%)'],
              columns[47]: row['资产的经营现金流量回报率(%)'],
              columns[48]: row['经营现金净流量与净利润的比率(%)'],
              columns[49]: row['经营现金净流量对负债比率(%)'],
              columns[50]: row['现金流量比率(%)']
              } for idx, row in indicator_data.iterrows()]
        )
        session.commit()
        session.close()

    def get_stock_financial_report(self, stock_code_m=None):
        if stock_code_m is None:
            stock_code_m = 'sz300227'
        stock_code = stock_code_m[2:]

        # Defining Web URL
        url_zcfzb = 'http://quotes.money.163.com/service/zcfzb_%s.html' % stock_code
        url_lrb = 'http://quotes.money.163.com/service/lrb_%s.html' % stock_code
        url_xjllb = 'http://quotes.money.163.com/service/xjllb_%s.html' % stock_code
        url = [url_zcfzb, url_lrb, url_xjllb]
        columns = ['报告日期'.decode('utf-8'), ]
        report_data = pd.DataFrame()

        # Getting web data
        for each_url in url:
            html = urllib.urlopen(each_url)
            data = html.read()
            # Remove --
            no_value = re.compile('--')
            data = no_value.sub('0', data)
            # Remove \n
            ul1 = re.compile('\n')
            data = ul1.sub('', data)
            # Remove Space
            space = re.compile(' ')
            data = space.sub('', data)
            '''
            # Remove 、
            dunhao = re.compile('、')
            data = dunhao.sub('', data)
            # Remove :
            maohao = re.compile('：')
            data = maohao.sub('', data)
            '''
            # Process Web data into a DF
            rows = []
            endata = data.decode('GB2312').encode('utf-8').split('\r')
            # print endata
            # Prepare column name list and data row list
            for line in endata:
                line = line.split(',')
                if (line[0] != '\t\t') & (line[0] != ''):
                    rows.append(line)
                    if line[0] != '报告日期':
                        # create column name list
                        percent = re.compile('\(%\)')
                        col_name = percent.sub('', line[0])
                        if each_url == url_xjllb:
                            col_name = col_name.decode('utf-8')
                            if col_name in columns:
                                col_name = '现金表'.decode('utf-8') + col_name
                        else:
                            col_name = col_name.decode('utf-8')
                        columns.append(col_name)

            df = pd.DataFrame(rows).T
            df.columns = df.iloc[0]
            df = df.iloc[1:, :]
            df['报告日期'] = df['报告日期'].astype('datetime64[ns]')
            df.set_index('报告日期', inplace=True)
            for column in df.columns:
                if column != '报告日期':
                    df = df.dropna()
                    df[column] = pd.to_numeric(df[column], errors='raise')

            if report_data.shape[1] == 0:
                report_data = df
            else:
                report_data = pd.concat([report_data, df], axis=1, join_axes=[report_data.index])
        report_data = report_data.dropna()
        report_data['stock_code'] = stock_code_m
        # Prepare DB Sessions
        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        financail_reports = Table('tb_StockFinancialReports', meta, autoload=True)
        # As the SQLAlchemy reading the table columns as unicode, so have to decode the columns names of DF

        # Save Table
        records = []
        for idx, row in report_data.iterrows():
            rd = {
                'stock_code': stock_code_m,
                columns[0]: idx, }
            for i in range(0, 242):
                rd.update({columns[i + 1]: row[i]})
            records.append(dict(rd))

        session.execute(financail_reports.insert(), records)
        session.commit()
        session.close()

    def get_stock_base_data(self):
        sql_select_stock_codes = 'select stock_code from tb_StockCodeList'
        self._clean_table('tb_StockMainFinancialIndicators')
        self._clean_table('tb_StockOperateIndicators')
        self._clean_table('tb_StockFinancialReports')
        df_stock_codes = pd.read_sql(sql_select_stock_codes, con=self._engine)
        for idx, row in df_stock_codes.iterrows():
            try:
                print '------------------------------------------------'
                print "Start to get stock %s datas" % row[0]
                self.get_stock_main_financail_indicators(row[0])
                self.get_stock_operation_indicators(row[0])
                self.get_stock_financial_report(row[0])
                print "Finished to get stock %s datas" % row[0]
            except:
                print row[0]
                self._log_mesg = self._log_mesg + row[0] + '\n'
                self._write_log(self._log_mesg, logPath='BaseDataErrorLog.txt')

    def get_single_stock_base_data(self, stock_code):
        # print "Getting main financail indicators"
        # self.get_stock_main_financail_indicators(stock_code)
        # print "Getting operational indicators"
        # self.get_stock_operation_indicators(stock_code)
        print "Getting  financial report"
        self.get_stock_financial_report(stock_code)
        print "Finished to get stock %s datas" % stock_code

    def _clean_table(self, table_name):
        conn = self._engine.connect()
        conn.execute("truncate %s" % table_name)
        print "Table %s is cleaned" % table_name

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
    # pp.get_stock_main_financail_indicators()
    pp.get_single_stock_base_data('sh603727')
    #pp.get_stock_base_data()

if __name__ == '__main__':
    main()
