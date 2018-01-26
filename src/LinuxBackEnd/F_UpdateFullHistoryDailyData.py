#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import sys

sys.path.append('/home/marshao/DataMiningProjects/Project_StockAutoTrade_LinuxBackEnd/StockAutoTrades/src')

import time, urllib, re, os
import pandas as pd
import datetime
from sqlalchemy import Table, MetaData
from sqlalchemy import update
from sqlalchemy.orm import sessionmaker
import C_GlobalVariable as glb


class C_Update_Full_History_Daily_Data(object):
    '''
    2018-01-26: This function is designed to update and calculate factors values.
    '''

    def __init__(self):
        gv = glb.C_GlobalVariable()
        self._emailobj = gv.get_emailobj()
        self._master_config = gv.get_master_config()
        self._input_path = self._master_config['ubuntu_file_input_dir']
        self._engine = self._master_config['db_engine']

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

    def update_industry_classes(self):
        '''
        This is the function to update industry classed information in the Table 'tb_StockFullHistoryDailyRecords'.
        :param self:
        :return:
        '''
        input_path = '/home/marshao/UWShare/source'
        columns = ['stock_code', 'stock_name', 'L1_industry_code', 'L1_industry_name', 'L2_industry_code',
                   'L2_industry_name',
                   'L3_industry_code', 'L3_industry_name', 'L4_industry_code', 'L4_industry_name', 'stock_PE',
                   'stock_PE_TTM',
                   'stock_PB', 'stock_DP']

        # Read new information from CSV file
        for rt, dirs, files in os.walk(input_path):
            for file in files:
                # df = pd.read_csv(os.path.join(rt, file), sep=',', skiprows=1, names=columns)
                print os.path.join(rt, file)
                dt = '/home/marshao/UWShare/outrange'
                pr = '/home/marshao/UWShare/processed'
                df_new_infor = pd.read_csv(os.path.join(rt, file), dtype=str, sep=',')
                df_new_infor.columns = [columns]


                # Prepare DB Sessions
        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        full_history_daily = Table('tb_StockFullHistoryDailyRecords', meta, autoload=True)

        for idx, row in df_new_infor.iterrows():
            stock_code_num = row['stock_code']
            stock_name = row['stock_name']
            L1_industry_code = row['L1_industry_code']
            L1_industry_name = row['L1_industry_name']
            L2_industry_code = row['L2_industry_code']
            L2_industry_name = row['L2_industry_name']
            L3_industry_code = row['L3_industry_code']
            L3_industry_name = row['L3_industry_name']

            stock_code = 'sh' + stock_code_num
            print stock_code
            # Read data from DB

            # print stock
            # session.execute(full_history_daily.update(), records)
            # session.commit()
        session.close()

def main():
    upd = C_Update_Full_History_Daily_Data()
    upd.update_industry_classes()


if __name__ == '__main__':
    main()
