#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import sys

sys.path.append('/home/marshao/DataMiningProjects/Project_StockAutoTrade_LinuxBackEnd/StockAutoTrades/src')

import time, os
import pandas as pd
import numpy as np
import multiprocessing as mp
from sqlalchemy import Table, MetaData, exists, update, and_, select, bindparam
from sqlalchemy.orm import sessionmaker
import C_GlobalVariable as glb
import csv


class C_Update_Full_History_Daily_Data(object):
    '''
    2018-01-26: This function is designed to update and calculate factors values.
    '''

    def __init__(self):
        gv = glb.C_GlobalVariable()
        self._emailobj = gv.get_emailobj()
        self._master_config = gv.get_master_config()
        self._calcu_config = gv.get_calcu_config()

        self._input_path = self._master_config['ubuntu_file_input_dir']
        self._engine = self._master_config['db_engine']
        self._processors = self._calcu_config['ubuntu_processors']

    def processes_pool(self, tasks, processors):
        # This is a self made Multiprocess pool
        task_total = len(tasks)
        loop_total = task_total / processors
        print "task total is %s, processors is %s, loop_total is %s" % (task_total, processors, loop_total)
        alive = True
        task_finished = 0
        # task_alive = 0
        # task_remain = task_total - task_finished
        # count = 0
        i = 0
        while i < loop_total:
            # print "This is the %s round" % i
            for j in range(processors):
                k = j + processors * i
                print "executing task %s" % k
                if k == task_total:
                    break
                tasks[k].start()
                j += 1

            for j in range(processors):
                k = j + processors * i
                if k == task_total:
                    break
                tasks[k].join()
                j += 1

            while alive == True:
                n = 0
                alive = False
                for j in range(processors):
                    k = j + processors * i
                    if k == task_total:
                        # print "This is the %s round of loop"%i
                        break
                    if tasks[k].is_alive():
                        alive = True
                    time.sleep(1)
            i += 1

    def write_errors(self, error_list):
        with open("errorlist.csv", "ab") as f:
            writer = csv.writer(f)
            writer.writerows(error_list)

    def load_industry_classes(self):
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
        return df_new_infor

    def update_industry_classes(self, df_new_infor):

        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        SZHisDaily = Table('tb_StockSZHisDaily', meta, autoload=True)
        SHHisDaily = Table('tb_StockSHHisDaily', meta, autoload=True)

        for idx, row in df_new_infor.iterrows():
            stock_code_num = row['stock_code']
            stock_name = row['stock_name']
            L1_industry_code = row['L1_industry_code']
            L1_industry_name = row['L1_industry_name']
            L2_industry_code = row['L2_industry_code']
            L2_industry_name = row['L2_industry_name']
            L3_industry_code = row['L3_industry_code']
            L3_industry_name = row['L3_industry_name']

            sh_stock_code = 'sh' + stock_code_num
            sz_stock_code = 'sz' + stock_code_num
            sz_ret = session.query(exists().where(SZHisDaily.columns['stock_code'] == sz_stock_code)).scalar()
            if sz_ret:
                stat = SZHisDaily.update(). \
                    where(SZHisDaily.columns['stock_code'] == sz_stock_code). \
                    values(L1_industry_code=L1_industry_code,
                           stock_name=stock_name,
                           L1_industry_name=L1_industry_name,
                           L2_industry_code=L2_industry_code,
                           L2_industry_name=L2_industry_name,
                           L3_industry_code=L3_industry_code,
                           L3_industry_name=L3_industry_name
                           )
                session.execute(stat)
                print 'updated stock %s' % sz_stock_code
            elif session.query(exists().where(SHHisDaily.columns['stock_code'] == sh_stock_code)).scalar():
                stat = SHHisDaily.update(). \
                    where(SHHisDaily.columns['stock_code'] == sh_stock_code). \
                    values(L1_industry_code=L1_industry_code,
                           stock_name=stock_name,
                           L1_industry_name=L1_industry_name,
                           L2_industry_code=L2_industry_code,
                           L2_industry_name=L2_industry_name,
                           L3_industry_code=L3_industry_code,
                           L3_industry_name=L3_industry_name
                           )
                session.execute(stat)
                print 'updated stock %s' % sh_stock_code
            session.commit()
        session.close()

    def load_stockcodes(self):
        sql_select_codes = 'select stock_code from tb_StockCodeList'
        df_stock_codes = pd.read_sql(con=self._engine, sql=sql_select_codes)
        return df_stock_codes

    def update_direct_factors(self, df_stock_codes, factor):
        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        SZFactors = Table('tb_StockSZFactors', meta, autoload=True)
        SHFactors = Table('tb_StockSHFactors', meta, autoload=True)
        SZHisDaily = Table('tb_StockSZHisDaily', meta, autoload=True)
        SHHisDaily = Table('tb_StockSHHisDaily', meta, autoload=True)
        sql_read_src_sz = ('select * from tb_StockSZHisDaily where stock_code = %s')
        sql_read_src_sh = ('select * from tb_StockSHHisDaily where stock_code = %s')
        sql_read_des_sz = ('select * from tb_StockSZFactors where stock_code = %s')
        sql_read_des_sh = ('select * from tb_StockSHFactors where stock_code = %s')


        error = []
        error_list = []
        count = 0

        for idx, row in df_stock_codes.iterrows():
            session = DBSession()
            stock_code = row['stock_code']
            try:
                sz_ret = session.query(exists().where(SZHisDaily.columns['stock_code'] == stock_code)).scalar()
                sh_ret = session.query(exists().where(SHHisDaily.columns['stock_code'] == stock_code)).scalar()
            except:
                sz_ret = False
                sh_ret = False
            if sz_ret:
                df_src = pd.read_sql(con=self._engine, sql=sql_read_src_sz, params=[stock_code])
                df_des = pd.read_sql(con=self._engine, sql=sql_read_des_sz, params=[stock_code])
                if factor == 'sp':
                    error = self.update_sp_ttm(stock_code, SZFactors, session, df_src, df_des)
                elif factor == 'ep':
                    error = self.update_ep_ttm(stock_code, 'sz', SZFactors, session, df_src, df_des)
                print 'updated stock %s, updated %s' % (stock_code, count)
            elif sh_ret:
                df_src = pd.read_sql(con=self._engine, sql=sql_read_src_sh, params=[stock_code])
                df_des = pd.read_sql(con=self._engine, sql=sql_read_des_sh, params=[stock_code])
                if factor == 'sp':
                    error = self.update_sp_ttm(stock_code, SZFactors, session, df_src, df_des)
                elif factor == 'ep':
                    error = self.update_ep_ttm(stock_code, 'sh', SZFactors, session, df_src, df_des)
                print 'updated stock %s, updated %s' % (stock_code, count)
            error_list.append(error)
            count += 1
            if count == 10:
                print "Releasing connections"
                session.commit()
                session.close()
                count = 0
        session.commit()
        session.close()
        self.write_errors(error_list)

    def source_loading(self, factor, stock_code=None):
        done = True
        # Using factors to load different source columns
        sql_read_src_sz = []
        sql_read_src_sh = []
        if stock_code is None:
            if factor == 'sp':
                sql_read_src_sz = ('select stock_code, quote_time, PS_TTM from tb_StockSZHisDaily')
                sql_read_src_sh = ('select stock_code, quote_time, PS_TTM from tb_StockSHHisDaily')
            elif factor == 'ep':
                sql_read_src_sz = ('select stock_code, quote_time, PE_TTM from tb_StockSZHisDaily')
                sql_read_src_sh = ('select stock_code, quote_time, PE_TTM from tb_StockSHHisDaily')
            elif factor == 'bp':
                sql_read_src_sz = ('select stock_code, quote_time, PB from tb_StockSZHisDaily')
                sql_read_src_sh = ('select stock_code, quote_time, PB from tb_StockSHHisDaily')
            elif factor == 'cfp':
                sql_read_src_sz = ('select stock_code, quote_time, PCF_TTM from tb_StockSZHisDaily')
                sql_read_src_sh = ('select stock_code, quote_time, PCF_TTM from tb_StockSHHisDaily')
            elif factor == 'mc' or factor == 'ln_mc':
                sql_read_src_sz = ('select stock_code, quote_time, total_equity from tb_StockSZHisDaily')
                sql_read_src_sh = ('select stock_code, quote_time, total_equity from tb_StockSHHisDaily')
            elif factor == 'fc' or factor == 'ln_fc':
                sql_read_src_sz = ('select stock_code, quote_time, market_equity from tb_StockSZHisDaily')
                sql_read_src_sh = ('select stock_code, quote_time, market_equity from tb_StockSHHisDaily')
            elif factor == 'fc_mc':
                sql_read_src_sz = ('select stock_code, quote_time, market_equity, total_equity from tb_StockSZHisDaily')
                sql_read_src_sh = ('select stock_code, quote_time, market_equity, total_equity from tb_StockSHHisDaily')
            elif factor == 'tc' or factor == 'ln_tc':
                sql_read_src_sz = (
                    'select stock_code, quote_time, total_asset from tb_StockMainFinancialIndicators where stock_code like "sz%%"')
                sql_read_src_sh = (
                    'select stock_code, quote_time, total_asset from tb_StockMainFinancialIndicators where stock_code like "sh%%"')
            elif factor == 'opg_ttm':
                sql_read_src_sz = ('select stock_code, quote_time, op_revenue from tb_StockFinancialReports')
                sql_read_src_sh = ('select stock_code, quote_time, op_revenue from tb_StockFinancialReports')
            else:
                print "No such factors"
                done = False
        else:
            if factor == 'sp':
                sql_read_src_sz = (
                'select stock_code, quote_time, PS_TTM from tb_StockSZHisDaily where stock_code = %s')
                sql_read_src_sh = (
                'select stock_code, quote_time, PS_TTM from tb_StockSHHisDaily where stock_code = %s')
            elif factor == 'ep':
                sql_read_src_sz = (
                'select stock_code, quote_time, PE_TTM from tb_StockSZHisDaily where stock_code = %s')
                sql_read_src_sh = (
                'select stock_code, quote_time, PE_TTM from tb_StockSHHisDaily where stock_code = %s')
            elif factor == 'bp':
                sql_read_src_sz = ('select stock_code, quote_time, PB from tb_StockSZHisDaily where stock_code = %s')
                sql_read_src_sh = ('select stock_code, quote_time, PB from tb_StockSHHisDaily where stock_code = %s')
            elif factor == 'cfp':
                sql_read_src_sz = (
                'select stock_code, quote_time, PCF_TTM from tb_StockSZHisDaily where stock_code = %s')
                sql_read_src_sh = (
                'select stock_code, quote_time, PCF_TTM from tb_StockSHHisDaily where stock_code = %s')
            elif factor == 'mc' or factor == 'ln_mc':
                sql_read_src_sz = (
                'select stock_code, quote_time, total_equity from tb_StockSZHisDaily where stock_code = %s')
                sql_read_src_sh = (
                'select stock_code, quote_time, total_equity from tb_StockSHHisDaily where stock_code = %s')
            elif factor == 'fc' or factor == 'ln_fc':
                sql_read_src_sz = (
                'select stock_code, quote_time, market_equity from tb_StockSZHisDaily where stock_code = %s')
                sql_read_src_sh = (
                'select stock_code, quote_time, market_equity from tb_StockSHHisDaily where stock_code = %s')
            elif factor == 'fc_mc':
                sql_read_src_sz = (
                'select stock_code, quote_time, market_equity, total_equity from tb_StockSZHisDaily where stock_code = %s')
                sql_read_src_sh = (
                'select stock_code, quote_time, market_equity, total_equity from tb_StockSHHisDaily where stock_code = %s')
            elif factor == 'tc' or factor == 'ln_tc':
                sql_read_src_sz = (
                    'select stock_code, quote_time, total_asset from tb_StockMainFinancialIndicators where stock_code = %s')
                sql_read_src_sh = (
                    'select stock_code, quote_time, total_asset from tb_StockMainFinancialIndicators where stock_code = %s')
            elif factor == 'opg_ttm':
                sql_read_src_sz = (
                    'select stock_code, quote_time, op_revenue from tb_StockFinancialReports where stock_code = %s')
                sql_read_src_sh = (
                    'select stock_code, quote_time, op_revenue from tb_StockFinancialReports where stock_code = %s')
            else:
                print "No such factors"
                done = False

        return done, sql_read_src_sz, sql_read_src_sh

    def run_update(self, factor, stock_code, Market_Factors, df_src, df_des, parameters):
        error = []
        stat = []
        session = []
        if factor == 'sp':
            stat, parameters, error = self.update_sp_ttm(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'ep':
            error = self.update_ep_ttm(stock_code, 'sz', Market_Factors, session, df_src, df_des)
        elif factor == 'cfp':
            stat, parameters, error = self.update_cfp_ttm(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'bp':
            stat, parameters, error = self.update_bp_ttm(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'mc':
            stat, parameters, error = self.update_mc(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'fc':
            stat, parameters, error = self.update_fc(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'tc':
            stat, parameters, error = self.update_tc(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'ln_mc':
            stat, parameters, error = self.update_ln_mc(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'ln_fc':
            stat, parameters, error = self.update_ln_fc(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'ln_tc':
            stat, parameters, error = self.update_ln_tc(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'fc_mc':
            stat, parameters, error = self.update_fc_mc(stock_code, Market_Factors, df_src, df_des, parameters)
        elif factor == 'opg_ttm':
            stat, parameters, error = self.update_opg_ttm(stock_code, Market_Factors, df_src, df_des, parameters)
        else:
            print " Unkonwn factor in run_update"
        return stat, parameters, error

    def bulk_update_direct_factors(self, df_stock_codes, factor, market=None):

        if market is None: market = 'sh'
        if factor is None:
            print "please specify a factor to update"
            return

        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        SZFactors = Table('tb_StockSZFactors', meta, autoload=True)
        SHFactors = Table('tb_StockSHFactors', meta, autoload=True)

        # Using factors to load different source columns
        done, sql_read_src_sz, sql_read_src_sh = self.source_loading(factor)
        if not done: return

        # Load Destination tables
        sql_read_des_sz = ('select id_tb, stock_code, quote_time from tb_StockSZFactors')
        sql_read_des_sh = ('select id_tb, stock_code, quote_time from tb_StockSHFactors')

        if market == 'sz':
            print "Start to read source table"
            df_src_o = pd.read_sql(con=self._engine, sql=sql_read_src_sz)
            stock_codes = df_src_o['stock_code'].unique()
            print "Start to read des table"
            df_des_o = pd.read_sql(con=self._engine, sql=sql_read_des_sz)
        elif market == 'sh':
            print "Start to read source table"
            df_src_o = pd.read_sql(con=self._engine, sql=sql_read_src_sh)
            stock_codes = df_src_o['stock_code'].unique()
            print "Start to read des table"
            df_des_o = pd.read_sql(con=self._engine, sql=sql_read_des_sh)
        else:
            print 'No such market'
            return

        session.commit()
        session.close()

        error = []
        error_list = []
        count = 0
        total_count = 0
        parameters = []
        print "Start to update"

        # Gettign slices of stock_code after dedicated one
        '''
        idx = df_stock_codes[df_stock_codes.stock_code == 'sh603768'].index[0]
        df_stock_codes = df_stock_codes.iloc[idx: , : ]
        '''
        for idx, row in df_stock_codes.iterrows():
            # session = DBSession()
            stock_code = row['stock_code']
            t0 = time.clock()
            try:
                # sz_ret = session.query(exists().where(SZHisDaily.columns['stock_code'] == stock_code)).scalar()
                # sh_ret = session.query(exists().where(SHHisDaily.columns['stock_code'] == stock_code)).scalar()
                stock_ret = (stock_code in stock_codes)
            except:
                stock_ret = False

            if stock_ret and market == 'sz':
                count += 1
                total_count += 1
                df_src = df_src_o.loc[df_src_o['stock_code'] == stock_code, :]
                df_des = df_des_o.loc[df_des_o['stock_code'] == stock_code, :]
                if not df_des.empty and not df_src.empty:
                    stat, parameters, error = self.run_update(factor, stock_code, SZFactors, df_src, df_des, parameters)
                else:
                    print "Stock %s is empty in either src or des table"%stock_code
                print 'updated stock %s, task %s, updated %s, factor %s, time consumed %s' % (
                stock_code, count, total_count, factor, (time.clock() - t0))
            elif stock_ret and market == 'sh':
                count += 1
                total_count += 1
                df_src = df_src_o.loc[df_src_o['stock_code'] == stock_code, :]
                df_des = df_des_o.loc[df_des_o['stock_code'] == stock_code, :]
                if not df_des.empty and not df_src.empty:
                    stat, parameters, error = self.run_update(factor, stock_code, SHFactors, df_src, df_des, parameters)
                else:
                    print "Stock %s is empty in either src or des table"%stock_code
                print 'updated stock %s, task %s, updated %s, factor %s , time consumned %s' % (
                stock_code, count, total_count, factor, (time.clock() - t0))
            error_list.append(error)
            # count += 1

            if count == 100:
                print "Execute updates"
                self.multi_processors_sql_execution(stat, parameters)
                parameters = []
                count = 0

        # session.execute(stat, parameters)
        if len(parameters) != 0:
            self.multi_processors_sql_execution(stat, parameters)
        self.write_errors(error_list)
        return

    def single_stock_bulk_update_direct_factors(self, stock_codes, factor, market=None):

        if factor is None:
            print "please specify a factor to update"
            return

        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        SZFactors = Table('tb_StockSZFactors', meta, autoload=True)
        SHFactors = Table('tb_StockSHFactors', meta, autoload=True)

        error = []
        error_list = []
        count = 0
        stat = []
        total_count = 0
        parameters = []

        for idx, row in stock_codes.iterrows():
            stock_code = row['stock_code']

            if (market is not None) and (stock_code[0:2] == market):
                # Using factors to load different source columns
                done, sql_read_src_sz, sql_read_src_sh = self.source_loading(factor, stock_code)
                if not done:
                    return
            if (market is not None) and (stock_code[0:2] != market):
                continue
            elif market is None:
                market = stock_code[0:2]


            t0 = time.clock()

            # Load Destination tables
            sql_read_des_sz = ('select id_tb, stock_code, quote_time from tb_StockSZFactors where stock_code = %s')
            sql_read_des_sh = ('select id_tb, stock_code, quote_time from tb_StockSHFactors where stock_code = %s')

            if market == 'sz':
                #print "Start to read SZ source table"
                df_src = pd.read_sql(con=self._engine, sql=sql_read_src_sz, params=[stock_code])
                # stock_codes = df_src_o['stock_code'].unique()
                #print "Start to read SZ des table"
                df_des = pd.read_sql(con=self._engine, sql=sql_read_des_sz, params=[stock_code])
            elif market == 'sh':
                #print "Start to read SH source table"
                df_src = pd.read_sql(con=self._engine, sql=sql_read_src_sh, params=[stock_code])
                # stock_codes = df_src_o['stock_code'].unique()
                #print "Start to read SH des table"
                df_des = pd.read_sql(con=self._engine, sql=sql_read_des_sh, params=[stock_code])
            else:
                print 'No such market'
                return

            session.commit()
            session.close()

            stock_ret = True
            if df_des.empty or df_src.empty:
                print "Stock %s is empty either in source or destination tables."%stock_code
                stock_ret = False

            if stock_ret and market == 'sz':
                count += 1
                total_count += 1
                stat, parameters, error = self.run_update(factor, stock_code, SZFactors, df_src, df_des, parameters)
                print 'updated stock %s, task %s, updated %s, factor %s, time consumed %s' % (
                stock_code, count, total_count, factor, (time.clock() - t0))
            elif stock_ret and market == 'sh':
                count += 1
                total_count += 1
                stat, parameters, error = self.run_update(factor, stock_code, SHFactors, df_src, df_des, parameters)
                print 'updated stock %s, task %s, updated %s, factor %s , time consumned %s' % (
                stock_code, count, total_count, factor, (time.clock() - t0))

            if count == 200:
                print "Execute updates"
                self.multi_processors_sql_execution(stat, parameters)
                parameters = []
                count = 0
            error_list.append(error)

        if len(parameters) != 0:
            self.multi_processors_sql_execution(stat, parameters)

        self.write_errors(error_list)
        return

    def single_stock_update_direct_factors(self, stock_code, factor, market=None):

        if market is None: market = 'sh'
        if factor is None:
            print "please specify a factor to update"
            return

        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        SZFactors = Table('tb_StockSZFactors', meta, autoload=True)
        SHFactors = Table('tb_StockSHFactors', meta, autoload=True)

        # Using factors to load different source columns
        done, sql_read_src_sz, sql_read_src_sh = self.source_loading(factor, stock_code)
        if not done: return

        # Load Destination tables
        sql_read_des_sz = ('select id_tb, stock_code, quote_time from tb_StockSZFactors where stock_code = %s')
        sql_read_des_sh = ('select id_tb, stock_code, quote_time from tb_StockSHFactors where stock_code = %s')

        if market == 'sz':
            print "Start to read source table"
            df_src = pd.read_sql(con=self._engine, sql=sql_read_src_sz, params=[stock_code])
            # stock_codes = df_src_o['stock_code'].unique()
            print "Start to read des table"
            df_des = pd.read_sql(con=self._engine, sql=sql_read_des_sz, params=[stock_code])
        elif market == 'sh':
            print "Start to read source table"
            df_src = pd.read_sql(con=self._engine, sql=sql_read_src_sh, params=[stock_code])
            # stock_codes = df_src_o['stock_code'].unique()
            print "Start to read des table"
            df_des = pd.read_sql(con=self._engine, sql=sql_read_des_sh, params=[stock_code])
        else:
            print 'No such market'
            return

        session.commit()
        session.close()

        stock_ret = True
        error = []
        error_list = []
        count = 0
        total_count = 0
        parameters = []
        t0 = time.clock()
        print "Start to update"

        if df_des.empty or df_src.empty:
            print "Stock %s is empty either in source or destination tables." % stock_code
            stock_ret = False

        if stock_ret and market == 'sz':
            count += 1
            total_count += 1
            stat, parameters, error = self.run_update(factor, stock_code, SZFactors, df_src, df_des, parameters)
            print 'updated stock %s, task %s, updated %s, factor %s, time consumed %s' % (
            stock_code, count, total_count, factor, (time.clock() - t0))
        elif stock_ret and market == 'sh':
            count += 1
            total_count += 1
            stat, parameters, error = self.run_update(factor, stock_code, SHFactors, df_src, df_des, parameters)
            print 'updated stock %s, task %s, updated %s, factor %s , time consumned %s' % (
            stock_code, count, total_count, factor, (time.clock() - t0))

        error_list.append(error)

        # session.execute(stat, parameters)
        if len(parameters) != 0:
            self.multi_processors_sql_execution(stat, parameters)

        self.write_errors(error_list)
        return

    def update_ep_ttm(self, stock_code, market, des_table, session, df_src_o, df_des):
        error_list = []
        stat = des_table.update(). \
            values(EP_TTM=bindparam('_EP_TTM')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        try:
            df_src = df_src_o.loc[:, ('quote_time', 'PE_TTM')]
            df_src['EP_TTM'] = np.round(df_src['PE_TTM'].rdiv(1), 5)
            df_src = df_src.loc[:, ('quote_time', 'EP_TTM')]
            df_des = df_des.loc[:, ('id_tb', 'stock_code', 'quote_time')]
            df_des = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
            df_des.fillna(0, inplace=True)
            # print df_des
            parameters = []
            for idx, row in df_des.iterrows():
                # parameters.append({'_SP_TTM': row.SP_TTM, '_stock_code': stock_code, '_quote_time': row.quote_time})
                parameters.append({'_EP_TTM': row.EP_TTM, '_id_tb': row.id_tb})
            session.execute(stat, parameters)
        except:
            print "Updating EP_TTM with %s is in trouble" % stock_code
            error_list.append((stock_code))
        return error_list

    def update_sp_ttm(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []
        #print des_table
        stat = des_table.update(). \
            values(SP_TTM=bindparam('_SP_TTM')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'PS_TTM')]
        df_src['SP_TTM'] = np.round(df_src['PS_TTM'].rdiv(1), 5)
        # df_src.drop(['PS_TTM'], axis=1, inplace=True)
        df_src = df_src.loc[:, ('quote_time', 'SP_TTM')]
        #df_des = df_des.loc[:, ('id_tb', 'stock_code', 'quote_time')]
        df_des = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
        df_des.fillna(0, inplace=True)

        for idx, row in df_des.iterrows():
            parameters.append({'_SP_TTM': row.SP_TTM, '_id_tb': row.id_tb})
        # print parameters
            # session.execute(stat, parameters)
        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_cfp_ttm(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []
        # print des_table
        stat = des_table.update(). \
            values(CFP_TTM=bindparam('_CFP_TTM')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'PCF_TTM')]
        df_src['CFP_TTM'] = np.round(df_src['PCF_TTM'].rdiv(1), 5)
        # df_src.drop(['PS_TTM'], axis=1, inplace=True)
        df_src = df_src.loc[:, ('quote_time', 'CFP_TTM')]
        # df_des = df_des.loc[:, ('id_tb', 'stock_code', 'quote_time')]
        df_des = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
        df_des.fillna(0, inplace=True)

        for idx, row in df_des.iterrows():
            parameters.append({'_CFP_TTM': row.CFP_TTM, '_id_tb': row.id_tb})
            # print parameters
            # session.execute(stat, parameters)
        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_bp_ttm(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []
        # print des_table
        stat = des_table.update(). \
            values(BP=bindparam('_BP')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'PB')]
        df_src['BP'] = np.round(df_src['PB'].rdiv(1), 5)
        # df_src.drop(['PS_TTM'], axis=1, inplace=True)
        df_src = df_src.loc[:, ('quote_time', 'BP')]
        # df_des = df_des.loc[:, ('id_tb', 'stock_code', 'quote_time')]
        df_des = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
        df_des.fillna(0, inplace=True)

        for idx, row in df_des.iterrows():
            parameters.append({'_BP': row.BP, '_id_tb': row.id_tb})
            # print parameters
            # session.execute(stat, parameters)
        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_mc(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []

        # print des_table
        stat = des_table.update(). \
            values(MC=bindparam('_MC')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'total_equity')]
        df_src['MC'] = np.round(df_src['total_equity'], 5)
        # df_src.drop(['PS_TTM'], axis=1, inplace=True)
        df_src = df_src.loc[:, ('quote_time', 'MC')]
        # df_des = df_des.loc[:, ('id_tb', 'stock_code', 'quote_time')]
        df_des = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
        df_des.fillna(0, inplace=True)

        for idx, row in df_des.iterrows():
            parameters.append({'_MC': row.MC, '_id_tb': row.id_tb})
        # print parameters
        # session.execute(stat, parameters)
        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_ln_mc(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []

        # print des_table
        stat = des_table.update(). \
            values(LN_MC=bindparam('_LN_MC')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'total_equity')]
        df_src = df_src_o.loc[df_src['total_equity'] > 0, :]
        df_src['LN_MC'] = np.round(np.log(df_src['total_equity']), 5)
        # df_src.drop(['PS_TTM'], axis=1, inplace=True)
        df_src = df_src.loc[:, ('quote_time', 'LN_MC')]
        # df_des = df_des.loc[:, ('id_tb', 'stock_code', 'quote_time')]
        df_des = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
        df_des.fillna(0, inplace=True)

        for idx, row in df_des.iterrows():
            parameters.append({'_LN_MC': row.LN_MC, '_id_tb': row.id_tb})
        # print parameters
        # session.execute(stat, parameters)
        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_fc(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []

        # print des_table
        stat = des_table.update(). \
            values(FC=bindparam('_FC')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'market_equity')]
        df_src['FC'] = np.round(df_src['market_equity'], 5)
        # df_src.drop(['PS_TTM'], axis=1, inplace=True)
        df_src = df_src.loc[:, ('quote_time', 'FC')]
        # df_des = df_des.loc[:, ('id_tb', 'stock_code', 'quote_time')]
        df_des = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
        df_des.fillna(0, inplace=True)

        for idx, row in df_des.iterrows():
            parameters.append({'_FC': row.FC, '_id_tb': row.id_tb})
        # print parameters
        # session.execute(stat, parameters)
        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_ln_fc(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []

        # print des_table
        stat = des_table.update(). \
            values(LN_FC=bindparam('_LN_FC')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'market_equity')]
        df_src = df_src_o.loc[df_src['market_equity'] > 0, :]
        df_src['LN_FC'] = np.round(np.log(df_src['market_equity']), 5)
        # df_src.drop(['PS_TTM'], axis=1, inplace=True)
        df_src = df_src.loc[:, ('quote_time', 'LN_FC')]
        # df_des = df_des.loc[:, ('id_tb', 'stock_code', 'quote_time')]
        df_des = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
        df_des.fillna(0, inplace=True)

        for idx, row in df_des.iterrows():
            parameters.append({'_LN_FC': row.LN_FC, '_id_tb': row.id_tb})
        # print parameters
        # session.execute(stat, parameters)
        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_tc(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []

        # print des_table
        stat = des_table.update(). \
            values(TC=bindparam('_TC')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'total_asset')]
        df_src['TC'] = np.round(df_src['total_asset'], 5)
        df_src = df_src.loc[:, ('quote_time', 'TC')]
        df_des = self.fill_daily_for_sessonal_factors(df_src, df_des, 'TC')

        for idx, row in df_des.iterrows():
            parameters.append({'_TC': row.TC, '_id_tb': row.id_tb})

        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_ln_tc(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []

        #Statment to run
        stat = des_table.update(). \
            values(LN_TC=bindparam('_LN_TC')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))

        #Factor Value Calculation
        df_src = df_src_o.loc[:, ('quote_time', 'total_asset')]
        df_src = df_src_o.loc[df_src['total_asset'] > 0, :]
        df_src['LN_TC'] = np.round(np.log(df_src['total_asset']), 5)
        df_src = df_src.loc[:, ('quote_time', 'LN_TC')]
        # Fill out missing data
        df_des = self.fill_daily_for_sessonal_factors(df_src, df_des, 'LN_TC')

        #Build parameters
        for idx, row in df_des.iterrows():
            parameters.append({'_LN_TC': row.LN_TC, '_id_tb': row.id_tb})

        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_fc_mc(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []

        # print des_table
        stat = des_table.update(). \
            values(FC_MC=bindparam('_FC_MC')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'market_equity', 'total_equity')]
        df_src = df_src_o.loc[df_src['total_equity'] > 0, :]
        df_src['FC_MC'] = np.round(df_src['market_equity'] / df_src['total_equity'], 5)
        # df_src.drop(['PS_TTM'], axis=1, inplace=True)
        df_src = df_src.loc[:, ('quote_time', 'FC_MC')]
        # df_des = df_des.loc[:, ('id_tb', 'stock_code', 'quote_time')]
        df_des = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
        df_des.fillna(0, inplace=True)

        for idx, row in df_des.iterrows():
            parameters.append({'_FC_MC': row.FC_MC, '_id_tb': row.id_tb})
        # print parameters
        # session.execute(stat, parameters)
        error_list.append((stock_code))
        return stat, parameters, error_list

    def update_opg_ttm(self, stock_code, des_table, df_src_o, df_des, parameters):
        error_list = []
        stat = des_table.update(). \
            values(OPG_TTM=bindparam('_OPG_TTM')). \
            where(and_(des_table.c.id_tb == bindparam('_id_tb')))
        df_src = df_src_o.loc[:, ('quote_time', 'op_revenue')]
        df_src = self.cal_ttm_growth(df_src, src_col='op_revenue', des_col='OPG_TTM')
        df_des = self.fill_daily_for_sessonal_factors(df_src, df_des, 'OPG_TTM')
        df_des.reset_index(inplace=True)
        df_des = df_des.loc[:, ('id_tb', 'quote_time', 'OPG_TTM')]
        for idx, row in df_des.iterrows():
            parameters.append({'_OPG_TTM': row.OPG_TTM, '_id_tb': row.id_tb})

        error_list.append((stock_code))
        return stat, parameters, error_list

    def cal_ttm_growth(self, df_src, src_col, des_col):
        '''
        From src_col to calculated ttm growth to des col
        :param df_src:
        :param src_col:
        :param des_col:
        :return:
        '''
        df_src.sort_values(by='quote_time', ascending=1, inplace=True)
        # Calculating TTM with rolling
        df_src['ttm'] = df_src[src_col].rolling(4).sum()
        #Calculating log growth
        df_src[des_col] = np.round(np.log(df_src.ttm).diff(), 5)
        df_src.sort_values(by='quote_time', ascending=0, inplace=True)
        # replace infinite value
        df_src.loc[(df_src[des_col] == np.inf) | (df_src[des_col] == -np.inf), des_col] = -99.0
        df_src.fillna(-99.0, inplace=True)

        return  df_src

    def fill_daily_for_sessonal_factors(self, df_src, df_des, factor):
        '''
        Fill out sessonal destination df missing daily values.
        :param df_src:
        :param df_des:
        :return: merged df_des
        '''
        # t0 = time.clock()
        #src_length = df_src.count()[0]
        df_des.set_index(['quote_time'], inplace=True)
        for idx, row in df_src.sort_values(by='quote_time', ascending=1).iterrows():
            df_des.loc[df_des.index >= row['quote_time'], factor] = row[factor]

        '''
        for i in range(0,src_length+1):
            if i < (src_length) and i> 0:
                df_des.loc[(df_des.index < df_src.loc[i-1, 'quote_time']) & (
                    df_des.index >= df_src.loc[i, 'quote_time']), factor] = df_src.loc[i, factor]
            elif i==(src_length):
                df_des.loc[(df_des.index < df_src.loc[src_length-1, 'quote_time']), factor] = df_src.loc[src_length-1, factor]
            else:
                df_des.loc[(df_des.index >= df_src.loc[i, 'quote_time']), factor] = df_src.loc[i, factor]
        '''
        df_des.fillna(0, inplace=True)
        # print "Time useage of fill missing is %s" % (time.clock() - t0)
        return df_des

    def multi_processors_update_industries(self):
        df_new_infor = self.load_industry_classes()
        infor_length = df_new_infor.count()[0]
        print infor_length

        # wait = input("Please")
        # print wait

        num_processor = self._processors
        index_beg = 0
        index_end = infor_length / num_processor

        processes = []
        for i in range(num_processor + 1):
            if i != num_processor:
                print "i:%s,  index_beg %s , index_end %s " % (i, index_beg, index_end)
                p = mp.Process(target=self.update_industry_classes,
                               args=(df_new_infor[index_beg:index_end],))
                processes.append(p)

                index_beg = index_end
                index_end = index_end + infor_length / num_processor

        self.processes_pool(tasks=processes, processors=num_processor)

    def multi_processors_update_direct_factors(self, factor=None, market=None):

        df_stock_codes = self.load_stockcodes()
        infor_length = df_stock_codes.count()[0]
        print infor_length

        # wait = input("Please")
        # print wait

        num_processor = self._processors
        index_beg = 0
        index_end = infor_length / num_processor

        processes = []
        for i in range(num_processor + 1):
            if i != num_processor:
                print "i:%s,  index_beg %s , index_end %s " % (i, index_beg, index_end)
                p = mp.Process(target=self.single_stock_bulk_update_direct_factors,
                               args=(df_stock_codes[index_beg:index_end], factor, market))
                processes.append(p)

                index_beg = index_end
                index_end = index_end + infor_length / num_processor

        self.processes_pool(tasks=processes, processors=num_processor)

    def multi_processors_sql_execution(self, stat, parameters):
        num_processor = 4
        infor_length = len(parameters)
        index_beg = 0
        task_length = infor_length / num_processor
        index_end = task_length
        processes = []

        for i in range(num_processor):
            print "i:%s,  index_beg %s , index_end %s " % (i, index_beg, index_end)
            p = mp.Process(target=self.sql_execution,
                               args=(stat, parameters[index_beg:index_end]))
            processes.append(p)

            index_beg = index_end
            if i == num_processor - 1:
                index_end = infor_length
            else:
                index_end = index_end + task_length

        self.processes_pool(tasks=processes, processors=num_processor)
        return

    def sql_execution(self, stat, parameters):
        # gv = glb.C_GlobalVariable()
        engine = glb.C_GlobalVariable().get_master_config()['db_engine']
        DBSession = sessionmaker(bind=engine)
        session = DBSession()
        session.execute(stat, parameters)
        session.commit()
        session.close()
        return

def main():
    upd = C_Update_Full_History_Daily_Data()
    # upd.multi_processors_update_industries()
    # upd.multi_processors_update_direct_factors('bp', 'sz')
    # upd.multi_processors_update_direct_factors('bp', 'sh')
    # upd.multi_processors_update_direct_factors('cfp', 'sz')
    # upd.multi_processors_update_direct_factors('cfp', 'sh')
    # upd.multi_processors_update_direct_factors('mc', 'sh')
    # upd.multi_processors_update_direct_factors('mc', 'sz')
    # upd.multi_processors_update_direct_factors('fc', 'sz')
    # upd.multi_processors_update_direct_factors('fc', 'sh')
    # upd.multi_processors_update_direct_factors('ln_mc', 'sh')
    # upd.multi_processors_update_direct_factors('ln_mc', 'sz')
    # upd.multi_processors_update_direct_factors('ln_fc', 'sz')
    # upd.multi_processors_update_direct_factors('ln_fc', 'sh')
    # upd.multi_processors_update_direct_factors('fc_mc', 'sz')
    # upd.multi_processors_update_direct_factors('fc_mc', 'sh')
    #upd.multi_processors_update_direct_factors('tc', 'sz')
    #upd.multi_processors_update_direct_factors('tc', 'sh')
    #upd.multi_processors_update_direct_factors('ln_tc', 'sz')
    #upd.multi_processors_update_direct_factors('ln_tc', 'sh')
    #upd.single_stock_update_direct_factors('sz000001', 'opg_ttm', 'sz')
    upd.multi_processors_update_direct_factors('opg_ttm', 'sz')
    upd.multi_processors_update_direct_factors('opg_ttm', 'sh')
    #upd.multi_processors_update_direct_factors('ep')
    #upd.update_single_stock_direct_factors('sh600835', 'sp')

if __name__ == '__main__':
    main()
