#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import sys

sys.path.append('/home/marshao/DataMiningProjects/Project_StockAutoTrade_LinuxBackEnd/StockAutoTrades/src')

import time, os
import pandas as pd
import multiprocessing as mp
from sqlalchemy import Table, MetaData, exists, update, and_, select
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

    def update_direct_factors(self, df_stock_codes):
        DBSession = sessionmaker(bind=self._engine)
        session = DBSession()
        meta = MetaData(self._engine)

        # Delare Table
        SZFactors = Table('tb_StockSZFactors', meta, autoload=True)
        SHFactors = Table('tb_StockSHFactors', meta, autoload=True)
        SZHisDaily = Table('tb_StockSZHisDaily', meta, autoload=True)
        SHHisDaily = Table('tb_StockSHHisDaily', meta, autoload=True)

        for idx, row in df_stock_codes.iterrows():
            stock_code = row['stock_code']
            sz_ret = session.query(exists().where(SZHisDaily.columns['stock_code'] == stock_code)).scalar()
            if sz_ret:
                stat = self.update_ep_ttm(stock_code, 'sz', SZFactors)
                session.execute(stat)
                print 'updated stock %s' % stock_code
            elif session.query(exists().where(SHHisDaily.columns['stock_code'] == stock_code)).scalar():
                stat = self.update_ep_ttm(stock_code, 'sh', SHFactors)
                session.execute(stat)
                print 'updated stock %s' % stock_code
            session.commit()
        session.close()

    def update_ep_ttm(self, stock_code, market, des_table):

        meta = MetaData(self._engine)
        sql_read_src_sz = ('select quote_time, PE_TTM from tb_StockSZHisDaily where stock_code = %s')
        sql_read_src_sh = ('select quote_time, PE_TTM from tb_StockSHHisDaily where stock_code = %s')
        sql_read_des_sz = ('select stock_code, quote_time from tb_StockSZFactors where stock_code = %s')
        sql_read_des_sh = ('select stock_code, quote_time  from tb_StockSHFactors where stock_code = %s')

        if market == 'sz':
            df_src = pd.read_sql(con=self._engine, sql=sql_read_src_sz, params=[stock_code])
            df_des = pd.read_sql(con=self._engine, sql=sql_read_des_sz, params=[stock_code])
            d_tb = 'tb_StockSZFactors'
        else:
            df_src = pd.read_sql(con=self._engine, sql=sql_read_src_sh, params=[stock_code])
            df_des = pd.read_sql(con=self._engine, sql=sql_read_des_sh, params=[stock_code])
            d_tb = 'tb_StockSHFactors'

        df_src['EP_TTM'] = df_src['PE_TTM'].rdiv(1)
        df_src.drop(['PE_TTM'], axis=1)
        df_result = pd.merge(df_des, df_src, how='inner', on=['quote_time'])
        df_result.to_sql('tmp', con=self._engine, if_exists='replace')
        # df_src['EP_TTM'] = np.where(df_src['PE_TTM']==0, 1/df_src['PE_TTM'], 0)
        # df_src.loc[df_src['EP_TTM'] != 0, 'EP_TTM'] = 1/df_src['PE_TTM']

        tmp = Table('tmp', meta, autoload=True)

        stat = des_table.update(). \
            values(EP_TTM=select([tmp.c.EP_TTM]).where(tmp.c.quote_time == des_table.c.quote_time)). \
            where(des_table.c.stock_code == stock_code)

        stock_code = "'sh600000'"
        stat = 'UPDATE %s d ' % d_tb + \
               'SET d.EP_TTM = (SELECT t.EP_TTM from tmp t where t.quote_time = d.quote_time) ' + \
               'WHERE d.stock_code = %s;' % stock_code


        return stat

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

    def multi_processors_update_direct_factors(self):
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
                p = mp.Process(target=self.update_direct_factors,
                               args=(df_stock_codes[index_beg:index_end],))
                processes.append(p)

                index_beg = index_end
                index_end = index_end + infor_length / num_processor

        self.processes_pool(tasks=processes, processors=num_processor)


def main():
    upd = C_Update_Full_History_Daily_Data()
    # upd.multi_processors_update_industries()
    upd.multi_processors_update_direct_factors()

if __name__ == '__main__':
    main()
