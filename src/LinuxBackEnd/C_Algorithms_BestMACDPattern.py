#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import datetime, time, progressbar
import pandas as pd
from sqlalchemy import create_engine, Table, Column, MetaData
import multiprocessing as mp


class C_Algorithems_BestPattern(object):
    def __init__(self):
        self._input_dir = '/home/marshao/DataMiningProjects/Input/'
        self._output_dir = '/home/marshao/DataMiningProjects/Output/'
        self._stock_name_file = self._output_dir + 'StockNames.csv'
        self._stock_codes = ['sz300226', 'sh600887']
        self._stock_market = ""
        self._window = '10'
        self._engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')
        self._metadata = MetaData(self._engine)
        self._log_mesg = ''
        self._op_log = 'operLog.txt'
        self._my_columns = []
        self._my_DF = pd.DataFrame(columns=self._my_columns)
        self._x_min = ['m5', 'm15', 'm30', 'm60']
        self._x_period = ['day', 'week']


class C_BestMACDPattern(C_Algorithems_BestPattern):
    '''
    Step1: decide what analysis period will be used (5min, 30min, daily)
    Step2: use MACD_Pattern_lists to calculate all trading signals of all patterns.
    Step3: use trading signals of all pattern to calculate ending profits of all pattern
    Step4: Analysis ending profits of all pattern to select the best pattern
    '''

    def _par(self, period):
        self._tb_MACDIndex = Table('tb_MACDIndex', self._metadata, autoload=True)
        self._tb_StockCodes = Table('tb_StockCodes', self._metadata, autoload=True)
        if period in self._x_min:
            self._tb_TradeSignal = Table('tb_StockIndex_MACD', self._metadata, autoload=True)
            self._tb_StockRecords = Table('tb_StockXMinRecords', self._metadata, autoload=True)
            self._tb_Trades = Table('tb_MACD_Trades', self._metadata, autoload=True)
        elif period in self._x_period:
            self._tb_TradeSignal = Table('tb_StockIndex_MACD_HalfHour', self._metadata, autoload=True)
            self._tb_StockRecords = Table('tb_StockXPeriodRecords', self._metadata, autoload=True)
            self._tb_Trades = Table('tb_MACD_Trades_HalfHour', self._metadata, autoload=True)

    def _MACD_trading_signals(self, period='m30', stock_code='sz300226'):
        # use MACD_Pattern_lists to calculate all trading signals of all patterns
        # Calculate signals for 30min data
        sql_fetch_halfHour_records = ("select * from tb_StockXMinRecords where period = %s and stock_code = %s")

        df_stock_records = pd.read_sql(sql_fetch_halfHour_records, con=self._engine, params=(period, stock_code),
                                       index_col='quote_time')
        df_MACD_index = pd.read_sql('tb_MACDIndex', con=self._engine, index_col='id_tb_MACDIndex')
        self._multi_tasks_cal_MACD_signals(df_MACD_index, df_stock_records)
        # print df_MACD_index, df_stock_records, df_stock_records.index[0].date()

    def _multi_tasks_cal_MACD_signals(self, df_MACD_index, df_stock_records):
        tasks = df_MACD_index.index.size / 7
        task_args = []
        processor = 1
        index_begin = 0
        index_end = tasks
        while processor <= 8:
            df = df_MACD_index.index[index_begin:index_end]
            task_args.append((df_stock_records, df,))
            processor += 1
            index_begin = index_end
            if processor != 8:
                index_end = processor * tasks
            else:
                index_end = df_MACD_index.index.size
            print df
        '''
        pool = mp.Pool(7)
        for t in task_args:
            pool.apply_async(self._MACD_signal_calculation, t)
        pool.close()
        pool.join()
        '''

    def _MACD_signal_calculation(self, df_stock_records, df_MACD_index):
        widgets = ['MACD_Pattern_BackTest: ',
                   progressbar.Percentage(), ' ',
                   progressbar.Bar(marker='0', left='[', right=']'), ' ',
                   progressbar.ETA()]
        progress = progressbar.ProgressBar(widgets=widgets)

        # loop breaker
        loop_breaker = 0
        df = df_stock_records
        #  The first loop, go through every MACD Pattern in df_MACD_index
        for j in progress(range(df_MACD_index.index.size)):
            if loop_breaker > 1:
                break
            loop_breaker += 1

            short_window = df_MACD_index.EMA_short_window[df_MACD_index.index[j]]
            long_window = df_MACD_index.EMA_long_window[df_MACD_index.index[j]]
            dif_window = df_MACD_index.DIF_window[df_MACD_index.index[j]]
            MACD_Pattern_Number = df_MACD_index.index[j]

            num_records = len(df.index)
            df['EMA_short'] = pd.ewma(df.close_price, span=short_window)
            df['EMA_long'] = pd.ewma(df.close_price, span=long_window)
            df['DIF'] = df.EMA_short - df.EMA_long
            df['DEA'] = pd.rolling_mean(df.DIF, window=dif_window)
            df['MACD'] = 2.0 * (df.DIF - df.DEA)
            df['Signal'] = 0
            df['EMA_short_window'] = short_window
            df['EMA_long_window'] = long_window
            df['DIF_window'] = dif_window
            df['MACD_Pattern_Number'] = MACD_Pattern_Number
            index_date = df.index[0].date()
            tradable = True

            i = 0
            while i < num_records:
                # for index in df.index:
                '''
                #Since T+1 policy, There trade should be disabled if there is a buy happend in each trading day.
                #If the index_date is not equal to record date, means diffrent day, set the tradable = True
                '''
                if i > 0:
                    if index_date != df.index[i].date():
                        tradable = True
                        index_date = df.index[i].date()

                    if ((df.DIF[df.index[i]] > df.DEA[df.index[i]]) & (
                                df.DIF[df.index[i - 1]] <= df.DEA[df.index[i - 1]])):
                        if tradable:
                            df.Signal[df.index[i]] = 1
                            tradable = False
                        else:
                            pass
                    elif ((df.DIF[df.index[i]] < df.DEA[df.index[i]]) &
                              (df.DIF[df.index[i - 1]] >= df.DEA[df.index[i - 1]])):
                        if tradable:
                            df.Signal[df.index[i]] = -1
                        else:
                            pass
                    else:
                        pass
                        # df.drop(df.index[i], axis = 0, inplace = True)
                else:
                    pass
                i += 1
            # Remove the no transaction record from the DB.
            df_save = df[df.Signal != 0]
            #df_save.to_sql('tb_StockIndex_MACD_HalfHour', con=self._engine, flavor='mysql', if_exists='append', index=True)
            print 'df == ',df
            # print 'df_save', df_save.index.size


def _MACD_ending_profits(self):
    pass


def _MACD_best_pattern(self):
    pass


def _progress_monitor(self):
    '''
    Setting up progress bar to monitor the progress of whole program.
    '''
    widgets = ['MACD_Pattern_BackTest: ',
               progressbar.Percentage(), ' ',
               progressbar.Bar(marker='0', left='[', right=']'), ' ',
               progressbar.ETA()]
    progress = progressbar.ProgressBar(widgets=widgets)


class C_BestSARPattern(C_Algorithems_BestPattern):
    pass


def main():
    MACDPattern = C_BestMACDPattern()
    MACDPattern._MACD_trading_signals()


if __name__ == '__main__':
    main()
