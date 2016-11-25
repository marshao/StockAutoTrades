#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import datetime, time, progressbar, math
import pandas as pd
from sqlalchemy import create_engine, Table, Column, MetaData
from sqlalchemy.sql import select, and_, or_, not_
from multiprocessing import Pool
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

    def __init__(self, period='m30'):
        C_Algorithems_BestPattern.__init__(self)
        self._tb_MACDIndex = Table('tb_MACDIndex', self._metadata, autoload=True)
        self._tb_StockCodes = Table('tb_StockCodes', self._metadata, autoload=True)
        self._tb_TradeSignal = Table('tb_StockIndex_MACD_New', self._metadata, autoload=True)
        if period in self._x_min:
            self._tb_StockRecords = Table('tb_StockXMinRecords', self._metadata, autoload=True)
            self._tb_Trades = Table('tb_MACD_Trades', self._metadata, autoload=True)
        elif period in self._x_period:
            self._tb_StockRecords = Table('tb_StockXPeriodRecords', self._metadata, autoload=True)
            self._tb_Trades = Table('tb_MACD_Trades_HalfHour', self._metadata, autoload=True)

    def _MACD_trading_signals(self, period='m30', stock_code='sz300226'):
        # use MACD_Pattern_lists to calculate all trading signals of all patterns
        # Calculate signals for 30min data
        sql_fetch_halfHour_records = ("select * from tb_StockXMinRecords where period = %s and stock_code = %s")

        df_stock_records = pd.read_sql(sql_fetch_halfHour_records, con=self._engine, params=(period, stock_code),
                                       index_col='quote_time')
        df_MACD_index = pd.read_sql('tb_MACDIndex', con=self._engine, index_col='id_tb_MACDIndex')
        #self._MACD_signal_calculation(df_MACD_index, df_stock_records)
        self._clean_table('tb_StockIndex_MACD_New')
        self._multi_processors_cal_MACD_signals(df_MACD_index, df_stock_records)
        # print df_MACD_index, df_stock_records, df_stock_records.index[0].date()

    def _clean_table(self, table_name):
        conn = self._engine.connect()
        conn.execute("truncate %s" %table_name)
        print "Table is cleaned"

    def _multi_processors_cal_MACD_signals(self, df_MACD_index, df_stock_records):
        print "Jumped into Multiprocessing "

        sql_fetch_halfHour_records = ("select * from tb_StockXMinRecords where period = 'm30' and stock_code = 'sz300226'")
        tasks = df_MACD_index.index.size / 7
        task_args = []
        processor = 1
        index_begin = 0
        index_end = tasks
        while processor <= 8:
            df = df_MACD_index[index_begin:index_end]
            df_stock_records = pd.read_sql(sql_fetch_halfHour_records, con=self._engine, index_col='quote_time')
            task_args.append((df, df_stock_records),)

            processor += 1
            index_begin = index_end
            if processor != 8:
                index_end = processor * tasks
            else:
                index_end = df_MACD_index.index.size

        '''
        pool = Pool(7)
        #for t in task_args:
        #    pool.map_async(self._MACD_signal_calculation(t[0], t[1]), ())
        pool.map_async(self._MACD_signal_calculation(task_args[0][0], task_args[0][1]), ())
        pool.map_async(self._MACD_signal_calculation(task_args[1][0], task_args[1][1]), ())
        pool.close()
        pool.join()

        '''
        p1 = mp.Process(target = self._MACD_signal_calculation, args = (task_args[0][0], task_args[0][1],))
        p2 = mp.Process(target=self._MACD_signal_calculation, args=(task_args[1][0], task_args[1][1],))
        p3 = mp.Process(target=self._MACD_signal_calculation, args=(task_args[2][0], task_args[2][1],))
        p4 = mp.Process(target=self._MACD_signal_calculation, args=(task_args[3][0], task_args[3][1],))
        p5 = mp.Process(target=self._MACD_signal_calculation, args=(task_args[4][0], task_args[4][1],))
        p6 = mp.Process(target=self._MACD_signal_calculation, args=(task_args[5][0], task_args[5][1],))
        p7 = mp.Process(target=self._MACD_signal_calculation, args=(task_args[6][0], task_args[6][1],))

        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()
        p6.start()
        p7.start()

        p1.join()
        p2.join()
        p3.join()
        p4.join()
        p5.join()
        p6.join()
        p7.join()


    def _MACD_signal_calculation(self, df_MACD_index, df_stock_records):
        print "Processing MACD Index"
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
            #if loop_breaker > 1:
            #    break
            #loop_breaker += 1

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
            engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')
            df_save = df[df.Signal != 0].drop(df.columns[[0,2,3,4,5,7]], axis=1)
            df_save.to_sql('tb_StockIndex_MACD_New', con=engine, flavor='mysql', if_exists='append', index=True)

    def _MACD_ending_profits(self, period='m30', stock_code='sz300226'):
        # Fetch out Signal, EMA_short_window, EMA_long_window, DIF_window, quote_time, MACD_pattern_number from tb_StockIndex_MACD_New into a Pandas DF
        # Fetch out pattern_list from DB into MACD_patterns
        conn = self._engine.connect()
        s = select([self._tb_TradeSignal.c.MACD_pattern_number]).distinct()
        MACD_patterns = conn.execute(s).fetchall()

        # Fetch out closing data from DB
        sql_fetch_halfHour_records = (
        "select stock_code, close_price, quote_time from tb_StockXMinRecords where period = %s and stock_code = %s")
        df_stock_records = pd.read_sql(sql_fetch_halfHour_records, con=self._engine, params=(period, stock_code),
                                       index_col='quote_time')
        self._clean_table('tb_MACD_Trades_HalfHour')
        self._multi_processors_cal_MACD_ending_profits(MACD_patterns, df_stock_records)

    def _multi_processors_cal_MACD_ending_profits(self, MACD_patterns, df_stock_records):
        total_pattern = len(MACD_patterns)
        processors = 7
        index_beg = 0
        index_end = total_pattern / 7
        pattern_slices = []
        for i in range(8):
            if i != 7:
                pattern_slices.append(MACD_patterns[index_beg:index_end])
                index_beg = index_end
                index_end = i * (total_pattern / 7)
            else:
                pattern_slices.append(MACD_patterns[index_beg:])
        # print pattern_slices[0][0][0]
        self._MACD_ending_profits_calculation(df_stock_records, pattern_slices[0])
        p1 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[0],))
        p2 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[1],))
        p3 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[2],))
        p4 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[3],))
        p5 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[4],))
        p6 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[5],))
        p7 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[6],))
        p8 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[7],))
        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()
        p6.start()
        p7.start()
        p8.start()
        p1.join()
        p2.join()
        p3.join()
        p4.join()
        p5.join()
        p6.join()
        p7.join()
        p8.join()

    def _MACD_ending_profits_calculation(self, df_stock_close_prices, pattern_slices):
        # df_StockIndex_MACD, df_stock_close_prices, MACD_TradeArray, MACD_Tradetype, EMA_short_windows, EMA_long_windows, DIF_windows, eachPattern
        # Create an DF for storing transaction data
        stockVolume_Begin = 10000
        cash_Begin = 200000.00
        tradeVolume = 10000
        cash_Current = cash_Begin
        stockVolume_Current = stockVolume_Begin
        totalValue_Begin = (stockVolume_Begin * df_stock_close_prices.close_price[0] + cash_Begin)
        totalValue_Current = totalValue_Begin
        profit_Rate = 0
        MACD_trade_column_names = ['stock_code', 'quote_time', 'tradeVolume', 'tradeCost',
                                   'stockVolume_Current', 'cash_Current', 'totalValue_Current', 'profit_Rate',
                                   'EMA_short_window', 'EMA_long_window', 'DIF_window', 'MACD_pattern_number']
        engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')

        widgets = ['MACD_Pattern_BackTest: ',
                   progressbar.Percentage(), ' ',
                   progressbar.Bar(marker='0', left='[', right=']'), ' ',
                   progressbar.ETA()]
        progress = progressbar.ProgressBar(widgets=widgets)

        for eachPattern in progress(pattern_slices):
            sql_fetch_signals = ('select * from tb_StockIndex_MACD_New where MACD_pattern_number = %s')
            pattern = eachPattern[0]
            df_MACD_signals = pd.read_sql(sql_fetch_signals, params=(pattern,), con=engine)
            EMA_short_windows = df_MACD_signals.EMA_short_window[0]
            EMA_long_windows = df_MACD_signals.EMA_long_window[0]
            DIF_windows = df_MACD_signals.DIF_window[0]
            MACD_trades = pd.DataFrame(columns=MACD_trade_column_names)  # create DF to save transactions

            numMACD = len(df_MACD_signals)
            for i in range(numMACD):
                '''
                df_stock_close_prices has a full list of every half hour,
                but df_StockIndex_MACD only has value when Signal != 0.
                Need to find out the close_price which record_time == StockIndex_MACD.record_time
                '''
                close_price = \
                df_stock_close_prices[df_stock_close_prices.index == df_MACD_signals.quote_time[i]].close_price.iloc[0]
                trade_cost = close_price * tradeVolume

                # Evaluate the trading signals and calculate the profits
                if df_MACD_signals.Signal[i] == 1:  # Positive Signal, buy more stocks
                    if cash_Current >= trade_cost:  # Have enough cash in hand
                        # tradeVolume = stockVolume_Current * tradePercent
                        stockVolume_Current = tradeVolume + stockVolume_Current
                        cash_Current = cash_Current - trade_cost
                        totalValue_Current = stockVolume_Current * close_price + cash_Current
                        profit_Rate = math.log(totalValue_Current / totalValue_Begin)

                        # Append HalfHour Profit Results into Array
                        transaction = [df_stock_close_prices.stock_code[i], df_MACD_signals.quote_time[i],
                                       tradeVolume, trade_cost, stockVolume_Current,
                                       cash_Current, totalValue_Current,
                                       profit_Rate, EMA_short_windows,
                                       EMA_long_windows, DIF_windows, pattern]
                        MACD_trades.loc[len(MACD_trades)] = transaction
                    else:
                        pass
                elif df_MACD_signals.Signal[i] == (-1):  # NEgative Signal, sell stocks
                    if stockVolume_Current >= tradeVolume:  # Have enough stocks in hand
                        stockVolume_Current = stockVolume_Current - tradeVolume
                        cash_Current = cash_Current + trade_cost
                        totalValue_Current = stockVolume_Current * close_price + cash_Current
                        profit_Rate = math.log(totalValue_Current / totalValue_Begin)

                        # Append HalfHour Profit Results into Array
                        transaction = [df_stock_close_prices.stock_code[i], df_MACD_signals.quote_time[i],
                                       tradeVolume, trade_cost, stockVolume_Current,
                                       cash_Current, totalValue_Current,
                                       profit_Rate, EMA_short_windows,
                                       EMA_long_windows, DIF_windows, pattern]
                        MACD_trades.loc[len(MACD_trades)] = transaction
                    else:
                        pass
                else:
                    pass
            if MACD_trades.size != 0:
                MACD_trades.to_sql('tb_MACD_Trades_HalfHour', con=engine, if_exists='append', index=False)



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
    MACDPattern._MACD_ending_profits()
    #MACDPattern._clean_table('tb_StockIndex_MACD_New')

if __name__ == '__main__':
    main()
