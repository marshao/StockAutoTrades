#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import datetime, time, progressbar, math
import pandas as pd
from sqlalchemy import create_engine, Table, Column, MetaData
from sqlalchemy.sql import select, and_, or_, not_
# from multiprocessing import Pool
import multiprocessing as mp
from CommuSocket import commu
from GetRealData import get_data_qq


# import matplotlib.pyplot as plt



# from C_GetDataFromWeb  *


class C_Algorithems_BestPattern(object):
    def __init__(self):
        self._input_dir = '/home/marshao/DataMiningProjects/Input/'
        self._output_dir = '/home/marshao/DataMiningProjects/Output/'
        self._stock_name_file = self._output_dir + 'StockNames.csv'
        # self._stock_codes = ['sz300226', 'sh600887','300146','600221']
        self._stock_codes = ['sz300226']
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
        self._trade_history_column = ['stock_code','trade_type','trade_volumn','trade_price','trade_time', 'trade_algorithem_name', 'trade_algorithem_method', 'stock_record_period','trade_result']

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return time_stamp

    def _time_tag_dateonly(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.datetime.now()
        only_date = time_stamp.date()
        return only_date

    def _write_log(self, log_mesg='', logPath='operLog.txt'):
        fullPath = self._output_dir + logPath
        with open(fullPath, 'a') as log:
            log.writelines(log_mesg)

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

    def best_pattern_daily_calculate(self):
        for stock_code in self._stock_codes:
            self._MACD_trading_signals()
            self._MACD_ending_profits()
            self._MACD_best_pattern()

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

    def _MACD_signal_calculation(self, df_MACD_index, df_stock_records, to_DB=True):
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
            if to_DB:  # to_DB == True if function call from data saving, to_DB ==False function call from apply, not need to save to db
                df_save.to_sql('tb_StockIndex_MACD_New', con=engine, if_exists='append', index=True)
        return df

    def apply_best_MACD_pattern_to_data(self, pattern_number=756, period='m30', stock_code='sz300226'):
        done = False
        # Get Best Pattern Number from DB
        pattern_number = self._get_best_pattern(stock_code)
        # Use Best Pattern Number to retrieve EMA_long, short and DIF window
        sql_select_MACD_pattern = ("select * from tb_MACDIndex where id_tb_MACDIndex = %s")
        df_MACD_index = pd.read_sql(sql_select_MACD_pattern, params=(pattern_number,), con=self._engine,
                                    index_col='id_tb_MACDIndex')
        # Fetch corresponsding stock records
        sql_fetch_halfHour_records = ("select * from tb_StockXMinRecords where period = %s and stock_code = %s")

        df_stock_records = pd.read_sql(sql_fetch_halfHour_records, con=self._engine, params=(period, stock_code),
                                       index_col='quote_time')
        # Send to calculate trade signal according to the best pattern
        df_signals = self._MACD_signal_calculation(df_MACD_index, df_stock_records, to_DB=False)[-1:]
        # return  df_signals
        if df_signals.Signal[0] != 0:  # 3 need to be change to 0
            # Get Stock Avaliable Informatoin
            done, df_stock_infor = self._checking_stock_in_hand(stock_code[2:])
            if done:
                # Get real time price information
                done, df_current_price = self._get_stock_current_price(stock_code)
                if done:
                    # Get cash avaliable information
                    done, cash_avaliable = self._get_stock_asset()
                    if done:
                        done = self._send_trading_command(df_stock_infor, df_current_price, cash_avaliable,
                                                   df_signals.Signal[0], pattern_number, period)
                        if done:
                            self._log_mesg = 'Trading command had been executed successfully at %s'%self._time_tag()
                        else:
                            self._log_mesg = 'Trading command had failed to be executed successfully at %s' % self._time_tag()
                    else:
                        self._log_mesg = "Unable to get cash avalible infomation at %s"%self._time_tag()
                else:
                    self._log_mesg = "Unable to get real time price information at %s"%self._time_tag()
            else:
                self._log_mesg = "Unable to get stock_avalible information at %s"%self._time_tag()
        else:
            self._log_mesg = "Trade Signal for stock %s is 0 at %s" % (stock_code, self._time_tag())
        print self._log_mesg

    def _checking_stock_in_hand(self, stock_code):
        # Need to udpate stock_in_hand Information first
        # This will request to send a code to frontend, and wait for response from confirmation from front end.
        receive = commu('1')  # update stock in hand information
        # 1.1 means remote getting stock in hand information runs correctly, program can be continue.
        done = False
        sql_select_stock_infor = (
        "select stockCode, stockAvaliable, currentValue, Datetime from tb_StockInhand where stockCode = %s order by Datetime DESC limit 1")
        if receive == '1.1':
            df_stock_infor = pd.read_sql(sql_select_stock_infor, params=(stock_code,), con=self._engine)
            print df_stock_infor
            done = True
            return done, df_stock_infor
        else:
            df_stock_infor = pd.read_sql(sql_select_stock_infor, params=(stock_code,), con=self._engine)
            self._log_mesg = "Could not get current stock in hand information at %s" % self._time_tag()
            return done, df_stock_infor

    def _get_stock_current_price(self, stock_code):
        # gd = C_GettingData()
        done = get_data_qq(stock_code)
        sql_select_current_price = (
            "select stock_code,current_price, buy1_price, buy2_price, sale1_price, sale2_price  from tb_StockRealTimeRecords where stock_code = %s ORDER by time DESC limit 1")
        if done:
            df_current_price = pd.read_sql(sql_select_current_price, params=(stock_code[2:],), con=self._engine)
            return done, df_current_price
        else:
            self._log_mesg = "Could not get current price data at %s" % self._time_tag()
            df_current_price = pd.read_sql(sql_select_current_price, params=(stock_code[2:],), con=self._engine)
            done = False
            return done, df_current_price
        ### how to call the function to get current price

    def _get_stock_asset(self):
        done = False  # The mark of success or not.
        cash_avabliable = -1
        cmd_line = '5'
        receives = commu(cmd_line).split()
        if receives[0] == '5.1':
            cash_avabliable = float(receives[1])
            done = True
        else:
            pass
        return done, cash_avabliable

    def _send_trading_command(self, df_stock_infor, df_current_price, cash_avaliable, signal, pattern_number, period):
        #'stock_code','trade_type','trade_volumn','trade_price','trade_time',
        # 'trade_algorithem_name', 'trade_algorithem_method', 'stock_record_period','trade_result'
        # signal = 3
        df_trade_history = pd.DataFrame(columns=self._trade_history_column)
        stock_code = df_stock_infor.stockCode[0]
        stock_avaliable = df_stock_infor.stockAvaliable[0]
        current_value = df_stock_infor.currentValue[0]
        trade_volumn = 0
        volumn_up_limit = 2000
        volumn_down_limit = 1000
        trade_algorithem_name = 'MACD Best Pattern'
        trade_algorithem_method = pattern_number
        done = False
        line = []
        trade_result = 0

        if signal == 2:  # 1 == buy,
            # Need to evaluate the cash avalible is enough to buy at least 1000 stocks
            # And also make sure the buy up limit will not over 2000 stocks
            current_price = df_current_price.current_price[0]
            buy1_price = df_current_price.buy1_price[0]
            buy2_price = df_current_price.buy2_price[0]
            trade_volumn = int(cash_avaliable/current_price/100)*100
            print trade_volumn
            if trade_volumn >= volumn_up_limit:
                trade_volumn = str(volumn_up_limit)
            elif trade_volumn <= volumn_down_limit:
                trade_volumn = '0'
            else:
                trade_volumn = str(trade_volumn)
                trade_result = 4

            cmd = '2 ' + stock_code+' ' + trade_volumn+' '+ str(current_price)
            df_trade_history.trade_type.loc[0] = int(signal)
        elif signal == 3:  # 3 == sale
            # Need to sale all stocks
            current_price = df_current_price.current_price[0]
            sale1_price = df_current_price.sale1_price[0]
            sale2_price = df_current_price.sale2_price[0]
            cmd = '3 ' + stock_code + ' ' + str(stock_avaliable) + ' ' + str(current_price)
        else:
            print "Unknown trading Signal"

        # Send trading command and analysis the result
        receives = commu(cmd).split()
        #print receives
        if receives[0] == '2.1' or receives[0] == '3.1':
            print 'Run into confirmed code'
            line.append(stock_code)
            line.append(receives[0])
            line.append(trade_volumn)
            line.append(current_price)
            line.append(self._time_tag())
            line.append(trade_algorithem_name)
            line.append(trade_algorithem_method)
            line.append(period)
            line.append(trade_result)
            df_trade_history.loc[len(df_trade_history)] = line
            df_trade_history.to_sql('tb_StockTradeHistory', con=self._engine, index=False, if_exists='append')
            print df_trade_history
            done = True
        else:
            pass

        return done

    def _get_best_pattern(self, stock_code):
        sql_select_best_pattern = (
        "select best_pattern from tb_StockBestPatterns where algorithem_name = 'MACD' and stock_code = %s ORDER by pattern_date DESC limit 1")
        con = self._engine.connect()
        result = con.execute(sql_select_best_pattern, stock_code)
        pattern = result.fetchall()[0]
        print pattern
        return pattern



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
        print total_pattern
        processors = 7
        index_beg = 0
        index_end = total_pattern / 7
        pattern_slices = []

        for i in range(8):
            if i != 7:
                print "i:%s,  index_beg %s , index_end %s " %(i, index_beg, index_end)
                pattern_slices.append(MACD_patterns[index_beg:index_end])
                index_beg = index_end
                index_end = index_end + total_pattern/7
            #else:
            #    print "i:%s,  index_beg %s , index_end %s " % (i, index_beg, index_end)
            #    pattern_slices.append(MACD_patterns[index_beg:])
            #i += 1


        p1 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[0],))
        p2 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[1],))
        p3 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[2],))
        p4 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[3],))
        p5 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[4],))
        p6 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[5],))
        p7 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[6],))
        #p8 = mp.Process(target=self._MACD_ending_profits_calculation, args=(df_stock_records, pattern_slices[7],))
        p1.start()
        p2.start()
        p3.start()
        p4.start()
        p5.start()
        p6.start()
        p7.start()
        #p8.start()
        p1.join()
        p2.join()
        p3.join()
        p4.join()
        p5.join()
        p6.join()
        p7.join()
        #p8.join()

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

    def _clean_table(self, table_name):
        conn = self._engine.connect()
        conn.execute("truncate %s" % table_name)
        print "Table is cleaned"

    def _MACD_best_pattern(self):
        # For now, the best pattern is the pattern witch can make the highest ending profit.
        sql_select_ending_profits = (
        'select stock_code, MACD_pattern_number as best_pattern, profit_rate from tb_MACD_Trades_HalfHour order by profit_rate DESC limit 1')
        df_MACD_ending_profits = pd.read_sql(sql_select_ending_profits, con=self._engine)
        df_MACD_ending_profits['algorithem_name'] = 'MACD'
        df_MACD_ending_profits['pattern_date'] = self._time_tag()
        df_MACD_ending_profits.to_sql('tb_StockBestPatterns', con=self._engine, index=False, if_exists='append')
        # gb_pattern = df_MACD_ending_profits.groupby('MACD_pattern_number')
        print df_MACD_ending_profits



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
    def __init__(self):
        C_Algorithems_BestPattern.__init__(self)
        self._ep = 0
        self._previous_SAR = 0
        self._price_high = 0
        self._price_low = 0
        self._AF = 0.02
        self._AF_limit = 0.20
        self._SAR_window = 5
        # matplotlib.style.use('ggplot')

    def _getting_stock_records(self, stock_code='sz300226', period='m30'):
        sql_read_records = (
        "select stock_code, quote_time, high_price, low_price, close_price from tb_StockXMinRecords where stock_code = %s and period = %s")
        df_records = pd.read_sql(sql_read_records, con=self._engine, params=(stock_code, period))
        return df_records

    def SAR_calculation(self, stock_code='sz300226', period='m30', records_window=5, af=0.02, af_limit=0.20):
        df_records = self._getting_stock_records(stock_code, period)
        trend, sar, ep = self._begining_trend(df_records, records_window)
        af_window = af
        df_records['SAR'] = sar
        df_records['EP'] = ep
        df_records['AF_value'] = af
        df_records['AF_limit'] = af_limit
        df_records['AF_window'] = af
        df_records['EPSAR'] = ep - sar
        df_records['Signal'] = 0
        records_count = len(df_records)
        i = records_window
        while i < records_count:
            prior_sar = df_records.SAR[i - 1]
            prior_af = df_records.AF_value[i - 1]
            prior_ep = df_records.EP[i - 1]
            if trend == 1:
                sar = float("%0.4f" % (prior_sar + prior_af * (prior_ep - prior_sar)))
                if af < af_limit:
                    af = af + af_window
                ep = df_records.high_price[i - records_window:i].max()
                df_records.SAR[i] = sar
                df_records.EP[i] = ep
                df_records.AF_value[i] = af
                df_records.EPSAR[i] = ep - sar
                if df_records.close_price[i] < sar:
                    trend = -1
                    df_records.Signal[i] = -1
                i += 1
            else:
                sar = float("%0.4f" % (prior_sar - prior_af * (prior_sar - prior_ep)))
                if af < af_limit:
                    af = af + af_window
                ep = df_records.low_price[i - records_window:i].min()
                df_records.SAR[i] = sar
                df_records.EP[i] = ep
                df_records.AF_value[i] = af
                df_records.EPSAR[i] = ep - sar
                if df_records.close_price[i] > sar:
                    trend = 1
                    df_records.Signal[i] = 1
                i += 1
        df_records.to_sql('tb_StockIndex_SAR', con=self._engine, if_exists='append', index=False)

    def _begining_trend(self, df_records, window):
        trend = 1
        sar = 0
        ep = 0
        sar = df_records.close_price[0:window].mean()
        day_window_close = df_records.close_price[window - 1]
        if day_window_close > sar:
            ep = df_records.high_price[0:window].max()
            return trend, sar, ep
        elif day_window_close <= sar:
            ep = df_records.low_price[0:window].min()
            trend = -1
            return trend, sar, ep

    def plot_df(self):
        sql_select = ("select quote_time, close_price, SAR from tb_StockIndex_SAR")
        df = pd.read_sql(sql_select, con=self._engine, index_col='quote_time')
        print df
        # plt.figure()
        #df.plot()

    def _sending_signal(self):
        pass






def main():
    SARPattern = C_BestSARPattern()
    # MACDPattern._get_best_pattern('sz300226')

    SARPattern.plot_df()
    #MACDPattern._clean_table('tb_StockIndex_MACD_New')

if __name__ == '__main__':
    main()

