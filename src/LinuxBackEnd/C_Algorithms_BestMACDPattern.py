#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import datetime, time, progressbar, math
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Table, Column, MetaData
from sqlalchemy.sql import select, and_, or_, not_
import multiprocessing as mp
from CommuSocket import commu
from GetRealData import get_data_qq
import C_GlobalVariable as glb


import cProfile



class C_Algorithems_BestPattern(object):
    '''
    Change: 2017-06-22 in _send_trading_command: modified the received signal to 1 and -1 to match the signal calculation result.
            2017-06-22 in _MACD_Singal_calcualtion_cross use MACD_short = 999 to represent real life stock operation and save them in DB.
    '''
    def __init__(self):
        gv = glb.C_GlobalVariable()
        self._master_config = gv.get_master_config()
        self._calcu_config = gv.get_calcu_config()
        self._emailobj = gv.get_emailobj()

        self._input_dir = self._master_config['ubuntu_input_dir']
        self._output_dir = self._master_config['ubuntu_output_dir']
        self._trading_volume = self._calcu_config['trading_volume']
        self._stock_inhand_uplimit = self._calcu_config['stock_inhand_uplimit']
        self._op_log = self._master_config['op_log']
        self._processors = self._calcu_config['ubuntu_processors']
        self._engine = self._master_config['dev_db_engine']
        self._metadata = MetaData(self._engine)

        self._stock_name_file = self._output_dir + 'StockNames.csv'
        self._SAR_log = self._output_dir + 'SARLog'
        self._stock_codes = ['sz002310']
        self._stock_market = ""
        self._window = '10'
        self._log_mesg = ''
        self._my_columns = []
        self._my_DF = pd.DataFrame(columns=self._my_columns)
        self._x_min = ['m5', 'm15', 'm30', 'm60']
        self._x_period = ['day', 'week']
        self._trade_history_column = ['stock_code', 'trade_type', 'trade_volumn', 'trade_price', 'trade_time',
                                      'trade_algorithem_name', 'trade_algorithem_method', 'stock_record_period',
                                      'trade_result']

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

    def _write_log(self, log_mesg, logPath='operLog.txt'):
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

    def _clean_table(self, table_name):
        conn = self._engine.connect()
        conn.execute("truncate %s" % table_name)
        print "Table %s is cleaned" % table_name

    def _progress_monitor(self):
        '''
        Setting up progress bar to monitor the progress of whole program.
        '''
        widgets = ['MACD_Pattern_BackTest: ',
                   progressbar.Percentage(), ' ',
                   progressbar.Bar(marker='0', left='[', right=']'), ' ',
                   progressbar.ETA()]
        progress = progressbar.ProgressBar(widgets=widgets)
        return progress

    def _processes_pool(self, tasks, processors):
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
            #print "This is the %s round" % i
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

    def _update_stock_inhand(self):
        receive = commu('1')  # update stock in hand information
        # 1.1 means remote getting stock in hand information runs correctly, program can be continue.
        print "Updated Stock Inhand"
        if receive != 'err':
            self._log_mesg = self._log_mesg + "     Update stock inhand information successfully at %s \n" % self._time_tag()
        else:
            self._log_mesg = self._log_mesg + "     Error: Cannot update stock inhand information successfully at %s \n" % self._time_tag()
            subject = "Front End Server Communication Error"
            message = 'Front end server error, failed to upate stock inhand information'
            self._emailobj.send_email(subject=subject, body=message)
        self._write_log(self._log_mesg)

    def _checking_stock_in_hand(self, stock_code):
        # Need to udpate stock_in_hand Information first
        # This will request to send a code to frontend, and wait for response from confirmation from front end.
        done = True
        now = self._time_tag()
        sql_select_stock_infor = (
            "select stockCode, stockRemain, stockAvaliable, currentValue, quote_time from tb_StockInhand where stockCode = %s and Datetime < %s and Datetime > %s - INTERVAL 6 HOUR order by quote_time DESC limit 1")

        df_stock_infor = pd.read_sql(sql_select_stock_infor, params=(stock_code, now, now), con=self._engine)
        self._log_mesg = self._log_mesg + "     Get df_stock_infor %s at %s \n" % (df_stock_infor, self._time_tag())

        if df_stock_infor.empty:
            self._log_mesg = self._log_mesg + "     There is currently no stock %s in hand at %s \n." % (
            stock_code, self._time_tag())
            df_stock_infor.set_value(0, 'stockCode', stock_code)
            df_stock_infor.set_value(0, 'stockAvaliable', 0)
            df_stock_infor.set_value(0, 'currentValue', 0.0)
            done = True
        self._write_log(self._log_mesg)
        # print df_stock_infor
        return done, df_stock_infor

    def _get_stock_current_price(self, stock_code):
        # gd = C_GettingData()
        done = get_data_qq(stock_code)
        sql_select_current_price = (
            "select stock_code,current_price, buy1_price, buy2_price, sale1_price, sale2_price  from tb_StockRealTimeRecords where stock_code = %s ORDER by time DESC limit 1")
        if done:
            df_current_price = pd.read_sql(sql_select_current_price, params=(stock_code[2:],), con=self._engine)
            self._log_mesg = self._log_mesg + "     Get stock %s current price at %s .\n" % (
            stock_code, self._time_tag())
        else:
            df_current_price = pd.read_sql(sql_select_current_price, params=(stock_code[2:],), con=self._engine)
            done = False
            self._log_mesg = self._log_mesg + "     Could not get stock %s current price data at %s\n" % (
            stock_code, self._time_tag())
            ### how to call the function to get current price
        # self._write_log(self._log_mesg)
        return done, df_current_price

    def _get_cash_avaliable(self):
        done = False  # The mark of success or not.
        cash_avabliable = -1
        cmd_line = '5'
        receives = commu(cmd_line).split()

        if receives[0] == '5.1':
            cash_avabliable = float(receives[1])
            done = True
            self._log_mesg = self._log_mesg + "     Get stock asset at %s \n" % (self._time_tag())
        else:
            self._log_mesg = self._log_mesg + "     Cannot get stock asset at %s \n" % (self._time_tag())
            pass

        #self._write_log(self._log_mesg)
        return done, cash_avabliable

    def _check_stock_resale(self, ):

        pass

    def _send_trading_command(self, df_stock_infor, df_current_price, cash_avaliable, signal, pattern_number, period):
        '''
        Sending Trading singal to Windows Front End
        :param df_stock_infor:
        :param df_current_price:
        :param cash_avaliable:
        :param signal: source signals from algorithem( 1 for buy, -1 for sale, 0 for nothing).
        :param pattern_number:
        :param period:
        :return:
        '''
        df_trade_history = pd.DataFrame(columns=self._trade_history_column)
        stock_code = df_stock_infor.stockCode[0]
        stock_Remain = df_stock_infor.stockRemain[0]
        stock_avaliable = df_stock_infor.stockAvaliable[0]
        current_value = df_stock_infor.currentValue[0]
        trade_volumn = 0
        volumn_up_limit = self._stock_inhand_uplimit
        #volumn_down_limit = 500
        oneshoot = self._trading_volume
        trade_algorithem_name = 'MACD Best Pattern'
        trade_algorithem_method = pattern_number
        done = False
        line = []
        trade_result = 0
        cmd = '5'

        if signal == 1:  # 1 == buy,
            print "Buying Stock %s" % stock_code
            # Need to evaluate the cash avalible is enough to buy at least 1000 stocks
            # And also make sure the buy up limit will not over 2000 stocks
            current_price = df_current_price.current_price[0]
            buy1_price = df_current_price.buy1_price[0]
            buy2_price = df_current_price.buy2_price[0]
            trade_volumn = int(cash_avaliable / current_price / 100) * 100
            #print trade_volumn
            print stock_Remain
            if (stock_Remain + oneshoot) < volumn_up_limit:
                if trade_volumn >= oneshoot:
                    trade_volumn = str(oneshoot)
                else:
                    trade_volumn = str(trade_volumn)
                cmd = '2 ' + stock_code + ' ' + trade_volumn + ' ' + str(current_price)
                self._log_mesg = self._log_mesg + "     Stock %s has the amount %s in hand at %s \n" % (
                    stock_code, stock_Remain, self._time_tag())
            else:
                self._log_mesg = self._log_mesg + "     Cannot buy more stock %s because " \
                                                  "the amount reach the up limit at %s \n" % (
                                                  stock_code, self._time_tag())
            df_trade_history.trade_type.loc[0] = int(signal)
        elif signal == -1:  # -1 == sale
            # Need to sale all stocks except the amount purchased in the same day
            '''
            today = self._time_tag_dateonly()
            sql_select_buy_today = ("select sum(trade_volumn) where stock_code = stock_code and Date_FORMAT(trade_time, '%Y-%m-%d') = today")
            today_buy = conn.execute(sql_select_buy_today).fetchall()[0][0]
            stock_avaliable = stock_avaliable - today_buy
            '''
            print "Saling stock %s" % stock_code
            current_price = df_current_price.current_price[0]
            sale1_price = df_current_price.sale1_price[0]
            sale2_price = df_current_price.sale2_price[0]
            if stock_avaliable != 0:
                cmd = '3 ' + stock_code + ' ' + str(stock_avaliable) + ' ' + str(current_price)
                trade_volumn = stock_avaliable
            elif stock_Remain != 0 and stock_avaliable == 0:
                trade_volumn = stock_Remain
                trade_type = 9 # 9 is the resale sign
                self._log_mesg = self._log_mesg + "     T1 policy blocking, No avaliable stock %s in hand, stock_Remain is %s , sales is cancelled " \
                                              "at %s \n." % (stock_code, stock_Remain, self._time_tag())
            else:
                self._log_mesg = self._log_mesg + "     No avaliable stock %s in hand, sales is cancelled " \
                                                  "at %s \n." % (stock_code, self._time_tag())
        else:
            print "Unknown trading Signal"

        self._log_mesg = self._log_mesg + "     Trading command CMD %s is sent at %s \n" % (cmd, self._time_tag())
        # Send trading command and analysis the result
        receives = commu(cmd).split()
        trade_type = receives[0]
        self._log_mesg = self._log_mesg + "     CMD %s is received at %s \n" % (cmd, self._time_tag())
        print self._log_mesg
        print "Receive is %s" % receives
        if receives[0] == '2.1' or receives[0] == '3.1':
            print 'Run into confirmed code'
            line.append(stock_code)
            line.append(trade_type)
            line.append(trade_volumn)
            line.append(current_price)
            line.append(self._time_tag())
            line.append(trade_algorithem_name)
            line.append(trade_algorithem_method)
            line.append(period)
            line.append(trade_result)
            df_trade_history.loc[len(df_trade_history)] = line
            df_trade_history.to_sql('tb_StockTradeHistory', con=self._engine, index=False, if_exists='append')
            self._log_mesg = self._log_mesg + "     Trades %s is saved at %s \n" % (df_trade_history, self._time_tag())
            print df_trade_history
            done = True
        else:
            pass

        #self._write_log(self._log_mesg)
        return done


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

    def best_pattern_daily_calculate(self, period=None, stock_codes=None):
        if stock_codes is None:
            stock_codes = self._stock_codes
        MACD_Ending_Profit_Cal = C_MACD_Ending_Profit_Calculation()
        MACD_Trading_Signal_Cal = C_MACD_Signal_Calculation()
        if period is None:
            period = 'day'
        for stock_code in stock_codes:
            MACD_Trading_Signal_Cal._MACD_trading_signals(period=period, stock_code=stock_code)
            MACD_Ending_Profit_Cal._MACD_ending_profits(period=period, stock_code=stock_code)
            self._save_MACD_best_pattern(period=period)

    def apply_best_MACD_pattern_to_data(self, period, stock_code, quo=None, ga=None, beta = None):
        done = False
        if quo is None:
            quo = 0.7
        if ga is None:
            ga = 0.3
        if beta is None:
            beta = 0.2

        self._clean_table('tb_StockIndex_MACD_New')
        # Initialize Signal Calculation
        C_MACD_Signal_Calculator = C_MACD_Signal_Calculation()
        # Get Best Pattern Number from DB
        pattern_number = self._get_best_pattern(stock_code)
        # Use Best Pattern Number to retrieve EMA_long, short and DIF window
        sql_select_MACD_pattern = ("select * from tb_MACDIndex where id_tb_MACDIndex = %s")
        df_MACD_index = pd.read_sql(sql_select_MACD_pattern, params=(pattern_number,), con=self._engine,
                                    index_col='id_tb_MACDIndex')
        # Fetch corresponsding stock records
        sql_fetch_min_records = (
            # "select * from tb_StockXMinRecords where period = %s and stock_code = %s order by quote_time DESC limit 550")
            "select * from (select * from tb_StockXMinRecords where period = %s and stock_code = %s order by quote_time DESC limit 555) as tbl order by quote_time ASC")
        sql_fetch_period_records = (
            "select * from tb_StockXPeriodRecords where period = %s and stock_code = %s")
        if period == 'day' or period == 'week':
            sql_fetch_records = sql_fetch_period_records
        else:
            sql_fetch_records = sql_fetch_min_records

        df_stock_records = pd.read_sql(sql_fetch_records, con=self._engine, params=(period, stock_code),
                                       index_col='quote_time')
        # Send to calculate trade signal according to the best pattern
        # '''
        df_signals = C_MACD_Signal_Calculator._MACD_signal_calculation_M30_3(df_MACD_index, df_stock_records,
                                                                             to_DB=True, for_real=True, quo=quo, ga=ga,
                                                                             beta=beta).tail(1)
        '''
        df_signals = C_MACD_Signal_Calculator._MACD_signal_calculation_M30_3(df_MACD_index, df_stock_records,
                                                                             to_DB=True, for_real=True, quo=quo, ga=ga,
                                                                             beta=beta)
        df_signals.to_csv('/home/marshao/DataMiningProjects/Output/signals.csv')
        '''
        signal = df_signals.Signal[0]
        # signal = -1# This line need to be removed
        print "\n -------------------------------"
        print "Step1: Trading Signal is %s" % signal
        self._log_mesg = self._log_mesg + "-----------------------------------------------------\n"
        self._log_mesg = self._log_mesg + "Step1: Get Trading Signal at %s \n" % self._time_tag()
        self._log_mesg = self._log_mesg + \
                         "      MACD Pattern %s with quo=%s and ga = %s was applied on stock %s, signal %s was got! at %s \n" % (
                             pattern_number, quo, ga, stock_code, signal, self._time_tag())

        if signal != 0:  # 3 need to be change to 0
            # Get Stock Avaliable Informatoin
            print "---------------------------------------------------"
            print "Step2: Get stock Avaliable"
            self._log_mesg = self._log_mesg + "Step2: Get stock Avaliable at %s \n" % self._time_tag()
            done, df_stock_infor = self._checking_stock_in_hand(stock_code[2:])
            print df_stock_infor
            if (done and signal == -1 and df_stock_infor.stockAvalible[0] != 0) or \
                    (signal == 1 and (
                        (df_stock_infor.stockRemain[0] + self._trading_volume) < self._stock_inhand_uplimit)):
                # Get real time price information
                print "---------------------------------------------------"
                print "Step3: Get real time price"
                self._log_mesg = self._log_mesg + "Step3: Get real time price at %s \n" % self._time_tag()
                done, df_current_price = self._get_stock_current_price(stock_code)
                if done:
                    # Get cash avaliable information
                    print "---------------------------------------------------"
                    print "Step4: Got cash Avaliable"
                    self._log_mesg = self._log_mesg + "Step4: Get cash Avaliable at %s \n" % self._time_tag()
                    done, cash_avaliable = self._get_cash_avaliable()
                    if done:
                        print "-------------------------------------------------"
                        print "step5: Send Traiding Singal"
                        self._log_mesg = self._log_mesg + "Step5: Send Trading Signal at %s \n" % self._time_tag()
                        done = self._send_trading_command(df_stock_infor, df_current_price, cash_avaliable,
                                                          signal, pattern_number, period)
                        if done:
                            self._log_mesg = self._log_mesg + '     Trading command had been executed successfully at %s \n' % self._time_tag()
                        else:
                            self._log_mesg = self._log_mesg + '     Trading command had failed to be executed successfully at %s \n' % self._time_tag()
                    else:
                        self._log_mesg = self._log_mesg + "     Unable to get cash avalible infomation at %s \n" % self._time_tag()
                else:
                    self._log_mesg = self._log_mesg + "     Unable to get real time price information at %s \n" % self._time_tag()
            else:
                self._log_mesg = self._log_mesg + "     Unable to get stock_avalible information or stockRemain %s over " \
                                                  "the up limit %s at %s \n" % (
                                                  df_stock_infor.stockRemain[0], self._stock_inhand_uplimit,
                                                  self._time_tag())
        else:
            self._log_mesg = self._log_mesg + "     Trade Signal for stock %s is 0 at %s \n" % (
            stock_code, self._time_tag())
        self._log_mesg = self._log_mesg + "     Trading Process is finished at %s \r\n" % self._time_tag()
        self._write_log(self._log_mesg)

        finish_sign = True
        return finish_sign

    def _get_best_pattern(self, stock_code, simplified=True):
        sql_select_best_pattern = (
            "select best_pattern, profit_rate from tb_StockBestPatterns where algorithem_name = 'MACD' and stock_code = %s ORDER by pattern_date DESC limit 1")
        # sql_select_best_pattern_parameters = (
        #    "select * from tb_StockIndex_MACD_New where MACD_pattern_number = %s ORDER by quote_time DESC limit 1")

        con = self._engine.connect()
        result = con.execute(sql_select_best_pattern, stock_code).fetchall()
        pattern = result[0][0]
        profit = result[0][1]
        # parameters = con.execute(sql_select_best_pattern_parameters, pattern).fetchall()
        # self._log_mesg = self._log_mesg + "     At %s, stock %s pattern %s with parameters %s did best profit: %s  at %s \r\n" % (
        #    parameters[0][8], stock_code, pattern, parameters[0][14], profit, self._time_tag())
        self._log_mesg = self._log_mesg + "     Stock %s pattern %s did best profit: %s  at %s \r\n" % (
        stock_code, pattern, profit, self._time_tag())
        if simplified:
            self._write_log(self._log_mesg)
        # print pattern
        return pattern

    def _save_MACD_best_pattern(self, period):
        # For now, the best pattern is the pattern witch can make the highest ending profit.
        if period == 'm30':
            sql_select_ending_profits = (
            'select stock_code, MACD_pattern_number as best_pattern, profit_rate from tb_MACD_Trades_HalfHour where DIF_window = 0 order by profit_rate DESC limit 1')
        else:
            sql_select_ending_profits = (
                'select stock_code, MACD_pattern_number as best_pattern, profit_rate from tb_MACD_Trades where DIF_window = 0 order by profit_rate DESC limit 1')

        df_MACD_ending_profits = pd.read_sql(sql_select_ending_profits, con=self._engine)
        df_MACD_ending_profits['algorithem_name'] = 'MACD'
        df_MACD_ending_profits['pattern_date'] = self._time_tag()
        df_MACD_ending_profits.to_sql('tb_StockBestPatterns', con=self._engine, index=False, if_exists='append')
        # gb_pattern = df_MACD_ending_profits.groupby('MACD_pattern_number')
        print df_MACD_ending_profits
        return df_MACD_ending_profits.best_pattern.iloc[0]

class C_MACD_Ending_Profit_Calculation(C_BestMACDPattern):
    def __init__(self):
        C_BestMACDPattern.__init__(self)

    def _MACD_ending_profits(self, period, stock_code):
        # Fetch out Signal, EMA_short_window, EMA_long_window, DIF_window, quote_time, MACD_pattern_number from tb_StockIndex_MACD_New into a Pandas DF
        # Fetch out pattern_list from DB into MACD_patterns
        conn = self._engine.connect()
        s = select([self._tb_TradeSignal.c.MACD_pattern_number]).distinct()
        MACD_patterns = conn.execute(s).fetchall()

        # Fetch out the last 555 closing prices records from DB
        sql_fetch_min_records = (
            "select * from (select stock_code, close_price, quote_time from tb_StockXMinRecords where period = %s and stock_code = %s order by quote_time DESC limit 555) as tbl order by quote_time ASC")
        sql_fetch_period_records = (
            "select * from (select stock_code, close_price, quote_time from tb_StockXPeriodRecords where period = %s and stock_code = %s order by quote_time DESC limit 555) as tbl order by quote_time ASC")

        # The follow part of code is to retrive all MACD trading signals into a DF to save the cost
        # of reading DB every time.
        sql_fetch_all_MACD_signals = ('select * from tb_StockIndex_MACD_New')
        df_all_signals = pd.read_sql(sql_fetch_all_MACD_signals, con=self._engine)

        if period == 'day' or period == 'week':
            sql_fetch_records = sql_fetch_period_records
        else:
            sql_fetch_records = sql_fetch_min_records

        df_stock_records = pd.read_sql(sql_fetch_records, con=self._engine, params=(period, stock_code),
                                       index_col='quote_time')
        self._clean_table('tb_MACD_Trades_HalfHour')
        self._clean_table('tb_MACD_Trades')
        self._multi_processors_cal_MACD_ending_profits(MACD_patterns, df_all_signals, df_stock_records, period)
        self._log_mesg = self._log_mesg + "Profit: MACD ending profit has been calculated at %s \n" % self._time_tag()
        # Selecting the best pattern and saving it into tbl_best_pattern
        pattern = self._save_MACD_best_pattern('m30')
        self._log_mesg = self._log_mesg + "Profit: MACD Pattern %s has the best performance at %s \n" % (
            pattern, self._time_tag())
        self._write_log(self._log_mesg)

    def _single_pattern_ending_profit_cal(self, stock_code, MACD_pattern, period):
        # Fetch out the last 555 closing prices records from DB
        sql_fetch_min_records = (
            "select * from (select stock_code, close_price, quote_time from tb_StockXMinRecords where period = %s and stock_code = %s order by quote_time DESC limit 555) as tbl order by quote_time ASC")
        sql_fetch_period_records = (
            "select * from (select stock_code, close_price, quote_time from tb_StockXPeriodRecords where period = %s and stock_code = %s order by quote_time DESC limit 555) as tbl order by quote_time ASC")
        if period == 'day' or period == 'week':
            sql_fetch_records = sql_fetch_period_records
        else:
            sql_fetch_records = sql_fetch_min_records

        df_stock_records = pd.read_sql(sql_fetch_records, con=self._engine, params=(period, stock_code),
                                       index_col='quote_time')

        # The follow part of code is to retrive all MACD trading signals into a DF to save the cost
        # of reading DB every time.
        sql_fetch_all_MACD_signals = ('select * from tb_StockIndex_MACD_New')
        df_all_signals = pd.read_sql(sql_fetch_all_MACD_signals, con=self._engine)

        self._clean_table('tb_MACD_Trades_HalfHour')
        self._clean_table('tb_MACD_Trades')
        self._MACD_ending_profits_calculation(df_stock_records, MACD_pattern, df_all_signals, period)
        self._log_mesg = self._log_mesg + "Profit: MACD ending profit has been calculated at %s \n" % self._time_tag()
        # Selecting the best pattern and saving it into tbl_best_pattern
        pattern = self._save_MACD_best_pattern('m30')
        self._log_mesg = self._log_mesg + "Profit: MACD Pattern %s has the best performance at %s \n" % (
            pattern, self._time_tag())
        self._write_log(self._log_mesg)

    def _multi_processors_cal_MACD_ending_profits(self, MACD_patterns, df_all_signals, df_stock_records, period):
        total_pattern = len(MACD_patterns)
        print total_pattern
        num_processor = self._processors
        index_beg = 0
        index_end = total_pattern / num_processor
        pattern_slices = []

        for i in range(num_processor + 1):
            if i != num_processor:
                print "i:%s,  index_beg %s , index_end %s " % (i, index_beg, index_end)
                pattern_slices.append(MACD_patterns[index_beg:index_end])
                index_beg = index_end
                index_end = index_end + total_pattern / num_processor


        processes = []
        for i in range(num_processor):
            p = mp.Process(target=self._MACD_ending_profits_calculation,
                           args=(df_stock_records, pattern_slices[i], df_all_signals, period,))
            processes.append(p)

        C_Algorithems_BestPattern._processes_pool(self, tasks=processes, processors=num_processor)

    def _MACD_ending_profits_calculation(self, df_stock_close_prices, pattern_slices, df_all_signals, period):
        # Fetch the lastest close_price and quote_time
        # print df_stock_close_prices.iloc[-1]
        # print "--------------------------------------------"
        #print df_stock_close_prices
        e_price = df_stock_close_prices.close_price[-1]
        e_quote_time = df_stock_close_prices.index[-1]
        e_stock_code = df_stock_close_prices.stock_code[-1]
        # Create an DF for storing transaction data
        MACD_trade_column_names = ['stock_code', 'quote_time', 'close_price', 'tradeVolume', 'tradeCost',
                                   'stockVolume_Current', 'cash_Current', 'totalValue_Current', 'profit_Rate',
                                   'EMA_short_window', 'EMA_long_window', 'DIF_window', 'MACD_pattern_number']
        #engine = create_engine('mysql+mysqldb://marshao:123@10.0.2.15/DB_StockDataBackTest')
        engine = self._engine


        widgets = ['MACD_Pattern_BackTest: ',
                   progressbar.Percentage(), ' ',
                   progressbar.Bar(marker='0', left='[', right=']'), ' ',
                   progressbar.ETA()]
        progress = progressbar.ProgressBar(widgets=widgets)

        loop_breaker = 0

        for eachPattern in progress(pattern_slices):
            # if loop_breaker==2:break
            # else: loop_breaker += 1
            stockVolume_Begin = self._calcu_config['stock_volume_begin']
            # cash_Begin = 120000.00
            cash_Begin = self._calcu_config['cash_begin']
            tradeVolume = self._trading_volume
            cash_Current = cash_Begin
            stockVolume_Current = stockVolume_Begin
            # Mulitple continue buying is allowed when up_limit > 2*tradeVolumn
            # stockVolume_up_limit = 6500
            stockVolume_up_limit = self._stock_inhand_uplimit
            totalValue_Begin = (stockVolume_Begin * df_stock_close_prices.close_price[0] + cash_Begin)
            # The signal order must be from the oldest to the newest
            pattern = eachPattern[0]
            # sql_fetch_signals = (
            # 'select * from tb_StockIndex_MACD_New where MACD_pattern_number = %s order by quote_time ASC')
            # df_MACD_signals = pd.read_sql(sql_fetch_signals, params=(pattern,), con=engine)

            df_MACD_signal_no_sort = df_all_signals.loc[df_all_signals['MACD_pattern_number'] == int(pattern)]
            df_MACD_signals = df_MACD_signal_no_sort.sort_values(by=['quote_time'], ascending=True)
            # print df_MACD_signals.EMA_long_window

            EMA_short_windows = df_MACD_signals.EMA_short_window.iloc[0]
            EMA_long_windows = df_MACD_signals.EMA_long_window.iloc[0]
            DIF_windows = df_MACD_signals.DIF_window.iloc[0]
            MACD_trades = pd.DataFrame(columns=MACD_trade_column_names)  # create DF to save transactions
            stock_code = df_stock_close_prices.stock_code.iloc[0]

            # print "\n ------------------------------------------------------"
            #print "\n Pattern number is : %s"%eachPattern
            for idx, row in df_MACD_signals.iterrows():
                '''
                The processing order must be from the oldest to the newest
                '''
                #print row
                close_price = df_stock_close_prices.loc[row.quote_time, 'close_price']
                # print close_price
                trade_cost = close_price * tradeVolume

                # Evaluate the trading signals and calculate the profits
                # if df_MACD_signals.Signal[i] == 1:  # Positive Signal, buy more stocks
                if row.Signal == 1:
                    if (cash_Current >= trade_cost) & (
                        (stockVolume_Current + tradeVolume) < stockVolume_up_limit):  # Have enough cash in hand
                        # tradeVolume = stockVolume_Current * tradePercent
                        stockVolume_Current = tradeVolume + stockVolume_Current
                        cash_Current = cash_Current - trade_cost
                        totalValue_Current = np.round((stockVolume_Current * close_price + cash_Current), 3)
                        profit_Rate = math.log(totalValue_Current / totalValue_Begin)

                        # Append HalfHour Profit Results into Array
                        transaction = [stock_code, row.quote_time, close_price,
                                       tradeVolume, trade_cost, stockVolume_Current,
                                       cash_Current, totalValue_Current,
                                       profit_Rate, EMA_short_windows,
                                       EMA_long_windows, DIF_windows, pattern]
                        MACD_trades.loc[len(MACD_trades)] = transaction
                        #print MACD_trades
                    else:
                        pass
                # elif df_MACD_signals.Signal[i] == (-1):  # NEgative Signal, sell stocks
                elif row.Signal == -1:
                    # if stockVolume_Current >= tradeVolume:  # Have enough stocks in hand
                    if stockVolume_Current != 0:  # Sale all stocks out
                        trade_cost = stockVolume_Current * close_price
                        # stockVolume_Current = stockVolume_Current - tradeVolume
                        tradeVolume = stockVolume_Current
                        stockVolume_Current = 0
                        cash_Current = cash_Current + trade_cost
                        totalValue_Current = np.round((stockVolume_Current * close_price + cash_Current), 3)
                        profit_Rate = math.log(totalValue_Current / totalValue_Begin)

                        # Append HalfHour Profit Results into Array
                        transaction = [stock_code, row.quote_time, close_price,
                                       tradeVolume, trade_cost, stockVolume_Current,
                                       cash_Current, totalValue_Current,
                                       profit_Rate, EMA_short_windows,
                                       EMA_long_windows, DIF_windows, pattern]
                        MACD_trades.loc[len(MACD_trades)] = transaction
                        #print MACD_trades
                    else:
                        pass
                else:
                    pass

            if MACD_trades.size != 0:
                # Calculate lastest profit rate with lastest close_price: e_price for each pattern
                stockVolume_Current = MACD_trades.iloc[-1].stockVolume_Current
                cash_Current = MACD_trades.iloc[-1].cash_Current
                totalValue_Current = np.round((stockVolume_Current * e_price + cash_Current), 3)
                profit_Rate = math.log(totalValue_Current / totalValue_Begin)
                transaction = [e_stock_code, e_quote_time, 0, 0, 0, 0, 0, totalValue_Current, profit_Rate, 0, 0, 0,
                               pattern]
                MACD_trades.loc[len(MACD_trades)] = transaction
                MACD_trades['profit_Rate'] = np.round(MACD_trades.profit_Rate, 5)
                #print MACD_trades
                MACD_trades['cash_Current'] = np.round(MACD_trades.cash_Current, 2)
                #print MACD_trades

                if period == 'm30':
                    #print MACD_trades
                    MACD_trades.to_sql('tb_MACD_Trades_HalfHour', con=engine, if_exists='append', index=False)
                else:
                    MACD_trades.to_sql('tb_MACD_Trades', con=engine, if_exists='append', index=False)
                    #MACD_trades.drop(MACD_trades.index, inplace=True)

class C_MACD_Signal_Calculation(C_BestMACDPattern):
    def __init__(self):
        C_BestMACDPattern.__init__(self)

    def _MACD_trading_signals(self, period, stock_code, quo=None, ga=None, beta=None):
        # use MACD_Pattern_lists to calculate all trading signals of all patterns
        # Calculate signals for 30min data
        if quo is None:
            quo = 0.7
        if ga is None:
            ga = 0.3
        if beta is None:
            beta = 0.2
        df_MACD_index = pd.read_sql('tb_MACDIndex', con=self._engine, index_col='id_tb_MACDIndex')
        # self._MACD_signal_calculation_cross(df_MACD_index, df_stock_records)
        self._clean_table('tb_StockIndex_MACD_New')
        self._multi_processors_cal_MACD_signals(df_MACD_index, stock_code, period, quo, ga, beta)
        # print df_MACD_index, df_stock_records, df_stock_records.index[0].date()

    def _multi_processors_cal_MACD_signals(self, df_MACD_index, stock_code, period, quo=None, ga=None, beta=None):
        if quo is None:
            quo = 0.7
        if ga is None:
            ga = 0.3
        if beta is None:
            beta = 0.2
        print "Jumped into Multiprocessing "
        # Initialize Signal Calculation
        C_MACD_Signal_Calculator = C_MACD_Signal_Calculation()
        # print period
        sql_fetch_min_records = (
            # "select * from tb_StockXMinRecords where period = %s and stock_code = %s order by quote_time DESC limit 550")
            "select * from (select * from tb_StockXMinRecords where period = %s and stock_code = %s order by quote_time DESC limit 555) as tbl order by quote_time ASC")
        sql_fetch_period_records = (
            "select * from tb_StockXPeriodRecords where period = %s and stock_code = %s")
        if period == 'day' or period == 'week':
            sql_fetch_records = sql_fetch_period_records
        else:
            sql_fetch_records = sql_fetch_min_records

        num_processor = self._processors
        MACD_pattern_size = df_MACD_index.index.size
        tasks = MACD_pattern_size / (num_processor - 1)
        print "In total: there are %s MACD Patterns" % MACD_pattern_size
        task_args = []
        p = 1
        index_begin = 0
        index_end = tasks
        while p <= num_processor:
            print "For %s part, index begin is %s, index end is %s" % (p, index_begin, index_end)
            df_index = df_MACD_index[index_begin:index_end]
            df_stock_records = pd.read_sql(sql_fetch_records, con=self._engine, index_col='quote_time',
                                           params=(period, stock_code))
            task_args.append((df_index, df_stock_records), )

            p += 1
            index_begin = index_end
            if p != num_processor:
                index_end = p * tasks
            else:
                index_end = MACD_pattern_size - 1
        # print task_args
        processes = []
        # print task_args.count(1)
        for_real = False
        to_DB = True
        for i in range(num_processor):
            # print "This is the %s th job"%i
            # Must be really careful of which calculation methond is in used.
            p = mp.Process(target=self._MACD_signal_calculation_M30_3,
                           args=(task_args[i][0], task_args[i][1], to_DB, for_real, quo, ga, beta))
            processes.append(p)

        C_Algorithems_BestPattern._processes_pool(self, tasks=processes, processors=num_processor)
        self._log_mesg = self._log_mesg + "-----------------------------------------------------\n"
        self._log_mesg = self._log_mesg + "Signal: MuiltiProcess MACD Signal Calculation with Method MACD_Signal_Calculation_MACD has been called at %s \n" % self._time_tag()
        self._write_log(self._log_mesg)

    def _single_pattern_signal_cal(self, MACD_pattern, period, stock_code, quo, ga, beta, simplified=True):
        self._clean_table('tb_StockIndex_MACD_New')

        sql_fetch_min_records = (
            # "select * from tb_StockXMinRecords where period = %s and stock_code = %s order by quote_time DESC limit 550")
            "select * from (select * from tb_StockXMinRecords where period = %s and stock_code = %s order by quote_time DESC limit 555) as tbl order by quote_time ASC")
        sql_fetch_period_records = (
            "select * from tb_StockXPeriodRecords where period = %s and stock_code = %s")
        if period == 'day' or period == 'week':
            sql_fetch_records = sql_fetch_period_records
        else:
            sql_fetch_records = sql_fetch_min_records
        df_stock_records = pd.read_sql(sql_fetch_records, con=self._engine, index_col='quote_time',
                                       params=(period, stock_code))

        sql_fetch_MACD_index = ("select * from tb_MACDIndex where id_tb_MACDIndex=%s")
        df_MACD_index = pd.read_sql(sql_fetch_MACD_index, con=self._engine, index_col='id_tb_MACDIndex', params=MACD_pattern)

        df_signals = self._MACD_signal_calculation_M30_3(df_MACD_index, df_stock_records, True, False, quo, ga, beta,
                                                         simplified)

        self._log_mesg = self._log_mesg + "-----------------------------------------------------\n"
        self._log_mesg = self._log_mesg + "Signal: Single MACD Pattern Signal Calculation with Method MACD_Signal_Calculation_MACD has been called at %s \n" % self._time_tag()
        if simplified:
            self._write_log(self._log_mesg)
        return df_signals

    def _MACD_signal_calculation_M30_3(self, df_MACD_index, df_stock_records, to_DB=True, for_real=False, quo=None,
                                       ga=None, beta=None, simplified=True):
        '''
        Calculate trading signals for stock operation.
        :param df_MACD_index:
        :param df_stock_records:
        :param to_DB:
        :param for_real: This is a production operation when = True
        :return:
        '''
        #print "Processing MACD Index"
        # print "------------------------------------------------------ \n"
        if quo is None:
            quo = 0.7
        if ga is None:
            ga = 0.3
        if beta is None:
            beta = 0.2
        widgets = ['MACD_Pattern_Trading Singal Calculation: ',
                   progressbar.Percentage(), ' ',
                   progressbar.Bar(marker='0', left='[', right=']'), ' ',
                   progressbar.ETA()]
        progress = progressbar.ProgressBar(widgets=widgets)

        # alpha is parameter for MACD(t) when buying stock with different sign
        alpha = [3.0, 2.0, 1.0]
        # beta is patameter for MACD(t) when buying stock with nagitive shink
        betaa = [0.1, 0.2, 0.3, 0.4, 0.5]
        # gama is parameter to MACD(MAX-P) when saleing stock
        gama = [0.8, 0.65, 0.5, 0.4, 0.45, 0.3]

        # loop breaker
        loop_breaker = 0
        df = df_stock_records
        #print df
        #  The first loop, go through every MACD Pattern in df_MACD_index

        for pattern_idx, pattern_row in progress(df_MACD_index.iterrows(), max_value=df_MACD_index.shape[0]):
            # for j in progress(range(df_MACD_index.index.size)):

            #if loop_breaker == 1:
            #    break
            #loop_breaker += 1


            short_window = pattern_row.EMA_short_window
            long_window = pattern_row.EMA_long_window
            dif_window = pattern_row.DIF_window
            MACD_Pattern_Number = pattern_idx
            #print "Pattern %s is under processing "%MACD_Pattern_Number

            df['EMA_short'] = np.round(pd.ewma(df.close_price, span=short_window), 3)
            df['EMA_long'] = np.round(pd.ewma(df.close_price, span=long_window), 3)
            df['DIF'] = np.round((df.EMA_short - df.EMA_long),3)
            df['DEA'] = np.round(pd.rolling_mean(df.DIF, window=dif_window), 3)
            df['MACD'] = np.round((2.0 * (df.DIF - df.DEA)),3)
            df['Signal'] = 0
            df['EMA_short_window'] = short_window
            df['EMA_long_window'] = long_window
            df['DIF_window'] = dif_window
            df['MACD_Pattern_Number'] = MACD_Pattern_Number
            df['Reason']=''

            index_date = df.index[0].date()
            tradable = True
            re_buy = False  # Evalution sign for the non-significant buy
            MACD_down = True  # Set stock is not in the down going tunnel
            #MACD Max value in the up going trend
            MACD_MAX_P = 0
            # MACD Average value calculation
            MACD_AVG = self._Cal_MACD_AVG(df.MACD, quo=quo)
            # This is the signal to switch the Agressive buy on or off
            # aggresive_buy = True
            aggresive_buy = False
            #Iter rows of stock records
            row_set = df.iterrows()
            last_idx, last = row_set.next()
            # print df
            #print "last is %s"%last_idx

            for idx, row in row_set:
                # Valid the T+1 policy and check whether there is re_sale signal
                if index_date != idx.date():
                    tradable = True
                    index_date = idx.date()

                # Buy1: MACD Different Sign
                if (row.MACD > 0) & (last.MACD < 0):
                    if row.MACD >= MACD_AVG:  # MACD value is significant
                        df.set_value(idx, 'Signal', 1)
                        MACD_down = False  # Set stock in up trend
                        tradable = False  # Stop saling stock in same day
                        MACD_MAX_P = row.MACD
                        df.set_value(idx, 'Reason', 'DSB quo:%s, ga:%s' % (quo, ga))
                    else:  # if the MACD is not significant
                        re_buy = True
                        df.set_value(idx, 'Reason', 'DSB_AR quo:%s, ga:%s' % (quo, ga))
                # Buy2: MACD Different Sign with unsignificant signal
                elif re_buy & (row.MACD > alpha[0] * last.MACD):
                    df.set_value(idx, 'Signal', 1)
                    MACD_down = False  # Set stock in up trend
                    tradable = False  # Stop saling stock in same day
                    MACD_MAX_P = row.MACD
                    re_buy = False
                    df.set_value(idx, 'Reason', 'ReBuy quo:%s, ga:%s' % (quo, ga))
                # Buy3: Aggresive buying policy, MACD nagative Shink
                elif aggresive_buy & (row.MACD < 0) & (last.MACD < 0):
                    if ((row.MACD - last.MACD)/abs(last.MACD)) >= beta:
                        df.set_value(idx, 'Signal', 1)
                        MACD_down = False  # Set stock in up trend
                        tradable = False  # Stop saling stock in same day
                        MACD_MAX_P = row.MACD
                        df.set_value(idx, 'Reason', 'A_Buy quo:%s, ga:%s' % (quo, ga))
                # Sale: Sale Policy, MACD Positive Shrink
                elif (row.MACD < ga * MACD_MAX_P) & (not MACD_down):
                    if tradable:
                        df.set_value(idx, 'Signal', -1)
                        MACD_MAX_P = 0
                        df.set_value(idx, 'Reason', 'MS_Sale quo:%s, ga:%s' % (quo, ga))
                    else:
                        # Assign re-sale signal -5 to signal value
                        df.set_value(idx, 'Signal', -9)
                        df.set_value(idx, 'Reason', 'MS_RSSign quo:%s, ga:%s' % (quo, ga))
                    MACD_down = True  # Setting Stock is in down trend
                else:  # Valid whether the stock is still in downgoing tunnel
                    # update MACD_MAX_P and MACD_MIN_P
                    if (not MACD_down) & (row.MACD > MACD_MAX_P): MACD_MAX_P = row.MACD

                    # Cancel Rebuy
                    if re_buy: re_buy = False

                    # In the next day, if signal is not in buy or sale, then valid resale
                    if tradable & MACD_down & (idx.hour == 10) & (idx.minute == 0):
                        # Need to see whether the day is Monday to decide how many days to go back
                        if idx.weekday() == 0:
                            pre_idx_open = idx - datetime.timedelta(days=1)
                        else:
                            pre_idx_open = idx - datetime.timedelta(days=1)
                        pre_idx_close = idx - datetime.timedelta(hours=1)
                        df_day = df.loc[pre_idx_open:pre_idx_close, ('Signal', 'MACD')]
                        if self._Cal_Resalble(df_day, idx):
                            df.set_value(idx, 'Signal', -1)
                            # print df.loc[idx]
                            MACD_down = True  # Setting Stock is in down trend
                            MACD_MAX_P = 0
                            df.set_value(idx, 'Reason', 'ReS quo:%s, ga:%s' % (quo, ga))
                last = row
                # last_idx = idxdf_save.EMA_short[-1] = 999
            # Remove the no transaction record from the DB.
            #engine = create_engine('mysql+mysqldb://marshao:123@10.0.2.15/DB_StockDataBackTest')
            engine = self._engine
            # print df[df.Signal == -1].count()
            if simplified:
                df_save = df[df.Signal != 0].drop(df.columns[[0, 2, 3, 4, 5, 7]], axis=1)
                df_save = df_save[df_save.Signal != -9]
            else:
                df_save = df.drop(df.columns[[0, 2, 3, 4, 5, 7]], axis=1)

            if to_DB:  # to_DB == True if function call from data saving, to_DB ==False function call from apply, not need to save to db
                if for_real:
                    # df_save.iloc[0:1, lambda df:['EMA_short']] = 999
                    # df_save.EMA_short[-1] = 999
                    pass
                #print df_save
                df_save.to_sql('tb_StockIndex_MACD_New', con=engine, if_exists='append', index=True)
                engine.dispose()
                # df.to_csv('/home/marshao/DataMiningProjects/Output/df1.csv')
        return df

    def _MACD_signal_calculation_cross(self, df_MACD_index, df_stock_records, to_DB=True, for_real=False):
        '''
        Calculate trading signals for stock operation.
        :param df_MACD_index:
        :param df_stock_records:
        :param to_DB:
        :param for_real: This is a production operation when = True
        :return:
        '''
        print "Processing MACD Index"
        widgets = ['MACD_Pattern_BackTest: ',
                   progressbar.Percentage(), ' ',
                   progressbar.Bar(marker='0', left='[', right=']'), ' ',
                   progressbar.ETA()]
        progress = progressbar.ProgressBar(widgets=widgets)

        # loop breaker
        loop_breaker = 0
        df = df_stock_records
        # print df
        #  The first loop, go through every MACD Pattern in df_MACD_index
        for j in progress(range(df_MACD_index.index.size)):
            # if loop_breaker > 1:
            #    break
            # loop_breaker += 1

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

            row_set = df.iterrows()
            last_idx, last = row_set.next()

            for idx, row in row_set:
                if index_date != idx.date():
                    tradable = True
                    index_date = idx.date()

                if (row.DIF > row.DEA) & (last.DIF <= last.DEA) & tradable:
                    df.set_value(idx, 'Signal', 1)
                    tradable = False
                elif (row.DIF < row.DEA) & (last.DIF >= last.DEA) & tradable:
                    df.set_value(idx, 'Signal', -1)
                else:
                    pass

                # print "Last index Date is %s"%last_idx
                # print "Row index date is %s"%idx
                # print "Row signal is %s at date %s" % (df.ix[idx].Signal, idx)
                last = row
                last_idx = idx

            # Remove the no transaction record from the DB.
            #engine = create_engine('mysql+mysqldb://marshao:123@10.0.2.15/DB_StockDataBackTest')
            engine = self._engine
            df_save = df[df.Signal != 0].drop(df.columns[[0, 2, 3, 4, 5, 7]], axis=1)
            # df_save = df.drop(df.columns[[0, 2, 3, 4, 5, 7]], axis=1)
            if to_DB:  # to_DB == True if function call from data saving, to_DB ==False function call from apply, not need to save to db
                if for_real:
                    df_save.EMA_short[0] = 999
                df_save.to_sql('tb_StockIndex_MACD_New', con=engine, if_exists='append', index=True)
        return df

    def _MACD_signal_calculation_MACD(self, df_MACD_index, df_stock_records, to_DB=True):
        print "Processing MACD Index"
        widgets = ['MACD_Pattern_BackTest: ',
                   progressbar.Percentage(), ' ',
                   progressbar.Bar(marker='0', left='[', right=']'), ' ',
                   progressbar.ETA()]
        progress = progressbar.ProgressBar(widgets=widgets)

        #engine = create_engine('mysql+mysqldb://marshao:123@10.0.2.15/DB_StockDataBackTest')
        engine = self._engine

        # loop breaker
        loop_breaker = 0
        df = df_stock_records
        frames = []
        # print df
        #  The first loop, go through every MACD Pattern in df_MACD_index
        for j in progress(range(df_MACD_index.index.size)):
            # if loop_breaker > 2:
            #    break
            # loop_breaker += 1

            short_window = df_MACD_index.EMA_short_window[df_MACD_index.index[j]]
            long_window = df_MACD_index.EMA_long_window[df_MACD_index.index[j]]
            dif_window = df_MACD_index.DIF_window[df_MACD_index.index[j]]
            MACD_Pattern_Number = df_MACD_index.index[j]

            num_records = len(df.index)
            df['EMA_short'] = pd.ewma(df.close_price, span=short_window)
            df['EMA_long'] = pd.ewma(df.close_price, span=long_window)
            df['DIF'] = df.EMA_short - df.EMA_long
            DIF = df.DIF.tolist()
            df['DEA'] = pd.rolling_mean(df.DIF, window=dif_window)
            DEA = df.DEA.tolist()
            df['MACD'] = 2.0 * (df.DIF - df.DEA)
            # df['MACD'] = 0
            MACD = df.MACD.tolist()
            df['Signal'] = 0
            df['EMA_short_window'] = short_window
            df['EMA_long_window'] = long_window
            df['DIF_window'] = dif_window
            df['MACD_Pattern_Number'] = MACD_Pattern_Number
            # print df
            index_date = df.index[0].date()
            # print "The first index_date is %s"%index_date
            tradable = True
            re_sale = False
            saleble = True

            i = 0

            while i < num_records - 1:
                # if i > 1:
                #    break
                # loop_breaker += 1
                # for i < num_records:

                # Since T+1 policy, There trade should be disabled if there is a buy happend in each trading day.
                # If the index_date is not equal to record date, means diffrent day, set the tradable = True
                # if i > 0:
                i += 1
                # print "The second index date is %s"%df.index[i].date()
                if index_date != df.index[i].date():
                    tradable = True
                    index_date = df.index[i].date()
                    another_day = True
                    if re_sale:
                        df.Signal.iloc[i] = -1
                        re_sale = False
                        # print "The third index date is %s"%df.index[i].date()
                # Action:(Buy1)
                if ((DIF[i] > DEA[i]) & (DIF[i - 1] <= DEA[i - 1])):
                    if tradable:
                        df.Signal.iloc[i] = 1
                        tradable = False
                        saleble = True
                    continue
                # Action: (Buy2)
                elif ((MACD[i] > MACD[i - 1]) & (DIF[i] > DEA[i])):
                    if tradable:
                        df.Signal.iloc[i] = 1
                        tradable = False
                        saleble = True
                    continue
                # Action:(Sale)
                elif (MACD[i] < MACD[i - 1]) & saleble:
                    if tradable:
                        df.Signal.iloc[i] = -1
                        saleble = False
                    else:
                        # If the sales cannot be done in day i because of the T+1 policy,
                        # then it must be sale in the day i+1, unless the day i+1 has another Buy command
                        re_sale = True
                        saleble = False

            # Remove the no transaction record from the DB.
            df_save = df[df.Signal != 0].drop(df.columns[[0, 2, 3, 4, 5, 7]], axis=1)
            # df_save = df.drop(df.columns[[0, 2, 3, 4, 5, 7]], axis=1)
            frames.append(df_save)
        df_save = pd.concat(frames)

        # print type(df_save)
        # print df_save

        if to_DB:  # to_DB == True if function call from data saving, to_DB ==False function call from apply, not need to save to db
            df_save.to_sql('tb_StockIndex_MACD_New', con=engine, if_exists='append', index=True, chunksize=3000)

    def _Cal_MACD_AVG(self, MACD_se, quo=None, theta=None, step=None):
        '''
        This function is calculate the MACD_AVG using MACD Series. MACD_AVG should filter out the smallest quo percent
        of records, make those records as useless records
        :param MACD_se: MACD Series
        :param quo: Portion of percentage should be kept
        :param theta: Inital excelarator of MACD
        :param step: Inital Step of excelarator's increasement
        :return:
        '''
        # The portion above the average.
        if quo is None:
            quo = 0.9
        # theta is the paramenter to sacle the MACD Average value
        if theta is None:
            theta = 0.5
        # Step to change the theta value
        if step is None:
            step = 0.02
        # MACD Average value
        MACD_AVG = theta * (round(np.sum(np.abs(MACD_se)) / MACD_se.count(), 5))
        # Percentage of number of MACD records above the MACD Average
        MACD_AVG_percent = MACD_se[MACD_se > MACD_AVG].count() / round(MACD_se.count(), 2)


        if MACD_AVG_percent < quo:
            # print "Quotion is %s"%quo
            while (MACD_AVG_percent < quo) and (theta - step > 0):
                theta -= step
                MACD_AVG = theta * (round(np.sum(np.abs(MACD_se)) / MACD_se.size, 5))
                MACD_AVG_percent = MACD_se[np.abs(MACD_se) > MACD_AVG].count() / float(MACD_se.count())
                # print "MACD above avg is %s and total is %s" % (MACD_se[MACD_se > MACD_AVG].count(), MACD_se.count())
                # print "MACD_AVG is %s and MACD_AVG_Percent is %s"%(MACD_AVG, MACD_AVG_percent)
            return MACD_AVG
        else:
            while (MACD_AVG_percent > quo):
                theta += step
                MACD_AVG = theta * (round(np.sum(np.abs(MACD_se)) / MACD_se.size, 5))
                MACD_AVG_percent = MACD_se[np.abs(MACD_se) > MACD_AVG].count() / float(MACD_se.count())
                # print "MACD above avg is %s and total is %s" % (MACD_se[MACD_se > MACD_AVG].count(), MACD_se.count())
                # print "MACD_AVG is %s and MACD_AVG_Percent is %s" % (MACD_AVG, MACD_AVG_percent)
            return MACD_AVG

    def _Cal_Resalble(self, df, idx):
        '''
        This is the function to evaluate whether the resale should be contact or not
        This is only valid for M30 data
        :param :
        :return True/False:
        '''
        resaleble = False
        for i, r in df.iterrows():
            if r.Signal == -9:
                # df.set_value(idx, 'Signal', -1)
                resaleble = True
            elif r.Signal == 1 or r.Signal == -1:
                resaleble = False
        # resaleble = False
        return  resaleble

class C_BestSARPattern(C_Algorithems_BestPattern):
    def __init__(self):
        C_Algorithems_BestPattern.__init__(self)
        self._ep = 0
        self._previous_SAR = 0
        self._price_high = 0
        self._price_low = 0
        self._AF = [0.01, 0.015, 0.02, 0.025, 0.03]
        self._AF_limit = [0.10, 0.15, 0.20, 0.25, 0.30]
        self._records_window = [5, 10]
        # matplotlib.style.use('ggplot')

    def SAR_patterns_exams(self, stock_code='sz300226', period='m30'):
        processes = []
        args = []
        self._clean_table('tb_StockIndex_SAR')
        i = 1
        df_records = self._getting_stock_records(stock_code, period)
        for af in self._AF:
            for af_limit in self._AF_limit:
                for window in self._records_window:
                    df = pd.DataFrame(df_records.values.copy(), df_records.index.copy(), df_records.columns.copy())
                    p = mp.Process(target=self.SAR_calculation, args=(df, i, stock_code, period, window, af, af_limit,))
                    processes.append(p)
                    i += 1
        # Send task to a self made process pool.
        C_Algorithems_BestPattern._processes_pool(self, tasks=processes, processors=5)

    def SAR_calculation(self, df_records, pattern_number, stock_code='sz300226', period='m30', records_window=5,
                        af=0.02, af_limit=0.20):

        trend, sar, ep = self._begining_trend(df_records, records_window)
        af_begin = af
        af_window = af
        df_records['SAR'] = sar
        df_records['EP'] = ep
        df_records['AF_value'] = af
        df_records['AF_limit'] = af_limit
        df_records['AF_window'] = af
        df_records['EPSAR'] = ep - sar
        df_records['trading_signal'] = 0
        df_records['pattern_number'] = pattern_number
        df_records['ending_profit'] = 0
        df_records['status'] = 1
        df_records['record_window'] = records_window
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
                    df_records.trading_signal[i] = -1
                    af = 0
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
                    df_records.trading_signal[i] = 1
                    af = 0
                i += 1
        df_records.to_sql('tb_StockIndex_SAR', con=self._engine, if_exists='append', index=False)

    def _getting_stock_records(self, stock_code='sz300226', period='m30'):
        if period == 'm30':
            sql_read_records = (
                "select stock_code, quote_time, high_price, low_price, close_price from tb_StockXMinRecords where stock_code = %s and period = %s")
        elif period == 'day':
            sql_read_records = (
                "select stock_code, quote_time, high_price, low_price, close_price from tb_StockXPeriodRecords where stock_code = %s and period = %s")
        else:
            sql_read_records = (
                "select stock_code, quote_time, high_price, low_price, close_price from tb_StockXMinRecords where stock_code = %s and period = %s")

        df_records = pd.read_sql(sql_read_records, con=self._engine, params=(stock_code, period))
        # print df_records
        return df_records

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

    def SAR_ending_profits_all_stocks(self):
        pass

    def SAR_ending_profit_all_patterns(self, stock_code):

        # Trading Rules:
        # 1: Buy or sales must have enough cash or stock
        # 3: Base on T+1 policy, buy + sale in one day is not allowed, sale + buy is valid.

        sql_select_SAR_trading_signals = (
            "select * from tb_StockIndex_SAR where stock_code = %s")
        df_ending_profit = pd.read_sql(sql_select_SAR_trading_signals, con=self._engine, params=(stock_code,))

        df_ending_profit['remove'] = False
        df_ending_profit['status'] = 2  # Status 2 means all signals have been examed and filtered
        df_patterns = df_ending_profit['pattern_number'].drop_duplicates()
        df_ending_profit.set_index('quote_time', inplace=True)
        df_grouped = df_ending_profit.groupby(by='pattern_number')
        # df_grouped.apply(self._trade_policy_T1)
        # print "singal pattern testing"
        i = 1
        while i <= len(df_patterns.index):
            df = df_ending_profit[df_ending_profit.pattern_number == i]
            print "pattern %s has %s records" % (i, len(df.index))
            df2 = df[df.trading_signal != 0]
            print "pattern %s has %s trades" % (i, len(df2.index))
            if len(df.index) != 0 and len(df2.index):
                self._trade_policy_T1(df)
            i += 1

    def _trade_policy_T1(self, df):
        e_price = df.close_price.iloc[-1]  # The last price of the selected period
        e_quote_time = df.index[-1]  # Get the last quote_time, the DF use the quote_time as index automatically
        df = df[df.trading_signal != 0]
        s_price = df.close_price.iloc[0]  # The start price of the selected period
        j = 1
        while j < len(df.index):
            last_day = df.index[j - 1].date()
            this_day = df.index[j].date()
            last_signal = df.trading_signal.iloc[j - 1]
            this_signal = df.trading_signal.iloc[j]
            if last_signal == 1 and this_signal == -1:
                if this_day == last_day:
                    index = df.index[j]
                    df.remove[index] = True
                    # print "set True at %s" % df.index[j]
            j += 1
        df = df[df.remove != True]
        # print "DF After T1 Policy"
        # file = self._SAR_log + '2.csv'
        # df.to_csv(path_or_buf=file)"
        self._ending_profit_for_singal_pattern(df, s_price, e_price, e_quote_time)
        # print "ending profit calculation finsihed."

    def _ending_profit_for_singal_pattern(self, df, s_price, e_price, e_quote_time):
        stock_volume_begin = 3000
        cash_begin = 200000.00
        trade_volume = 3000
        cash_inhand = cash_begin
        stock_volume_inhand = stock_volume_begin
        start_total_value = stock_volume_inhand * s_price + cash_inhand
        df['current_total_value'] = 0
        df['trade_cost'] = 0
        df['stock_volume_inhand'] = 0
        df['cash_inhand'] = 0
        for i in range(len(df.index)):
            current_price = df.close_price.iloc[i]
            if df.trading_signal.iloc[i] == 1:
                # buying stock
                trade_cost = trade_volume * current_price
                if cash_inhand >= trade_cost:
                    cash_inhand = cash_inhand - trade_cost
                    stock_volume_inhand = stock_volume_inhand + trade_volume
                    current_total_value = stock_volume_inhand * current_price + cash_inhand
                    df.ending_profit.iloc[i] = math.log(current_total_value / start_total_value)
                    df.trade_cost.iloc[i] = trade_cost
                    df.cash_inhand.iloc[i] = cash_inhand
                    df.stock_volume_inhand.iloc[i] = stock_volume_inhand
                    df.current_total_value.iloc[i] = current_total_value
                    df.status.iloc[i] = 3
                    # print "trade cost is %s, cash_inhand is %s, buy stocks" % (trade_cost, cash_inhand)
                else:
                    df.status.iloc[i] = 3
                    df.remove.iloc[i] = True
                    # print "No Money::   trade cost is %s, cash_inhand is %s," % (trade_cost, cash_inhand)
            else:
                trade_cost = stock_volume_inhand * current_price
                if stock_volume_inhand != 0:
                    cash_inhand = cash_inhand + trade_cost
                    stock_volume_inhand = 0
                    current_total_value = stock_volume_inhand * current_price + cash_inhand
                    df.ending_profit.iloc[i] = math.log(current_total_value / start_total_value)
                    df.status.iloc[i] = 3
                    df.trade_cost.iloc[i] = trade_cost
                    df.cash_inhand.iloc[i] = cash_inhand
                    df.stock_volume_inhand.iloc[i] = stock_volume_inhand
                    df.current_total_value.iloc[i] = current_total_value
                else:
                    df.status.iloc[i] = 3
                    df.remove.iloc[i] = True

        # print df
        file = self._SAR_log + '4.csv'
        df = df[df.remove != True]
        df.to_csv(path_or_buf=file)
        df.drop(['remove', 'id_tb_StockIndex_SAR', 'trade_cost', 'cash_inhand', 'stock_volume_inhand',
                 'current_total_value'], axis=1, inplace=True)

        # Calculate current profit rate base on each pattern, and save it into DB with status = 4
        current_total_value = stock_volume_inhand * e_price + cash_inhand
        ending_profit = math.log(current_total_value / start_total_value)
        # print "Pattern %s current total value is %s"%(df.pattern_number.iloc[0], current_total_value)

        df.reset_index(level=0, inplace=True)
        ls = [e_quote_time, df.stock_code.iloc[-1], 0, 0, 0, 0, 0, 0, df.AF_value.iloc[-1], df.AF_limit.iloc[-1],
              df.AF_window.iloc[-1], 0, df.pattern_number.iloc[-1], 4,
              ending_profit, df.record_window.iloc[-1]]
        df.loc[len(df)] = ls
        df.to_sql('tb_StockIndex_SAR', con=self._engine, if_exists='append', index=False)
        print "Saved pattern %s" % df.pattern_number.iloc[-1]

    def _sending_signal(self):
        pass



def main():
    # SARPattern = C_BestSARPattern()
    # SARPattern.SAR_patterns_exams(period='day')
    # SARPattern.SAR_ending_profit_all_patterns('sz300226')


    MACDPattern = C_BestMACDPattern()
    #MACD_Trading_Signal_Cal = C_MACD_Signal_Calculation()
    #MACD_Ending_Profit_Cal = C_MACD_Ending_Profit_Calculation()
    #MACD_Trading_Signal_Cal._MACD_trading_signals(period="m30", stock_code="sz002310", quo=0.7, ga=0.3)
    #MACD_Ending_Profit_Cal._MACD_ending_profits(period='m30', stock_code='sz002310')
    # MACDPattern._save_MACD_best_pattern(period='m30')
    # MACDPattern._get_best_pattern('sz002310')
    #MACDPattern.apply_best_MACD_pattern_to_data(period='m30', stock_code='sz002310', quo=0.7, ga=0.3, beta=0.2)
    #commu('1')
    #cal_specific_pattern()
    # MACDPattern._get_best_pattern('sz002310')
    caL_all_pattern()

def caL_all_pattern():
    # gama is parameter to MACD(MAX-P) when saleing stock
    # Using loops to find out the best MACD parameters.
    MACDPattern = C_BestMACDPattern()
    MACD_Trading_Signal_Cal = C_MACD_Signal_Calculation()
    MACD_Ending_Profit_Cal = C_MACD_Ending_Profit_Calculation()
    #gama = [0.8, 0.7, 0.65, 0.6, 0.4, 0.45, 0.3]
    quo = [0.5, 0.6, 0.7, 0.75, 0.8, 0.9]
    # beta = [0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]
    beta = [0.2]
    # quo = [0.7]
    gama = [0.3]
    for each_quo in quo:
        for each_ga in gama:
            for each_beta in beta:
                MACD_Trading_Signal_Cal._MACD_trading_signals(period="m30", stock_code="sz002310", quo=each_quo,
                                                              ga=each_ga, beta=each_beta)
                MACD_Ending_Profit_Cal._MACD_ending_profits(period='m30', stock_code='sz002310')
                #MACDPattern._save_MACD_best_pattern(period='m30')
                MACDPattern._get_best_pattern('sz002310')


def cal_specific_pattern():
    MACDPattern = C_BestMACDPattern()
    MACD_Trading_Signal_Cal = C_MACD_Signal_Calculation()
    MACD_Ending_Profit_Cal = C_MACD_Ending_Profit_Calculation()
    gama = [0.3]
    quo = [0.7]
    #beta = [0.1, 0.15, 0.2, 0.25,  0.3, 0.35, 0.4, 0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9]
    beta = [0.2]
    pattern_signal = ["2115", ]
    pattern_profit = [["2115"]]
    for each_quo in quo:
        for each_ga in gama:
            for each_beta in beta:
                MACD_Trading_Signal_Cal._single_pattern_signal_cal(MACD_pattern=pattern_signal, period="m30",
                                                                   stock_code="sz002310", quo=each_quo, ga=each_ga,
                                                                   beta=each_beta)
                MACD_Ending_Profit_Cal._single_pattern_ending_profit_cal(MACD_pattern=pattern_profit, period='m30',
                                                                         stock_code='sz002310')
                # MACDPattern._save_MACD_best_pattern(period='m30')
                MACDPattern._get_best_pattern('sz002310')


if __name__ == '__main__':
    main()
    # cProfile.run('main()', filename="my.profile")
    #import pstats

    # p = pstats.Stats("my.profile")
    #p.strip_dirs().sort_stats("time").print_stats(15)
