#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import pandas as pd
import csv
from sqlalchemy import Table

from C_Algorithms_BestMACDPattern import C_Algorithems_BestPattern, C_MACD_Ending_Profit_Calculation, \
    C_MACD_Signal_Calculation


class C_Algorithems_BestPattern_pool_validation(C_Algorithems_BestPattern):
    '''
    Change: 2017-06-22 in _send_trading_command: modified the received signal to 1 and -1 to match the signal calculation result.
            2017-06-22 in _MACD_Singal_calcualtion_cross use MACD_short = 999 to represent real life stock operation and save them in DB.
    '''

    def __init__(self):
        C_Algorithems_BestPattern.__init__(self)

    def _send_trading_command_pool(self, stock_pool, asset_pool, stock_code, current_price, cash_avaliable, signal,
                                   pattern_number,
                                   period):
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
        # stock_code = df_stock_infor.stockCode[0]
        # stock_Remain = df_stock_infor.stockRemain[0]
        stock_Remain = stock_pool[stock_code][1]
        # stock_avaliable = df_stock_infor.stockAvaliable[0]
        stock_avaliable = stock_pool[stock_code][0]
        trade_volumn = 0
        volumn_up_limit = self._stock_inhand_uplimit
        # volumn_down_limit = 500
        oneshoot = self._trading_volume
        trade_algorithem_name = 'MACD Best Pattern'
        trade_algorithem_method = pattern_number
        done = False
        line = []
        trade_result = 0
        cmd = '5'

        if signal == 1:  # 1 == buy,
            print "Buying Stock %s" % stock_code
            trade_volumn = int(cash_avaliable / current_price / 100) * 100

            if (stock_Remain + oneshoot) < volumn_up_limit:
                if trade_volumn >= oneshoot:
                    trade_volumn = oneshoot
                else:
                    trade_volumn = trade_volumn

                # Calculate Cash Left
                # updated_cash_avaliable = cash_avaliable - (trade_volumn*current_price)
                print current_price
                asset_pool['cash_avaliable'] -= (trade_volumn * current_price)
                asset_pool['total_stock_value'] += (trade_volumn * current_price)
                stock_pool[stock_code][0] += trade_volumn
                stock_pool[stock_code][1] += trade_volumn
                stock_pool[stock_code][2] += trade_volumn * current_price
                '''
                cmd = '2 ' + stock_code + ' ' + trade_volumn + ' ' + str(current_price)
                self._log_mesg = self._log_mesg + "     Stock %s has the amount %s in hand at %s \n" % (
                    stock_code, stock_Remain, self._time_tag())
                '''
            else:
                self._log_mesg = self._log_mesg + "     Cannot buy more stock %s because " \
                                                  "the amount reach the up limit at %s \n" % (
                                                      stock_code, self._time_tag())
            df_trade_history.trade_type.loc[0] = int(signal)
        elif signal == -1:  # -1 == sale
            # Need to sale all stocks except the amount purchased in the same day
            print "Saling stock %s" % stock_code
            if stock_avaliable != 0:
                cmd = '3 ' + stock_code + ' ' + str(stock_avaliable) + ' ' + str(current_price)
                trade_volumn = stock_avaliable
                # updated_cash_avaliable = cash_avaliable + (trade_volumn * current_price)
                asset_pool['cash_avaliable'] += (trade_volumn * current_price)
                asset_pool['total_stock_value'] -= (trade_volumn * current_price)
                stock_pool[stock_code][0] -= trade_volumn
                stock_pool[stock_code][1] -= trade_volumn
                stock_pool[stock_code][2] -= trade_volumn * current_price

            elif stock_Remain != 0 and stock_avaliable == 0:
                trade_volumn = stock_Remain
                trade_type = 9  # 9 is the resale sign
                self._log_mesg = self._log_mesg + "     T1 policy blocking, No avaliable stock %s in hand, stock_Remain is %s , sales is cancelled " \
                                                  "at %s \n." % (stock_code, stock_Remain, self._time_tag())
            else:
                self._log_mesg = self._log_mesg + "     No avaliable stock %s in hand, sales is cancelled " \
                                                  "at %s \n." % (stock_code, self._time_tag())
        else:
            print "Unknown trading Signal"

        self._log_mesg = self._log_mesg + "     Trading command CMD %s is sent at %s \n" % (cmd, self._time_tag())

        self._log_mesg = self._log_mesg + "     CMD %s is received at %s \n" % (cmd, self._time_tag())
        print self._log_mesg

        '''
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
        '''
        done = True
        # self._write_log(self._log_mesg)
        return done


class C_BestMACDPattern_PoolValidation(C_Algorithems_BestPattern_pool_validation):
    '''
    Step1: decide what analysis period will be used (5min, 30min, daily)
    Step2: use MACD_Pattern_lists to calculate all trading signals of all patterns.
    Step3: use trading signals of all pattern to calculate ending profits of all pattern
    Step4: Analysis ending profits of all pattern to select the best pattern
    '''

    def __init__(self, period='m30'):
        C_Algorithems_BestPattern_pool_validation.__init__(self)
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

    def apply_best_MACD_pattern_to_data(self, period, stock_code, quo=None, ga=None, beta=None):
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

    def apply_best_MACD_pattern_to_data_test(self, period, stock_code, feed_records, stock_pool, asset_pool, quo=None,
                                             ga=None, beta=None):
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

        # df_stock_records = pd.read_sql(sql_fetch_records, con=self._engine, params=(period, stock_code),
        #                               index_col='quote_time')
        df_stock_records = feed_records
        # Send to calculate trade signal according to the best pattern
        # '''
        df_signals = C_MACD_Signal_Calculator._MACD_signal_calculation_M30_3(df_MACD_index, df_stock_records,
                                                                             to_DB=True, for_real=True, quo=quo, ga=ga,
                                                                             beta=beta).tail(1)

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
            # done, df_stock_infor = self._checking_stock_in_hand_pool(stock_code[2:], stock_pool)
            done = True
            stock_infor = stock_pool[stock_code]
            # print stock_infor
            '''
            if (done and signal == -1 and df_stock_infor.stockAvalible[0] != 0) or \
                    (signal == 1 and (
                                (df_stock_infor.stockRemain[0] + self._trading_volume) < self._stock_inhand_uplimit)):
            '''
            if (done and signal == -1 and stock_infor[0] != 0) or \
                    (signal == 1 and (
                                (stock_infor[1] + self._trading_volume) < self._stock_inhand_uplimit)):
                # Get real time price information
                print "---------------------------------------------------"
                print "Step3: Get real time price"
                self._log_mesg = self._log_mesg + "Step3: Get real time price at %s \n" % self._time_tag()
                # done, df_current_price = self._get_stock_current_price_pool(stock_code)
                done = True
                current_price = float(df_stock_records['close_price'].tail(1)[0])
                if done:
                    # Get cash avaliable information
                    print "---------------------------------------------------"
                    print "Step4: Got cash Avaliable"
                    self._log_mesg = self._log_mesg + "Step4: Get cash Avaliable at %s \n" % self._time_tag()
                    # done, cash_avaliable = self._get_cash_avaliable_pool()
                    done = True
                    cash_avaliable = asset_pool['cash_avaliable']
                    if done:
                        print "-------------------------------------------------"
                        print "step5: Send Traiding Singal"
                        self._log_mesg = self._log_mesg + "Step5: Send Trading Signal at %s \n" % self._time_tag()
                        done = self._send_trading_command_pool(stock_pool, asset_pool, stock_code, current_price,
                                                               cash_avaliable,
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
                                                      stock_infor[1], self._stock_inhand_uplimit,
                                                      self._time_tag())
        else:
            #
            # Update Asset
            #
            self._log_mesg = self._log_mesg + "     Trade Signal for stock %s is 0 at %s \n" % (
                stock_code, self._time_tag())
        self._log_mesg = self._log_mesg + "     Trading Process is finished at %s \r\n" % self._time_tag()
        self._write_log(self._log_mesg)

        finish_sign = True
        return finish_sign

    def _get_best_pattern(self, stock_code):
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


def main():
    c = C_Algorithems_BestPattern_pool_validation()
    # c._get_cash_avaliable_pool()
    test_records()



def test_records():
    c = C_BestMACDPattern_PoolValidation()
    # stock_codes = ['sz002310', 'sh600867', 'sz300146', 'sh600271']
    stock_codes = ['sz002310', 'sz300146', 'sh600271']
    stock_codes = ['sh600867', 'sz300146', 'sh600271']
    stock_codes = ['sz002310', 'sh600867']
    stock_codes = ['sz300146', 'sh600867']
    stock_codes = ['sz002310', 'sz300146']
    stock_codes = ['sh600271', 'sz300146']
    stock_codes = ['sz002310', 'sz300383']
    stock_codes = ['sz002310', 'sz300383', 'sz300146']
    stock_records_dict = {}
    # Prepare the stock pool using
    # Values Meaning: stock_avaliable, stock_remain, stock_value
    stock_pool = {'sz002310': [0, 0, 0.0],
                  'sh600867': [0, 0, 0.0],
                  'sz300146': [0, 0, 0.0],
                  'sh600271': [0, 0, 0.0],
                  'sz300383': [0, 0, 0.0],
                  'sz002180': [0, 0, 0.0],
                  'sh603658': [0, 0, 0.0]
                  }
    asset_pool = {'cash_begin': 100000.00,
                  'cash_avaliable': 100000.00,
                  'total_stock_value': 0.0,
                  'total_asset': 100000.00

                  }

    # Prepare Pricing Data
    sql_fetch_min_records = (
        "select * from (select * from tb_StockXMinRecords where period = %s and stock_code = %s order by quote_time DESC) as tbl order by quote_time ASC")

    for each_stock in stock_codes:
        stock_records_dict[each_stock] = pd.read_sql(sql_fetch_min_records, con=c._engine, params=('m30', each_stock),
                                                     index_col='quote_time')
    '''
    stock_records_dict = {
        stock_codes[0]: pd.read_sql(sql_fetch_min_records, con=c._engine, params=('m30', stock_codes[0]),
                                    index_col='quote_time'),
        stock_codes[1]: pd.read_sql(sql_fetch_min_records, con=c._engine, params=('m30', stock_codes[1]),
                                    index_col='quote_time'),
        stock_codes[2]: pd.read_sql(sql_fetch_min_records, con=c._engine, params=('m30', stock_codes[2]),
                                    index_col='quote_time'),
        stock_codes[3]: pd.read_sql(sql_fetch_min_records, con=c._engine, params=('m30', stock_codes[3]),
                                    index_col='quote_time')
    }
    '''
    # Prepare processing order data
    sql_select_quote_time = (
    "select quote_time from tb_StockXMinRecords where period = 'm30' and stock_code = 'sz002310' order by quote_time DESC")
    df_quote_time = pd.read_sql(sql_select_quote_time, con=c._engine)
    test_count = 690
    end_pos = len(df_quote_time) - test_count
    start_pos = end_pos + 400
    end_time = df_quote_time.iloc[end_pos][0]
    start_time = df_quote_time.iloc[start_pos][0]
    # print start_time, end_time

    f = open('stock_pool_text.csv', 'wb')
    writer = csv.writer(f)
    header = ['quote_time', asset_pool.keys()[0], asset_pool.keys()[1],
              asset_pool.keys()[2], asset_pool.keys()[3]]

    for each_stock in stock_codes:
        header.append(each_stock)
    writer.writerow(header)

    # print stock_pool.values()[3][2]
    # Process
    i = 0
    while i < 290:
        content = []
        common = []
        total_stock_value = 0.0
        for stock_code in stock_codes:
            df = stock_records_dict[stock_code]
            df = df[df.index >= start_time]
            feed_records = df[df.index <= end_time]
            last_row = feed_records.tail(1)

            # Send for process
            c.apply_best_MACD_pattern_to_data_test('m30', stock_code, feed_records, stock_pool, asset_pool)

            # Update pool
            stock_pool[stock_code][2] = stock_pool[stock_code][0] * float(last_row['close_price'][0])
            total_stock_value += stock_pool[stock_code][2]
            asset_pool['total_stock_value'] = total_stock_value
            asset_pool['total_asset'] = asset_pool['cash_avaliable'] + total_stock_value

            common = [last_row.index[0], asset_pool.values()[0], asset_pool.values()[1],
                      asset_pool.values()[2], asset_pool.values()[3]]

            content.extend([last_row['stock_code'][0], last_row['close_price'][0], stock_pool[stock_code][0],
                            stock_pool[stock_code][1], stock_pool[stock_code][2]])
            # content.extend(stock_pool[stock_code])

        content = common + content
        writer.writerow(content)
        i += 1
        end_pos -= 1
        start_pos -= 1
        end_time = df_quote_time.iloc[end_pos][0]
        start_time = df_quote_time.iloc[start_pos][0]
    f.close()
    print "Asset Pool is: ", asset_pool
    print "Stock Pool is: ", stock_pool
    #print asset_pool, stock_pool

if __name__ == '__main__':
    main()

