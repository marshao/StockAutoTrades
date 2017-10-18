#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type


import pandas as pd
import datetime, time
from C_Algorithms_BestMACDPattern import C_MACD_Signal_Calculation, C_BestMACDPattern
from src import C_GlobalVariable as glb


class C_Operation_Validation(object):
    '''
    Change: 2017-06-22 in _send_trading_command: modified the received signal to 1 and -1 to match the signal calculation result.
            2017-06-22 in _MACD_Singal_calcualtion_cross use MACD_short = 999 to represent real life stock operation and save them in DB.
    '''

    def __init__(self):
        gv = glb.C_GlobalVariable()
        self._master_config = gv.get_master_config()
        self._calcu_config = gv.get_calcu_config()
        self._stock_p_m30 = gv.get_stock_config()['stock_m30_config']

        self._output_dir = self._master_config['ubuntu_output_dir']
        self._input_dir = self._master_config['ubuntu_input_dir']
        self._operation_log = self._output_dir + self._master_config['op_log']
        self._engine = self._master_config['dev_db_engine']
        self._validation_log = self._output_dir + 'validateLog.txt'



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

    def _write_log(self, log_mesg, logPath='validateLog.txt'):
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

    def get_last_signal_from_DB(self, stock_code):
        MACD_Trading_Signal_Cal = C_MACD_Signal_Calculation()
        MACDPattern = C_BestMACDPattern()
        pattern_signal = [MACDPattern._get_best_pattern(stock_code, simplified=False), ]
        quo = self._stock_p_m30[stock_code][0]
        ga = self._stock_p_m30[stock_code][1]
        beta = self._stock_p_m30[stock_code][2]
        df_signals = MACD_Trading_Signal_Cal._single_pattern_signal_cal(MACD_pattern=pattern_signal, period="m30",
                                                                        stock_code="sz002310", quo=quo, ga=ga,
                                                                        beta=beta, simplified=False)
        # print df_signals.tail(1)
        return df_signals.tail(1)

    def get_last_signal_from_log(self, stock_code, quote_time):

        f = reversed(open(self._operation_log, 'r').readlines())
        # with reversed(open(self._operation_log, 'r').readlines()) as f:
        done = False
        inline_signal = str(0)
        curr = f.next()
        for line in f:
            prev, curr = line, f.next()
            if curr.startswith("Step1:"):
                words = prev.split()
                inline_stock_code = words[13][0:8]
                inline_quote_time = words[19] + ' ' + words[20][0:5]
                if (inline_stock_code == stock_code):
                    if (quote_time == inline_quote_time):
                        inline_signal = words[15]
                        # quote_time = words[20]
                        print 'inline: ' + inline_stock_code + ' at ' + inline_quote_time + ' signal is: ' + inline_signal
                        done = True
                        break
        return done, inline_signal

    def signal_validation(self, stock_code):
        '''
        Verify whether the signals in log match the signal from validation.
        :param stock_code:
        :return:
        '''
        df_last_compute_signal = self.get_last_signal_from_DB(stock_code)
        quote_time = str(df_last_compute_signal.index[0])[0:15] + '3'
        signal = str(df_last_compute_signal['Signal'][0])
        done, inline_signal = self.get_last_signal_from_log(stock_code, quote_time)
        if done:
            if signal == inline_signal:
                print "The signal is verified, stock %s generate signal %s at %s" % (
                stock_code, inline_signal, quote_time)
            else:
                print "The signal is not verified, stock %s generate verification signal %s and inline signal %s at %s" % (
                    stock_code, signal, inline_signal, quote_time)
        else:
            print "The signal is not verified, stock %s could not find corresponding signal time %s in log, the system " \
                  "verification signal is %s" % (stock_code, quote_time, signal)

    def transaction_validation(self, stock_code):
        '''
        Verify whether the buy or sale command is processed successfully or not.
        :param stock_code:
        :return:
        '''
        pass


def main():
    ov = C_Operation_Validation()
    ov.signal_validation('sh600867')
    # ov.get_last_signal_from_DB('sz002310')
    #ov.get_last_signal_from_log('sz002310')


if __name__ == '__main__':
    main()
