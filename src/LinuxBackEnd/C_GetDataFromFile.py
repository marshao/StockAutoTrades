#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import datetime, time, os
import pandas as pd
import C_GlobalVariable as glb


class C_GetDataFromFile(object):
    '''
    Change: 2017-06-22 in _send_trading_command: modified the received signal to 1 and -1 to match the signal calculation result.
            2017-06-22 in _MACD_Singal_calcualtion_cross use MACD_short = 999 to represent real life stock operation and save them in DB.
    '''

    def __init__(self):
        gv = glb.C_GlobalVariable()
        self._emailobj = gv.get_emailobj()
        self._master_config = gv.get_master_config()
        self._input_path = self._master_config['ubuntu_file_input_dir']
        self._engine = self._master_config['db_engine']

    def _get_daily_data(self):
        columns = ['stock_code', 'stock_name', 'quote_time', 'stock_industry', 'stock_idea', 'stock_location',
                   'open_price', 'high_price', 'low_price', 'close_price',
                   'hfq_price', 'qfq_price', 'price_change', 'trading_volumn', 'trading_amount', 'turnover',
                   'market_equity',
                   'total_equity', 'up_limited', 'down_limited', 'PE_TTM', 'PS_TTM', 'PCF_TTM', 'PB', 'MA_5',
                   'MA_10', 'MA_20', 'MA_30', 'MA_60', 'MA_Cross', 'MACD_DIF', 'MACD_DEA', 'MACD_MACD', 'MACD_Cross',
                   'KDJ_K', 'KDJ_D',
                   'KDJ_J', 'KDJ_Cross', 'Boll_Medim', 'Boll_Up', 'Boll_Down', 'PSY', 'PSYMA', 'RSI1', 'RSI2', 'RSI3',
                   'vibration', 'LMR']
        for rt, dirs, files in os.walk(self._input_path):
            for file in files:
                # df = pd.read_csv(os.path.join(rt, file), sep=',', skiprows=1, names=columns)
                print os.path.join(rt, file)
                dt = '/home/marshao/UWShare/outrange'
                pr = '/home/marshao/UWShare/processed'
                df = pd.read_csv(os.path.join(rt, file), sep=',')
                df.columns = [columns]
                try:
                    df.to_sql(name='tb_StockFullHistoryDailyRecords', if_exists='append', con=self._engine, index=False)
                    os.rename(os.path.join(rt, file), os.path.join(pr, file))
                except:
                    os.rename(os.path.join(rt, file), os.path.join(dt, file))

    def _relocate_m30_data(self):
        hd = '/home/marshao/UWShare/2017'
        sd = '/home/marshao/UWShare/m30'

        for rt, dirs, files in os.walk(hd):
            for file in files:
                if file.find('30min') != -1:
                    os.rename(os.path.join(rt, file), os.path.join(sd, file))
                    print os.path.join(sd, file)

    def get_m30_data(self):
        sd = '/home/marshao/UWShare/m30'
        pr = '/home/marshao/UWShare/processed'
        od = '/home/marshao/UWShare/outrange'
        columns = ['stock_code', 'quote_time', 'open_price', 'high_price', 'low_price', 'close_price', 'trading_volumn',
                   'trading_amount', 'trading_count']

        # self._relocate_m30_data()

        for rt, dirs, files in os.walk(sd):
            for file in files:
                print os.path.join(rt, file)
                df = pd.read_csv(os.path.join(rt, file), sep=',', skiprows=0, header=1)
                df.columns = [columns]
                df.to_sql(name='tb_2017M30Records', if_exists='append', con=self._engine, index=False)
                '''
                try:
                    df.to_sql(name='tb_2017M30Records', if_exists='append', con=self._engine, index=False)
                    os.rename(os.path.join(rt, file), os.path.join(pr, file))
                except:
                    os.rename(os.path.join(rt, file), os.path.join(od, file))
                '''

def main():
    gf = C_GetDataFromFile()
    # gf._relocate_m30_data()
    gf.get_m30_data()


if __name__ == '__main__':
    main()
