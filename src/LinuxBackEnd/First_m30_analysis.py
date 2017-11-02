#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import pandas as pd
import scipy as sp
import numpy as np
import matplotlib as mp
import statsmodels.api as sm
import sklearn as sk
import datetime
from sqlalchemy import create_engine

dev_db_engine = create_engine('mysql+mysqldb://marshao:123@10.176.50.233/DB_StockDataBackTest')


def first_m30_follow():
    sql_select_stockcodes = ("select stock_code from tb_StockCodeList")
    df_codes = pd.read_sql(sql_select_stockcodes, con=dev_db_engine)
    df_compares = pd.DataFrame(
        columns=['stock_code', 'follow_count', 'total_count', 'follow_rate', 'reverse_count', 'reverse_rate'])
    i = 0
    for index, code_line in df_codes.iterrows():
        code = code_line['stock_code']
        sql_select_m30_records = (
        "SELECT stock_code, quote_time, open_price, close_price FROM tb_2017M30Records where stock_code = %(code)s")
        df_sh = pd.read_sql(sql_select_m30_records, con=dev_db_engine, params={'code': code})
        quote_day = []
        vib_f30 = []
        vib_day = []
        for idx, row in df_sh.iterrows():
            t = row['quote_time'].to_pydatetime()
            if str(t.time()) == '10:00:00':
                quote_day.append(str(t.date()))
                day_open = row['open_price']
                first_close = row['close_price']
                vib_f30.append(np.log(first_close / day_open))
            elif str(t.time()) == '15:00:00':
                day_close = row['close_price']
                vib_day.append(np.log(day_close / day_open))

        np_vib_f30 = np.array(vib_f30).T
        np_vib_day = np.array(vib_day).T
        np_rates = np.array([quote_day, vib_f30, vib_day]).T
        np_fod = np.array((np_vib_f30 / np_vib_day).T)
        df_rates = pd.DataFrame(np_rates, columns=['quote_date', 'vib_f30', 'vib_day'])
        df_rates.set_index('quote_date')
        df_rates.loc[:, 'fod'] = np_fod
        df1 = df_rates[(df_rates['fod'] < 0.5) & (df_rates['fod'] > 0)]
        df2 = df_rates[df_rates['fod'] < 0]  # nagitive cov
        print "%s/%s or %s and %.2f" % (
        df1.shape[0], df_rates.shape[0], code, (float(df1.shape[0]) / float(df_rates.shape[0])))
        df_compares.loc[i, 'stock_code'] = code
        df_compares.loc[i, 'follow_count'] = df1.shape[0]
        df_compares.loc[i, 'total_count'] = df_rates.shape[0]
        df_compares.loc[i, 'follow_rate'] = float(df1.shape[0]) / float(df_rates.shape[0])
        df_compares.loc[i, 'reverse_count'] = df2.shape[0]
        df_compares.loc[i, 'reverse_rate'] = float(df2.shape[0]) / float(df_rates.shape[0])
        i += 1


def main():
    pass
    # cal_specific_pattern()
    # caL_all_pattern()


if __name__ == '__main__':
    main()
