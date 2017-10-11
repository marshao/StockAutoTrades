#!/usr/local/bin/python
# coding: utf-8

import pandas, urllib, datetime
from sqlalchemy import create_engine


def get_data_qq(stock_code='sz300226'):
    data_source = {'qq_realtime': 'http://qt.gtimg.cn/q=%s'}
    engine = create_engine('mysql+mysqldb://marshao:123@10.0.2.15/DB_StockDataBackTest')
    got = True
    url = data_source['qq_realtime'] % stock_code
    html = urllib.urlopen(url)
    data = html.read()

    real_time_DF_columns_qq = ['stock_name', 'stock_code', 'current_price', 'close_price', 'open_price',
                               'current_buy_amount', 'current_sale_amount',
                               'buy1_price', 'buy1_apply', 'buy2_price', 'buy2_apply', 'buy3_price', 'buy3_apply',
                               'buy4_price', 'buy4_apply', 'buy5_price', 'buy5_apply',
                               'sale1_price', 'sale1_apply', 'sale2_price', 'sale2_apply', 'sale3_price',
                               'sale3_apply',
                               'sale4_price', 'sale4_apply', 'sale5_price', 'sale5_apply',
                               'time', 'net_chg', 'net_chg_percent', 'high_price', 'low_price', 'total_volumn',
                               'total_amount',
                               'turnover_rate', 'PE', 'circulation_market_value', 'total_market_value', 'PB',
                               'limit_up', 'limit_down']
    real_time_data_DF = pandas.DataFrame(columns=real_time_DF_columns_qq)
    sPos = data.find('=')
    data = data[sPos + 2:-3].split('~')
    tmp_l = []
    tmp_l.append(data[1])  # stock_name
    tmp_l.append(data[2])  # stock_code
    tmp_l.append(float(data[3]))  # current_price
    tmp_l.append(float(data[4]))  # close_price
    tmp_l.append(float(data[5]))  # open_price
    tmp_l.append(int(data[7]))  # buying_amount
    tmp_l.append(int(data[8]))  # saling_amount
    tmp_l.append(float(data[9]))  # buy1_price
    tmp_l.append(int(data[10]))  # buy1_amount
    tmp_l.append(float(data[11]))  # buy2_price
    tmp_l.append(int(data[12]))  # buy2_amount
    tmp_l.append(float(data[13]))  # buy3_price
    tmp_l.append(int(data[14]))  # buy3_amount
    tmp_l.append(float(data[15]))  # buy4_price
    tmp_l.append(int(data[16]))  # buy4_amount
    tmp_l.append(float(data[17]))  # buy5_price
    tmp_l.append(int(data[18]))  # buy5_amount
    tmp_l.append(float(data[19]))  # sale1_price
    tmp_l.append(int(data[20]))  # sale1_amount
    tmp_l.append(float(data[21]))  # sale2_price
    tmp_l.append(int(data[22]))  # sale2_amount
    tmp_l.append(float(data[23]))  # sale3_price
    tmp_l.append(int(data[24]))  # sale3_amount
    tmp_l.append(float(data[25]))  # sale4_price
    tmp_l.append(int(data[26]))  # sale4_amount
    tmp_l.append(float(data[27]))  # sale5_price
    tmp_l.append(int(data[28]))  # sale5_amount
    tmp_l.append(datetime.datetime.strptime(data[30], '%Y%m%d%H%M%S'))  # trade_time
    tmp_l.append(float(data[31]))  # net_chg
    tmp_l.append(float(data[32]))  # net_chg_percent
    tmp_l.append(float(data[33]))  # high_price
    tmp_l.append(float(data[34]))  # low_price
    tmp_l.append(int(data[36]))  # trading_volumn
    tmp_l.append(int(data[37]))  # trading_amount
    tmp_l.append(float(data[38]))  # turnover_rate
    if data[39] != '':
        tmp_l.append(float(data[39]))  # PE
    else:
        tmp_l.append(0.00)  # PE
    tmp_l.append(float(data[44]))  # circulate_market_value
    tmp_l.append(float(data[45]))  # total_market_value
    tmp_l.append(float(data[46]))  # PB
    tmp_l.append(float(data[47]))  # limit_up
    tmp_l.append(float(data[48]))  # limit_down
    real_time_data_DF.loc[len(real_time_data_DF)] = tmp_l
    print "processed real time data", real_time_data_DF
    real_time_data_DF.to_sql('tb_StockRealTimeRecords', engine, if_exists='append', index=False)
    return got


def main():
    get_data_qq()


if __name__ == '__main__':
    main()
