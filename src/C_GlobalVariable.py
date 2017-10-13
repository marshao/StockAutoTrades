#!/usr/local/bin/python
# coding: utf-8

from sqlalchemy import create_engine


class C_GlobalVariable(object):
    '''
    This is a global variable definition class, all global variables should be centrally controlled and
     referenced from this file.
    '''

    def __init__(self):
        self._master_config = {'ubuntu_input_dir': '/home/marshao/DataMiningProjects/Input/',
                               'ubuntu_output_dir': '/home/marshao/DataMiningProjects/Output/',
                               'win_input_dir': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\',
                               'win_output_dir': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\',
                               'win_install_dir': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\',
                               'win_processed_dir': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\processed\\',
                               'op_log': 'operLog.txt',
                               'validate_log': 'validateLog.txt',
                               'pro_back_ip': '10.175.10.231',
                               'pro_back_name': 'bei1ubt81',
                               'pro_front_ip': '10.175.10.99',
                               'pro_front_name': 'bei1python',
                               'dev_back_ip': '10.176.50.233',
                               'dev_back_name': 'bei2ubt-dev',
                               'dev_front_ip': '10.176.50.232',
                               'dev_front_name': 'bei2python',
                               'db_name': 'DB_StockDataBackTest',
                               'pro_db_engine': create_engine(
                                   'mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest'),
                               'dev_db_engine': create_engine(
                                   'mysql+mysqldb://marshao:123@10.176.50.233/DB_StockDataBackTest')
                               }

        self._stock_p_m30 = {'stock_codes': ['sz002310', 'sh600867', 'sz300146', 'sh600271'],
                             'sz002310': [0.7, 0.3, 0.2],
                             'sh600867': [0.8, 0.6, 0.2],
                             'sz300146': [0.7, 0.3, 0.2],
                             'sh600271': [0.9, 0.4, 0.2]
                             }

        self._calcu_config = {'ubuntu_processors': 2,
                              'win_port': 32768,
                              'trading_volume': 3000,
                              'stock_inhand_uplimit': 3500,
                              'cash_begin': 60000.0,
                              'stock_volume_begin': 0,
                              'period': 'm30'
                              }

    def get_master_config(self):
        return self._master_config

    def get_stock_config(self):
        return self._stock_p_m30

    def get_calcu_config(self):
        return self._calcu_config


def main():
    te = C_GlobalVariable


if __name__ == '__main__':
    main()
