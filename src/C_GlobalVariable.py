#!/usr/local/bin/python
# coding: utf-8

from sqlalchemy import create_engine
import C_Email
import datetime


class C_GlobalVariable(object):
    '''
    This is a global variable definition class, all global variables should be centrally controlled and
     referenced from this file.
    '''

    def __init__(self):
        self._calcu_config = {'ubuntu_processors': 1,
                              'trading_volume': 3000,
                              'stock_inhand_uplimit': 3500,
                              'cash_begin': 60000.0,
                              'stock_volume_begin': 0,
                              'period': 'm30',
                              'pro_db_engine': create_engine(
                                  'mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest?charset=utf8',
                                  encoding='utf-8', pool_size=150),
                              'dev_db_engine': create_engine(
                                  'mysql+mysqldb://marshao:123@10.176.50.233/DB_StockDataBackTest?charset=utf8',
                                  encoding='utf-8', pool_size=150)
                              }

        self._master_config = {'ubuntu_input_dir': '/home/marshao/DataMiningProjects/Input/',
                               'ubuntu_file_input_dir': '/home/marshao/UWShare/hist-data',
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
                               'win_port': 32768,
                               'db_name': 'DB_StockDataBackTest',
                               'db_engine': self._calcu_config['pro_db_engine'],
                               'x_min':['m1', 'm5', 'm15', 'm30', 'm60'],
                               'x_period': ['day', 'week'],
                               'q_count': ['320', '50', '16', '16', '4'],
                               'fq': ['qfq', 'hfq', 'bfq'],
                               'data_source':{'sina': 'http://hq.sinajs.cn/list=', 'qq_realtime': 'http://qt.gtimg.cn/q=%s',
                                                'qq_1_min': 'http://web.ifzq.gtimg.cn/appstock/app/minute/query?_var=min_data_%s&code=%s',
                                                'qq_x_min': 'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=%s,%s,,%s',
                                                'qq_x_period': 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,%s,,,%s,%s'},
                               'start_morning':datetime.time(9, 20, 0),
                               'end_morning':datetime.time(11, 32, 0),
                               'start_afternoon':datetime.time(12, 50, 0),
                               'end_afternoon':datetime.time(15, 10, 0)
                               }

        '''
        self._stock_config = {'stock_codes': ['sz002310', 'sh600867', 'sz300146', 'sh600271'],
                             'stock_m30_config':{
                                'sz002310': [0.7, 0.3, 0.2],
                                'sh600867': [0.8, 0.6, 0.2],
                                'sz300146': [0.7, 0.3, 0.2],
                                'sh600271': [0.9, 0.4, 0.2]}

                             }
        '''
        # ga, quo, beta
        self._stock_config = {'stock_codes': ['sz002310', 'sh603658', 'sz300383', 'sz002180'],
                              'stock_m30_config': {
                                  'sz002310': [0.7, 0.3, 0.2],
                                  'sh603658': [0.8, 0.8, 0.2],
                                  'sz300383': [0.8, 0.3, 0.2],
                                  'sz002180': [0.8, 0.3, 0.2]}

                              }

        '''
                self._data_source = {'sina': 'http://hq.sinajs.cn/list=', 'qq_realtime': 'http://qt.gtimg.cn/q=%s',
                                     'qq_1_min': 'http://web.ifzq.gtimg.cn/appstock/app/minute/query?_var=min_data_%s&code=%s',
                                     'qq_x_min': 'http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=%s,%s,,%s',
                                     'qq_x_period': 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,%s,,,%s,%s'}
                self._start_morning = datetime.time(9, 20, 0)
                self._end_morning = datetime.time(11, 32, 0)
                self._start_afternoon = datetime.time(12, 50, 0)
                self._end_afternoon = datetime.time(15, 10, 0)

                _conf = {
                'stockInHandFile': 'C:\DataMining\\03.StockAutoTrades\output\\stockInHand.csv',
                'stockTradeToday': 'C:\DataMining\\03.StockAutoTrades\output\\stockTradeToday.csv',
                'outputDir': 'C:\DataMining\\03.StockAutoTrades\output\\',
                'installDir': 'C:\DataMining\\03.StockAutoTrades\\',
                'confFile': '_conf.ini',
                'opLog': 'operlog.txt',
                'trLog': 'tradeLog.txt',
                'assetFile': 'stockAsset.txt'}

                _attrs_window = {'mainWindowHandle': '0',
                                 'stockHoldingHandle': '0',
                                 'cashAvaliableHandle': '0',
                                 'stockValueHandle': '0',
                                 'totalAssetHandle': '0',
                                 'saveAsEditHandle': '0'}

                _attrs_buy_window = {'buyStockCodeHandle': [],
                                     'buyPriceHandle': [], 'buyAmountHandle': [],
                                     'buyButtonHandle': '0', 'buyLastPriceHandle': '0',
                                     'buyPrice1Handle': '0', 'buyPrice2Handle': '0'}

                _attrs_sale_window = {'saleStockCodeHandle': [], 'salePriceHandle': [],
                                      'saleAmountHandle': [], 'saleButtonHandle': '0',
                                      'saleLastPriceHandle': '0', 'salePrice1Handle': '0', 'salePrice2Handle': '0', }

                _trade_windows_properties = {'mainWindowName': '广发证券核新网上交易系统7.60',
                                             'stockCodeWindowXY': [291, 114, 1032],
                                             'stockPriceWindowXY': [291, 150, 1033],
                                             'stockAmountWindowXY': [291, 186, 1034],
                                             'buyWindowName': '买入[B]', 'buyWindowClass': 'Button',
                                             'buySaleWindowXY': [315, 210, 1006],
                                             'lastBuyPriceXY': [454, 174, 1024],
                                             'lastSalePriceXY': [562, 174, 1027],
                                             'buy1PriceXY': [454, 192, 1018],
                                             'buy2PriceXY': [454, 206, 1025],
                                             'sale1PriceXY': [454, 156, 1021],
                                             'sale2PriceXY': [454, 143, 1022],
                                             'stockHoldingXY': [211, 182, 1047],
                                             'cashAvaliableXY': [274, 108, 1012],
                                             'stockValueXY': [516, 126, 1014],
                                             'totalAssetXY': [517, 144, 1015],
                                             'saveAsEditXY': [271, 372, 1152]}

                _stock_trades = {'stockCode': '000003', 'tradeAmount': '1000', 'tradePrice': '1000',
                                 'buyLastPrice': '0', 'buyPrice1': '0', 'buyPrice2': '0',
                                 'saleLastPrice': '0', 'salePrice1': '0', 'salePrice2': '0'}

                _asset_infor = {'cashAvaliable': 0, 'stockValue': 0, 'totalAsset': 0}
                '''

    def get_master_config(self):
        return self._master_config

    def get_stock_config(self):
        return self._stock_config

    def get_calcu_config(self):
        return self._calcu_config

    def get_emailobj(self):
        emailobj = C_Email.C_Email()
        return emailobj

    def get_con(self, type='pro'):
        if type == 'pro':
            con = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')
        else:
            con = create_engine('mysql+mysqldb://marshao:123@10.176.50.233/DB_StockDataBackTest')
        return con


def main():
    te = C_GlobalVariable


if __name__ == '__main__':
    main()
