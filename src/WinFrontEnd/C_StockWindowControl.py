# coding=GBK

import win32gui, win32com.client, win32api, win32con
from win32con import *
from time import sleep
import datetime
import pandas as pd
from sqlalchemy import create_engine

__metclass__ = type


class C_StockWindowControl:

    WM_CHAR = 0x0102

    _log_mesg = 'Log Start'

    _conf = {
        'stockInHandFile': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\stockInHand.csv',
        'outputDir': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\',
        'installDir': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\',
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

    _attrs_buy_window = {'buyStockCodeHandle': '0',
                         'buyPriceHandle': '0', 'buyAmountHandle': '0',
                         'buyButtonHandle': '0', 'buyLastPriceHandle': '0',
                         'buyPrice1Handle': '0', 'buyPrice2Handle': '0'}

    _attrs_sale_window = {'saleStockCodeHandle': '0', 'salePriceHandle': '0',
                          'saleAmountHandle': '0', 'saleButtonHandle': '0',
                          'saleLastPriceHandle': '0', 'salePrice1Handle': '0', 'salePrice2Handle': '0', }

    _trade_windows_properties = {'mainWindowName': '广发证券核新网上交易系统7.56',
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

    # tempwindow = win32gui.GetActiveWindow()

    # tempwindow = win32gui.GetWindow(7607276)

    def __init__(self):

        self._find_main_window()
        mainWindowHwnd = self._attrs_window['mainWindowHandle']
        print mainWindowHwnd
        # self._set_fore_ground_window(mainWindowHwnd)
        # win32gui.SetActiveWindow(mainWindowHwnd)
        # sleep(1)

        '''
        Initiate Buy/Sale window handles
        '''
        # try:

        sleep(1)
        self._set_fore_ground_window(mainWindowHwnd)
        print  "Set window fore ground"
        sleep(3)
        win32gui.SetActiveWindow(mainWindowHwnd)
        print "set active window"
        sleep(3)
        self._sale_page()
        win32gui.EnumChildWindows(mainWindowHwnd, self._enum_child_window_proc, 'Sale')
        print self._attrs_sale_window

        sleep(1)
        # self._set_fore_ground_window(mainWindowHwnd)
        self._buy_page()
        win32gui.EnumChildWindows(mainWindowHwnd, self._enum_child_window_proc, 'Buy')
        print self._attrs_buy_window
        '''
            Initiate Stock Holding and Capital Information
            '''

        sleep(1)
        # self._set_fore_ground_window(mainWindowHwnd)
        self._stock_holding_page()
        win32gui.EnumChildWindows(mainWindowHwnd, self._enum_child_window_proc, 'Infor')
        print self._attrs_window

        if self._save_stock_infor_to_file(mainWindowHwnd) == False:
            self._log_mesg = '\n Sorry, system can not initialize Trading Software at ' + self._time_tag()
        else:
            self._log_mesg = '\n Yes, system initialized Trading Software successfully at ' + self._time_tag()

        self.update_asset()
        print self._asset_infor

        self.update_quote_price()
        print self._stock_trades
        # except:
        #   self._log_mesg = '\n Sorry, system can not initialize Trading Software at ' + self._time_tag()

        print self._log_mesg
        self._write_log(self._log_mesg)
        self._time_tag()

    def _enum_window_proc(self, hwnd, tradeCode):
        if hwnd == 133240 or hwnd == 133236:
            rect = win32gui.GetWindowRect(hwnd)
            x = rect[0]
            y = rect[1]
            # w = rect[2] - x
            # h = rect[3] - y
            print "\n Windows Handle %d" % hwnd
            print "Windows X %d" % x
            print "Windows y %d" % y
            print "Windows Title ", win32gui.GetWindowText(hwnd)
        '''
        if tradeCode == 'Buy':
            (x1, y1) = tradeWindows.get('stockCodeWindowXY')
            if x == x1 and y == y1:
                attrBuyWindow['buyStockCodeHandle'] = hwnd
                print "Windows Handle %d" %hwnd
                #print "Windows X %d" %x
                #print "Windows y %d" %y
            (x1, y1) = tradeWindows.get('stockPriceWindowXY')
            if x == x1 and y == y1:
                attrBuyWindow['buyPriceHandle'] = hwnd
            (x1, y1) = tradeWindows.get('stockAmountWindowXY')
            if x == x1 and y == y1:
                attrBuyWindow['buyAmountHandle'] = hwnd

        else:
            pass

        hwndStock = 0
        if win32gui.GetWindowText(hwnd) == tradeWindows.get('mainWindowName')[0]:
            self._attrs_window['mainWindowHanle'] = hwnd
        '''

    def _get_handle_by_XY(self, hwnd, x, y, attrs, sizeKey, handleKey):
        if (x == self._trade_windows_properties[sizeKey][0]) and (
                        y == self._trade_windows_properties[sizeKey][1] and attrs[handleKey] == '0'):
            attrs[handleKey] = hwnd
            # print win32gui.GetWindowText(hwnd)
            # print attrs
        else:
            pass

    def _enum_child_window_proc(self, hwnd, tradeCode):

        if tradeCode == 'Buy':
            if win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['stockCodeWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockCodeWindowXY'
                handleKey = 'buyStockCodeHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_buy_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['stockPriceWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockPriceWindowXY'
                handleKey = 'buyPriceHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_buy_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['stockAmountWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockAmountWindowXY'
                handleKey = 'buyAmountHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_buy_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['buySaleWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'buySaleWindowXY'
                handleKey = 'buyButtonHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_buy_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['lastBuyPriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'lastBuyPriceXY'
                handleKey = 'buyLastPriceHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_buy_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['buy1PriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'buy1PriceXY'
                handleKey = 'buyPrice1Handle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_buy_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['buy2PriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'buy2PriceXY'
                handleKey = 'buyPrice2Handle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_buy_window, sizeKey, handleKey)
            else:
                pass
        elif tradeCode == 'Sale':
            if win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['stockCodeWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockCodeWindowXY'
                handleKey = 'saleStockCodeHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_sale_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['stockPriceWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockPriceWindowXY'
                handleKey = 'salePriceHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_sale_window, sizeKey, handleKey)

            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['stockAmountWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockAmountWindowXY'
                handleKey = 'saleAmountHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_sale_window, sizeKey, handleKey)

            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['buySaleWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'buySaleWindowXY'
                handleKey = 'saleButtonHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_sale_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['lastSalePriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'lastSalePriceXY'
                handleKey = 'saleLastPriceHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_sale_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['sale1PriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'sale1PriceXY'
                handleKey = 'salePrice1Handle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_sale_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['sale2PriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'sale2PriceXY'
                handleKey = 'salePrice2Handle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_sale_window, sizeKey, handleKey)
            else:
                pass
        elif tradeCode == 'Infor':
            if win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['stockHoldingXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockHoldingXY'
                handleKey = 'stockHoldingHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['cashAvaliableXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'cashAvaliableXY'
                handleKey = 'cashAvaliableHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['stockValueXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockValueXY'
                handleKey = 'stockValueHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_window, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['totalAssetXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'totalAssetXY'
                handleKey = 'totalAssetHandle'
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_window, sizeKey, handleKey)
            else:
                pass
        elif tradeCode == 'saveAs':
            if win32gui.GetDlgCtrlID(hwnd) == self._trade_windows_properties['saveAsEditXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'saveAsEditXY'
                handleKey = 'saveAsEditHandle'
                print win32gui.GetWindowText(hwnd)
                self._get_handle_by_XY(hwnd, winRec[0], winRec[1], self._attrs_window, sizeKey, handleKey)
            else:
                pass

        else:
            pass

    def _save_stock_infor_to_file(self, hwnd):

        filename = self._conf['stockInHandFile']
        self._asset_infor['cashAvaliable'] = win32gui.GetWindowText(self._attrs_window['cashAvaliableHandle'])
        self._asset_infor['stockValue'] = win32gui.GetWindowText(self._attrs_window['stockValueHandle'])
        self._asset_infor['totalAsset'] = win32gui.GetWindowText(self._attrs_window['totalAssetHandle'])

        win32api.keybd_event(VK_CONTROL, 0, 0, 0)
        # sleep(1)
        win32api.keybd_event(83, 0, 0, 0)
        win32api.keybd_event(83, 0, win32con.KEYEVENTF_KEYUP, 0)
        # sleep(1)
        win32api.keybd_event(VK_CONTROL, 0, win32con.KEYEVENTF_KEYUP, 0)

        sleep(1)
        hwndPop = self._find_window_by_name('Save As')
        # hwndPop = self._attrs_window['mainWindowHandle']
        # print 'Savs as window has handle of %s' % hwndPop

        # self._set_fore_ground_window(hwndPop)
        # win32gui.SetActiveWindow(hwndPop)
        # sleep(2)
        win32gui.EnumChildWindows(hwndPop, self._enum_child_window_proc, 'saveAs')

        dirWindowHandle = self._attrs_window['saveAsEditHandle']
        # print "dir window has handle of %s"%dirWindowHandle

        for char in filename:
            win32api.SendMessage(dirWindowHandle, win32con.WM_CHAR, ord(char), None)

        sleep(1)

        win32api.keybd_event(VK_MENU, 0, 0, 0)
        sleep(1)
        win32api.keybd_event(83, 0, 0, 0)
        win32api.keybd_event(83, 0, win32con.KEYEVENTF_KEYUP, 0)
        sleep(1)
        win32api.keybd_event(VK_MENU, 0, win32con.KEYEVENTF_KEYUP, 0)

        sleep(1)
        win32api.keybd_event(VK_MENU, 0, 0, 0)
        win32api.keybd_event(89, 0, 0, 0)
        win32api.keybd_event(89, 0, win32con.KEYEVENTF_KEYUP, 0)
        win32api.keybd_event(VK_MENU, 0, win32con.KEYEVENTF_KEYUP, 0)

        self._log_mesg = '\n Congratulation, system saved stocks in hand message successfully at ' + self._time_tag()
        saved = True

        # print self._log_mesg
        return saved

    def _close_stock_window(self):

        win32api.keybd_event(VK_MENU, 0, 0, 0)

        win32api.keybd_event(115, 0, 0, 0)
        win32api.keybd_event(115, 0, win32con.KEYEVENTF_KEYUP, 0)  # 閲婃斁鎸夐敭
        win32api.keybd_event(VK_MENU, win32con.KEYEVENTF_KEYUP, 0)

    def _buy_page(self):
        # win32gui.SetForeGroundWindow(mainWindowHwnd)
        # Active Buy Window
        win32api.keybd_event(VK_F1, 0, 0, 0)  # 閲婃斁鎸夐敭
        win32api.keybd_event(VK_F1, 0, win32con.KEYEVENTF_KEYUP, 0)
        sleep(1)

    def _sale_page(self):
        # setForeGroundWindow(mainWindowHwnd)
        # win32gui.SetForegroundWindow(mainWindowHwnd)
        # Active Buy Window
        win32api.keybd_event(VK_F2, 0, 0, 0)  # 閲婃斁鎸夐敭
        win32api.keybd_event(VK_F2, 0, win32con.KEYEVENTF_KEYUP, 0)
        sleep(1)

    def _stock_holding_page(self):
        # win32gui.SetForeGroundWindow(mainWindowHwnd)
        # Active Infor Window
        win32api.keybd_event(VK_F4, 0, 0, 0)  # 閲婃斁鎸夐敭
        win32api.keybd_event(VK_F4, 0, win32con.KEYEVENTF_KEYUP, 0)
        sleep(1)

    def buy_stock(self, stockTrades):

        stockCode = stockTrades['stockCode']
        tradeAmount = stockTrades['tradeAmount']
        tradePrice = stockTrades['tradePrice']

        # Active Buy Window
        self._buy_page()

        try:
            # Send stockCode
            for char in stockCode:
                win32api.SendMessage(self._attrs_buy_window['buyStockCodeHandle'], win32con.WM_CHAR, ord(char), None)
            print "message sent"

            # Send tradeAmount
            for char in tradeAmount:
                win32api.SendMessage(self._attrs_buy_window['buyAmountHandle'], win32con.WM_CHAR, ord(char), None)
            print "Amount sent"

            # Send tradePrice
            for char in tradePrice:
                win32api.SendMessage(self._attrs_buy_window['buyPriceHandle'], win32con.WM_CHAR, ord(char), None)
            print "Price sent"

            # Send Sale Command
            sleep(1)
            win32api.keybd_event(66, 0, 0, 0)
            win32api.keybd_event(66, 0, win32con.KEYEVENTF_KEYUP, 0)  # 閲婃斁鎸夐敭

            # Send Y to Confirm the buy
            sleep(1)
            win32api.keybd_event(89, 0, 0, 0)
            win32api.keybd_event(89, 0, win32con.KEYEVENTF_KEYUP, 0)

            # Send Enter to confirm any message
            sleep(1)
            win32api.keybd_event(VK_RETURN, 0, 0, 0)
            win32api.keybd_event(VK_RETURN, 0, win32con.KEYEVENTF_KEYUP, 0)
            self._log_mesg = '\n Congratulation, system issue a BUY command of stock code %s, buy price %s, buy amount %s successfully.' % (
                stockCode, tradePrice, tradeAmount)
        except:
            self._log_mesg = '\n Sorry, system cannot issue a BUY command of stock code %s, buy price %s, buy amount %s' % (
                stockCode, tradePrice, tradeAmount)
            # write to log file
            self._write_log(self._log_mesg, self._conf['trLog'])

    def sale_stock(self, stockTrades):

        stockCode = stockTrades['stockCode']
        tradeAmount = stockTrades['tradeAmount']
        tradePrice = stockTrades['tradePrice']

        # Active Buy Window
        self._sale_page()

        try:
            # Send stockCode
            for char in stockCode:
                win32api.SendMessage(self._attrs_sale_window['saleStockCodeHandle'], win32con.WM_CHAR, ord(char), None)
            print "message sent"

            # Send tradeAmount
            for char in tradeAmount:
                win32api.SendMessage(self._attrs_sale_window['saleAmountHandle'], win32con.WM_CHAR, ord(char), None)
            print "Amount sent"

            # Send tradePrice
            for char in tradePrice:
                win32api.SendMessage(self._attrs_sale_window['salePriceHandle'], win32con.WM_CHAR, ord(char), None)
            print "Price sent"

            # Send Buy Command
            sleep(1)
            win32api.keybd_event(83, 0, 0, 0)
            win32api.keybd_event(83, 0, win32con.KEYEVENTF_KEYUP, 0)  # 閲婃斁鎸夐敭

            # Send Y to Confirm the Sale
            sleep(1)
            win32api.keybd_event(89, 0, 0, 0)
            win32api.keybd_event(89, 0, win32con.KEYEVENTF_KEYUP, 0)

            # Send Enter to confirm any message
            sleep(1)
            win32api.keybd_event(VK_RETURN, 0, 0, 0)
            win32api.keybd_event(VK_RETURN, 0, win32con.KEYEVENTF_KEYUP, 0)

            self._log_mesg = '\n Congratulation, system issue a SALE command of stock code %s, buy price %s, buy amount %s successfully.' % (
                stockCode, tradePrice, tradeAmount)
        except:
            self._log_mesg = '\n Sorry, system cannot issue a SALE command of stock code %s, buy price %s, buy amount %s' % (
                stockCode, tradePrice, tradeAmount)
            # write to log file
            self._write_log(self._log_mesg, self._conf['trLog'])

    def set_trade_price(self, price, priceIndex, trade='b'):

        price = str(price)
        if trade == 's':
            self._sale_page()
        else:
            self._buy_page()

        if priceIndex == 'b1':
            self._stock_trades['tradePrice'] = self._stock_trades['buyPrice1']
        elif priceIndex == 'b2':
            self._stock_trades['tradePrice'] = self._stock_trades['buyPrice2']
        elif priceIndex == 'b0':
            self._stock_trades['tradePrice'] = self._stock_trades['buyLastPrice']
        elif priceIndex == 's1':
            self._stock_trades['tradePrice'] = self._stock_trades['salePrice2']
        elif priceIndex == 's2':
            self._stock_trades['tradePrice'] = self._stock_trades['salePrice2']
        elif priceIndex == 's0':
            self._stock_trades['tradePrice'] = self._stock_trades['saleLastPrice']
        elif priceIndex == 'c0':
            self._stock_trades['tradePrice'] = price
        else:
            pass

    def set_stock_code(self, stockCode='000003', trade='b'):

        if trade == 's':
            self._sale_page()
        else:
            self._buy_page()

        if len(stockCode) == 6:
            self._stock_trades['stockCode'] = stockCode
            self._log_mesg = '\n Set stock code to %s ' % stockCode, " at ", self._time_tag()
            for char in stockCode:
                win32api.SendMessage(self._attrs_buy_window['buyStockCodeHandle'], win32con.WM_CHAR, ord(char), None)
        else:
            self._log_mesg = '\n ERROR: Set a wrong stock code %s ' % stockCode, " at ", self._time_tag()

            self._write_log(self._log_mesg)

    def set_trade_amount(self, tradeAmount='1000', trade='b'):
        if trade == 's':
            self._sale_page()
        else:
            self._buy_page()

        if int(tradeAmount) <= 0:
            self._log_mesg = '\n ERROR: System cannot take a nagtive trade amount'
        else:
            self._stock_trades['tradeAmount'] = tradeAmount
            self._log_mesg = '\n System set tradeAmount to %s' % tradeAmount
            # Send tradeAmount
            for char in tradeAmount:
                win32api.SendMessage(self._attrs_buy_window['buyAmountHandle'], win32con.WM_CHAR, ord(char), None)

                self._write_log(self._log_mesg)

    def update_quote_price(self):
        self._buy_page()

        self._stock_trades['buyLastPrice'] = win32gui.GetWindowText(self._attrs_buy_window['buyLastPriceHandle'])
        self._stock_trades['buyPrice1'] = win32gui.GetWindowText(self._attrs_buy_window['buyPrice1Handle'])
        self._stock_trades['buyPrice2'] = win32gui.GetWindowText(self._attrs_buy_window['buyPrice2Handle'])

        self._stock_trades['saleLastPrice'] = win32gui.GetWindowText(self._attrs_sale_window['saleLastPriceHandle'])
        self._stock_trades['salePrice1'] = win32gui.GetWindowText(self._attrs_sale_window['salePrice1Handle'])
        self._stock_trades['salePrice2'] = win32gui.GetWindowText(self._attrs_sale_window['salePrice2Handle'])

    def update_asset(self):
        '''
        Read Asset information from the external file
        '''
        fullPath = self._conf['outputDir'] + self._conf['assetFile']
        total_asset = []
        # set stock_holding page be active
        self._stock_holding_page()
        sleep(1)

        # Getting value from application
        self._asset_infor['cashAvaliable'] = win32gui.GetWindowText(self._attrs_window['cashAvaliableHandle'])
        total_asset.append(self._asset_infor['cashAvaliable'])
        self._asset_infor['stockValue'] = win32gui.GetWindowText(self._attrs_window['stockValueHandle'])
        total_asset.append(self._asset_infor['stockValue'])
        self._asset_infor['totalAsset'] = win32gui.GetWindowText(self._attrs_window['totalAssetHandle'])
        total_asset.append(self._asset_infor['totalAsset'])
        date_time = self._time_tag()
        total_asset.append(date_time)

        # write stock asset information into DB.
        df = pd.DataFrame(columns=['cash_avaliable', 'stock_value', 'total_asset', 'date_time'])
        df.loc[len(df)] = total_asset
        engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')
        df.to_sql('tb_StockAsset', con=engine, index=False, if_exists='append')
        return self._asset_infor['cashAvaliable']

    def _get_menu(self, hwnd):
        print win32gui.GetMenu(hwnd)

    def _set_fore_ground_window(self, hwnd):
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        win32gui.SetForegroundWindow(hwnd)
        win32gui.SetActiveWindow(hwnd)

    def _set_active_window2(self, hwnd):
        # win32gui.MoveWindow(hwnd, 0,0,800,600, True)
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        win32gui.SetForegroundWindow(hwnd)

    def _set_window_top(self, hwnd):

        print hwnd, " Move Window"
        win32gui.MoveWindow(hwnd, 0, 0, 800, 600, True)
        # win32gui.BringWindowToTop(hwnd)

    def _find_window_by_nameEx(self, parentHWND, childAfter, windowClass, windowName):
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        hwnd = win32gui.FindWindowEx(parentHWND, childAfter, windowClass, windowName)

        print "find Window Finished", windowName, " ", hwnd

    def _find_main_window(self, windowName=_trade_windows_properties['mainWindowName']):
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        hwnd = win32gui.FindWindow(None, windowName)
        win32gui.MoveWindow(hwnd, 0, 0, 800, 600, True)
        self._attrs_window['mainWindowHandle'] = hwnd

    def _find_window_by_name(self, windowName):
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        hwnd = win32gui.FindWindow(None, windowName)
        return hwnd

    def _get_active_window(self):
        hwnd = win32gui.GetForegroundWindow()
        print hwnd
        return hwnd

    def _get_handle_by_control_ID(self, parentHwnd, controlID):
        # print win32gui.GetDlgCtrlID(controlID)
        print win32gui.GetDlgItem(parentHwnd, controlID)

    def _write_log(self, logMesg=_log_mesg, logPath=_conf['opLog']):
        fullPath = self._conf['outputDir'] + logPath
        with open(fullPath, 'a') as log:
            log.writelines(logMesg)

    def _time_tag(self):
        # return time.asctime(time.localtime(time.time()))
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def main():
    '''
    Initiate the window handles of Stock Sale Page and Stock Buy Page
    After the initiation, all handles had been retrieved and saved in two Dictionaries
   '''
    swc = C_StockWindowControl()

    # Find the main window handle and reposition the window
    # swc.findMainWindow()
    # Initiate window handles
    # windowsInitiation()

    # findStockWindowByName(2495344, 0, None, windowName)

    # win32gui.EnumChildWindows(_attrs_window['mainWindowHandle'], callbacktest, None)
    sleep(1)
    swc._buy_page()
    # swc.set_stock_code('300218')
    # swc.set_trade_amount('10000')

    sleep(1)
    swc._stock_holding_page()

    # print "waiting for prices"

    # swc.update_quote_price()
    # print 'test of set_stock_code and set_trade_amount', swc._stock_trades

    # sleep(1)
    # swc.set_trade_price(price=20, priceIndex='c0', trade='b')

    # print 'test of set_trade_price', swc._stock_trades

    '''
    Buy/Sale Stock
    buy_stock(_stock_trades)
    sale_stock(_stock_trades)
    '''
    # sleep(1)
    # buyPage()
    # sleep(1)
    # saveStockInfor(None)

    # hwndMainWindowStock = _attrs_window.get('mainWindowHandle',0)

    # print "Stock Holding Information: ", win32gui.GetWindowText(857134)


if __name__ == '__main__':
    main()
