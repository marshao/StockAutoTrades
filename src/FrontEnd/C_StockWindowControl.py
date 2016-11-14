#coding=GBK

import win32gui, win32com.client, win32api, win32con
from win32con import *
from time import sleep
import time
import io
import src.DBConnector


__metclass__ = type

class C_StockWindowControl:
    """
    20161102 A WindowControl Class will be build in here, from this class, I should be able to get the attributes of XiaDan window
    and be able to toggle few actions to the window.
    Class1: windowClass
        Attr1: windowName
        Attr2: windowSize
        Attr2: windowTitle

        Act1: setWindowActive
        Act2: setWindowInactive
        Act3: resizeWindow to normal window
        Act4: clsoeWindow

    Class2: buttonClass
        Attr1: buttonTitle
        Attr2: buttonName
        Attr3: buttonPosition

        Act1: hitButton
        Act2: setButtonActive

    Class3: inputBoxClass
        Attr1: inputBoxTitle
        Attr2: inputBoxName
        Attr3: inputBoxPosition

        Act1: setInputBoxActive
        Act2: inputText
    """

    WM_CHAR = 0x0102

    __logMesg__ = 'Log Start'

    __conf__ = {
        'stockInHandFile': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\stockInHand.csv',
        'outputDir': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\',
        'installDir': 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\',
        'confFile': '__conf__.ini',
        'opLog': 'operlog.txt',
        'trLog': 'tradeLog.txt'}

    __attrsWindow__ = {'mainWindowHandle': '0',
                   'stockHoldingHandle': '0',
                   'cashAvaliableHandle': '0',
                   'stockValueHandle': '0',
                   'totalAssetHandle': '0',
                   'saveAsEditHandle': '0'}

    __attrsBuyWindow__ = {'buyStockCodeHandle': '0',
                      'buyPriceHandle': '0', 'buyAmountHandle': '0',
                      'buyButtonHandle': '0', 'buyLastPriceHandle': '0',
                      'buyPrice1Handle': '0', 'buyPrice2Handle': '0'}

    __attrsSaleWindow__ = {'saleStockCodeHandle': '0', 'salePriceHandle': '0',
                       'saleAmountHandle': '0', 'saleButtonHandle': '0',
                       'saleLastPriceHandle': '0', 'salePrice1Handle': '0', 'salePrice2Handle': '0', }

    __tradeWindowsProperties__ = {'mainWindowName': '广发证券核新网上交易系统7.56',
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

    __stockTrades__ = {'stockCode': '000003', 'tradeAmount': '1000', 'tradePrice': '1000',
                   'buyLastPrice': '0', 'buyPrice1': '0', 'buyPrice2': '0',
                   'saleLastPrice': '0', 'salePrice1': '0', 'salePrice2': '0'}

    __assetInfor__ = {'cashAvaliable': 0, 'stockValue': 0, 'totalAsset': 0}

    # tempwindow = win32gui.GetActiveWindow()

    # tempwindow = win32gui.GetWindow(7607276)

    def __init__(self):

        self.__findMainWindow__()
        # win32gui.EnumWindows(EnumWindowProc, None)
        # print "Parrent Window Hwnd is %d" %__attrsWindow__['mainWindowHandle']
        mainWindowHwnd = self.__attrsWindow__['mainWindowHandle']
        # secondWindowHwnd = getHandleByControlID(mainWindowHwnd, controlID)
        self.__setForeGroundWindow__(mainWindowHwnd)

        '''
        Initiate Buy/Sale window handles
        '''
        try:

            self.__buyPage__()
            win32gui.EnumChildWindows(mainWindowHwnd, self.__EnumChildWindowProc__, 'Buy')
            print self.__attrsBuyWindow__

            self.__salePage__()
            win32gui.EnumChildWindows(mainWindowHwnd, self.__EnumChildWindowProc__, 'Sale')
            print self.__attrsSaleWindow__

            '''
            Initiate Stock Holding and Capital Information
            '''
            self.__stockHoldingPage__()
            win32gui.EnumChildWindows(mainWindowHwnd, self.__EnumChildWindowProc__, 'Infor')
            print self.__attrsWindow__

            if self.__saveStockInforToFile__(mainWindowHwnd) == False:
                self.__logMesg__ = '\n Sorry, system can not initialize Trading Software at ' + self.__timeTag__()
            else:
                self.__logMesg__ = '\n Yes, system initialized Trading Software successfully at ' + self.__timeTag__()

            self.updateAsset()
            print self.__assetInfor__

            self.updateQuotePrice()
            print self.__stockTrades__
        except:
            self.__logMesg__ = '\n Sorry, system can not initialize Trading Software at ' + self.__timeTag__()

        print self.__logMesg__
        self.__writelog__(self.__logMesg__)

    def __EnumWindowProc__(self, hwnd, tradeCode):
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
            self.__attrsWindow__['mainWindowHanle'] = hwnd
        '''

    def __getHandleByXY__(self, hwnd, x, y, attrs, sizeKey, handleKey):
        if (x == self.__tradeWindowsProperties__[sizeKey][0]) and (
                y == self.__tradeWindowsProperties__[sizeKey][1] and attrs[handleKey] == '0'):
            attrs[handleKey] = hwnd
            # print attrs
        else:
            pass

    def __EnumChildWindowProc__(self, hwnd, tradeCode):

        if tradeCode == 'Buy':
            if win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['stockCodeWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockCodeWindowXY'
                handleKey = 'buyStockCodeHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsBuyWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['stockPriceWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockPriceWindowXY'
                handleKey = 'buyPriceHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsBuyWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['stockAmountWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockAmountWindowXY'
                handleKey = 'buyAmountHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsBuyWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['buySaleWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'buySaleWindowXY'
                handleKey = 'buyButtonHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsBuyWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['lastBuyPriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'lastBuyPriceXY'
                handleKey = 'buyLastPriceHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsBuyWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['buy1PriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'buy1PriceXY'
                handleKey = 'buyPrice1Handle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsBuyWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['buy2PriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'buy2PriceXY'
                handleKey = 'buyPrice2Handle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsBuyWindow__, sizeKey, handleKey)
            else:
                pass
        elif tradeCode == 'Sale':
            if win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['stockCodeWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockCodeWindowXY'
                handleKey = 'saleStockCodeHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsSaleWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['stockPriceWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockPriceWindowXY'
                handleKey = 'salePriceHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsSaleWindow__, sizeKey, handleKey)

            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['stockAmountWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockAmountWindowXY'
                handleKey = 'saleAmountHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsSaleWindow__, sizeKey, handleKey)

            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['buySaleWindowXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'buySaleWindowXY'
                handleKey = 'saleButtonHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsSaleWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['lastSalePriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'lastSalePriceXY'
                handleKey = 'saleLastPriceHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsSaleWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['sale1PriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'sale1PriceXY'
                handleKey = 'salePrice1Handle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsSaleWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['sale2PriceXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'sale2PriceXY'
                handleKey = 'salePrice2Handle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsSaleWindow__, sizeKey, handleKey)
            else:
                pass
        elif tradeCode == 'Infor':
            if win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['stockHoldingXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockHoldingXY'
                handleKey = 'stockHoldingHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['cashAvaliableXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'cashAvaliableXY'
                handleKey = 'cashAvaliableHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['stockValueXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'stockValueXY'
                handleKey = 'stockValueHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsWindow__, sizeKey, handleKey)
            elif win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['totalAssetXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'totalAssetXY'
                handleKey = 'totalAssetHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsWindow__, sizeKey, handleKey)
            else:
                pass
        elif tradeCode == 'saveAs':
            if win32gui.GetDlgCtrlID(hwnd) == self.__tradeWindowsProperties__['saveAsEditXY'][2]:
                winRec = win32gui.GetWindowRect(hwnd)
                sizeKey = 'saveAsEditXY'
                handleKey = 'saveAsEditHandle'
                self.__getHandleByXY__(hwnd, winRec[0], winRec[1], self.__attrsWindow__, sizeKey, handleKey)
            else:
                pass
        else:
            pass



    def __closeStockWindow__(self):

        win32api.keybd_event(VK_MENU, 0, 0, 0)

        win32api.keybd_event(115, 0, 0, 0)
        win32api.keybd_event(115, 0, win32con.KEYEVENTF_KEYUP, 0)  # 閲婃斁鎸夐敭
        win32api.keybd_event(VK_MENU, win32con.KEYEVENTF_KEYUP, 0)

    def __buyPage__(self):
        # win32gui.SetForeGroundWindow(mainWindowHwnd)
        # Active Buy Window
        win32api.keybd_event(VK_F1, 0, 0, 0)  # 閲婃斁鎸夐敭
        win32api.keybd_event(VK_F1, 0, win32con.KEYEVENTF_KEYUP, 0)
        sleep(1)

    def __salePage__(self):
        # setForeGroundWindow(mainWindowHwnd)
        # win32gui.SetForegroundWindow(mainWindowHwnd)
        # Active Buy Window
        win32api.keybd_event(VK_F2, 0, 0, 0)  # 閲婃斁鎸夐敭
        win32api.keybd_event(VK_F2, 0, win32con.KEYEVENTF_KEYUP, 0)
        sleep(1)

    def __stockHoldingPage__(self):
        # win32gui.SetForeGroundWindow(mainWindowHwnd)
        # Active Infor Window
        win32api.keybd_event(VK_F4, 0, 0, 0)  # 閲婃斁鎸夐敭
        win32api.keybd_event(VK_F4, 0, win32con.KEYEVENTF_KEYUP, 0)
        sleep(1)

    def buyStock(self, stockTrades):

        stockCode = stockTrades['stockCode']
        tradeAmount = stockTrades['tradeAmount']
        tradePrice = stockTrades['tradePrice']

        # Active Buy Window
        self.__buyPage__()

        try:
            # Send stockCode
            for char in stockCode:
                win32api.SendMessage(self.__attrsBuyWindow__['buyStockCodeHandle'], win32con.WM_CHAR, ord(char), None)
            print "message sent"

            # Send tradeAmount
            for char in tradeAmount:
                win32api.SendMessage(self.__attrsBuyWindow__['buyAmountHandle'], win32con.WM_CHAR, ord(char), None)
            print "Amount sent"

            # Send tradePrice
            for char in tradePrice:
                win32api.SendMessage(self.__attrsBuyWindow__['buyPriceHandle'], win32con.WM_CHAR, ord(char), None)
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
            self.__logMesg__ = '\n Congratulation, system issue a BUY command of stock code %s, buy price %s, buy amount %s successfully.' % (
            stockCode, tradePrice, tradeAmount)
        except:
            self.__logMesg__ = '\n Sorry, system cannot issue a BUY command of stock code %s, buy price %s, buy amount %s' % (
            stockCode, tradePrice, tradeAmount)
        # write to log file
            self.__writelog__(self.__logMesg__, self.__conf__['trLog'])

    def saleStock(self, stockTrades):

        stockCode = stockTrades['stockCode']
        tradeAmount = stockTrades['tradeAmount']
        tradePrice = stockTrades['tradePrice']

        # Active Buy Window
        self.__salePage__()

        try:
            # Send stockCode
            for char in stockCode:
                win32api.SendMessage(self.__attrsSaleWindow__['saleStockCodeHandle'], win32con.WM_CHAR, ord(char), None)
            print "message sent"

            # Send tradeAmount
            for char in tradeAmount:
                win32api.SendMessage(self.__attrsSaleWindow__['saleAmountHandle'], win32con.WM_CHAR, ord(char), None)
            print "Amount sent"

            # Send tradePrice
            for char in tradePrice:
                win32api.SendMessage(self.__attrsSaleWindow__['salePriceHandle'], win32con.WM_CHAR, ord(char), None)
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

            self.__logMesg__ = '\n Congratulation, system issue a SALE command of stock code %s, buy price %s, buy amount %s successfully.' % (
            stockCode, tradePrice, tradeAmount)
        except:
            self.__logMesg__ = '\n Sorry, system cannot issue a SALE command of stock code %s, buy price %s, buy amount %s' % (
            stockCode, tradePrice, tradeAmount)
        # write to log file
            self.__writelog__(self.__logMesg__, self.__conf__['trLog'])

    def setTradePrice(self, price, priceIndex, trade='b'):

        price = str(price)
        if trade == 's':
            self.__salePage__()
        else:
            self.__buyPage__()

        if priceIndex == 'b1':
            self.__stockTrades__['tradePrice'] = self.__stockTrades__['buyPrice1']
        elif priceIndex == 'b2':
            self.__stockTrades__['tradePrice'] = self.__stockTrades__['buyPrice2']
        elif priceIndex == 'b0':
            self.__stockTrades__['tradePrice'] = self.__stockTrades__['buyLastPrice']
        elif priceIndex == 's1':
            self.__stockTrades__['tradePrice'] = self.__stockTrades__['salePrice2']
        elif priceIndex == 's2':
            self.__stockTrades__['tradePrice'] = self.__stockTrades__['salePrice2']
        elif priceIndex == 's0':
            self.__stockTrades__['tradePrice'] = self.__stockTrades__['saleLastPrice']
        elif priceIndex == 'c0':
            self.__stockTrades__['tradePrice'] = price
        else:
            pass

    def setStockCode(self, stockCode='000003', trade='b'):

        if trade == 's':
            self.__salePage__()
        else:
            self.__buyPage__()

        if len(stockCode) == 6:
            self.__stockTrades__['stockCode'] = stockCode
            self.__logMesg__ = '\n Set stock code to %s ' % stockCode, " at ",self.__timeTag__()
            for char in stockCode:
                win32api.SendMessage(self.__attrsBuyWindow__['buyStockCodeHandle'], win32con.WM_CHAR, ord(char), None)
        else:
            self.__logMesg__ = '\n ERROR: Set a wrong stock code %s ' % stockCode," at ", self.__timeTag__()

            self.__writelog__(self.__logMesg__)

    def setTradeAmount(self, tradeAmount='1000', trade='b'):
        if trade == 's':
            self.__salePage__()
        else:
            self.__buyPage__()

        if int(tradeAmount) <= 0:
            self.__logMesg__ = '\n ERROR: System cannot take a nagtive trade amount'
        else:
            self.__stockTrades__['tradeAmount'] = tradeAmount
            self.__logMesg__ = '\n System set tradeAmount to %s' % tradeAmount
            # Send tradeAmount
            for char in tradeAmount:
                win32api.SendMessage(self.__attrsBuyWindow__['buyAmountHandle'], win32con.WM_CHAR, ord(char), None)

                self.__writelog__(self.__logMesg__)

    def updateQuotePrice(self):
        self.__buyPage__()

        self.__stockTrades__['buyLastPrice'] = win32gui.GetWindowText(self.__attrsBuyWindow__['buyLastPriceHandle'])
        self.__stockTrades__['buyPrice1'] = win32gui.GetWindowText(self.__attrsBuyWindow__['buyPrice1Handle'])
        self.__stockTrades__['buyPrice2'] = win32gui.GetWindowText(self.__attrsBuyWindow__['buyPrice2Handle'])

        self.__stockTrades__['saleLastPrice'] = win32gui.GetWindowText(self.__attrsSaleWindow__['saleLastPriceHandle'])
        self.__stockTrades__['salePrice1'] = win32gui.GetWindowText(self.__attrsSaleWindow__['salePrice1Handle'])
        self.__stockTrades__['salePrice2'] = win32gui.GetWindowText(self.__attrsSaleWindow__['salePrice2Handle'])

    def updateAsset(self):
        '''
        Read Asset information from the external file
        '''
        self.__stockHoldingPage__()
        self.__assetInfor__['cashAvaliable'] = win32gui.GetWindowText(self.__attrsWindow__['cashAvaliableHandle'])
        self.__assetInfor__['stockValue'] = win32gui.GetWindowText(self.__attrsWindow__['stockValueHandle'])
        self.__assetInfor__['totalAsset'] = win32gui.GetWindowText(self.__attrsWindow__['totalAssetHandle'])

    def __getMenu__(self, hwnd):
        print win32gui.GetMenu(hwnd)

    def __setForeGroundWindow__(self, hwnd):
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        win32gui.SetForegroundWindow(hwnd)

    def __setActiveWindow2__(self, hwnd):
        # win32gui.MoveWindow(hwnd, 0,0,800,600, True)
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        win32gui.SetForegroundWindow(hwnd)

    def __setWindowTop__(self, hwnd):

        print hwnd, " Move Window"
        win32gui.MoveWindow(hwnd, 0, 0, 800, 600, True)
        # win32gui.BringWindowToTop(hwnd)

    def __findWindowByNameEx__(self, parentHWND, childAfter, windowClass, windowName):
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        hwnd = win32gui.FindWindowEx(parentHWND, childAfter, windowClass, windowName)

        print "find Window Finished", windowName, " ", hwnd

    def __findMainWindow__(self, windowName=__tradeWindowsProperties__['mainWindowName']):
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        hwnd = win32gui.FindWindow(None, windowName)
        win32gui.MoveWindow(hwnd, 0, 0, 800, 600, True)
        self.__attrsWindow__['mainWindowHandle'] = hwnd

    def __findWindowByName__(self, windowName):
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.SendKeys('%')
        hwnd = win32gui.FindWindow(None, windowName)
        return hwnd

    def __getActiveWindow__(self):
        hwnd = win32gui.GetForegroundWindow()
        print hwnd
        return hwnd

    def __getHandleByControlID__(self, parentHwnd, controlID):
        # print win32gui.GetDlgCtrlID(controlID)
        print win32gui.GetDlgItem(parentHwnd, controlID)

    def __writelog__(self, logMesg = __logMesg__, logPath=__conf__['opLog']):
        fullPath = self.__conf__['outputDir'] + logPath
        with open(fullPath, 'a') as log:
            log.writelines(logMesg)

    def __saveStockInforToFile__(self, hwnd):

        filename = self.__conf__['stockInHandFile']
        # windowName = 'Save As'
        # dirWindowClass = 'Edit'

        self.__assetInfor__['cashAvaliable'] = win32gui.GetWindowText(self.__attrsWindow__['cashAvaliableHandle'])
        self.__assetInfor__['stockValue'] = win32gui.GetWindowText(self.__attrsWindow__['stockValueHandle'])
        self.__assetInfor__['totalAsset'] = win32gui.GetWindowText(self.__attrsWindow__['totalAssetHandle'])
        print self.__assetInfor__
        # stockHoldingPage()
        try:
            win32api.keybd_event(VK_CONTROL, 0, 0, 0)
            sleep(1)
            win32api.keybd_event(83, 0, 0, 0)
            win32api.keybd_event(83, 0, win32con.KEYEVENTF_KEYUP, 0)
            sleep(1)
            win32api.keybd_event(VK_CONTROL, 0, win32con.KEYEVENTF_KEYUP, 0)

            sleep(1)
            hwndPop = self.__findWindowByName__('Save As')
            # print hwndPop

            win32gui.EnumChildWindows(self, hwndPop, self.__EnumChildWindowProc__, 'saveAs')
            # dirWindowHandle = findWindowByNameEx(hwndPop, None, None, dirWindowName)
            # dirWindowHandle = findWindowByName(dirWindowName)
            dirWindowHandle = self.__attrsWindow__['saveAsEditHandle']
            # print dirWindowHandle

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

            self.__logMesg__ = '\n Congratulation, system saved stocks in hand message successfully at ' + self.__timeTag__()
            saved = True
        except:
            self.__logMesg__ = '\n Sorry, system cannot save stocks in hand message at \n' + self.__timeTag__()
            saved = False

            self.__writelog__(self.__logMesg__)
        print self.__logMesg__
        return saved

    def __timeTag__(self):
        return time.asctime(time.localtime(time.time()))


def main():
    '''
    Initiate the window handles of Stock Sale Page and Stock Buy Page
    After the initiation, all handles had been retrieved and saved in two Dictionaries
   '''
    swc = C_StockWindowControl()

    # Find the main window handle and reposition the window
    #swc.findMainWindow()
    # Initiate window handles
    #windowsInitiation()

    # findStockWindowByName(2495344, 0, None, windowName)

    # win32gui.EnumChildWindows(__attrsWindow__['mainWindowHandle'], callbacktest, None)
    sleep(1)

    swc.setStockCode('300218')
    swc.setTradeAmount('10000')
    sleep(3)

    print "waiting for prices"

    swc.updateQuotePrice()
    print 'test of setStockCode and setTradeAmount', swc.__stockTrades__

    sleep(1)
    swc.setTradePrice(price=20, priceIndex='c0', trade='b')

    print 'test of setTradePrice', swc.__stockTrades__

    '''
    Buy/Sale Stock
    buyStock(__stockTrades__)
    saleStock(__stockTrades__)
    '''
    # sleep(1)
    # buyPage()
    # sleep(1)
    # saveStockInfor(None)

    # hwndMainWindowStock = __attrsWindow__.get('mainWindowHandle',0)

    # print "Stock Holding Information: ", win32gui.GetWindowText(857134)


if __name__ == '__main__':
    main()