#coding=GBK 

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

import win32gui, win32com.client, win32api, win32con
from win32con import *
from time import sleep
import time
import io
import DBConnector





WM_CHAR = 0x0102

logMesg = 'Log Start'

conf = {'stockInHandFile':'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\stockInHand.csv',
        'outputDir':'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\',
        'installDir':'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\',
        'confFile':'conf.ini',
        'opLog':'operlog.txt',
        'trLog':'tradeLog.txt'}

attrsWindow = {'mainWindowHandle':'0',
               'stockHoldingHandle':'0',
               'cashAvaliableHandle':'0',
               'stockValueHandle':'0',
               'totalAssetHandle':'0',
               'saveAsEditHandle':'0'}

attrsBuyWindow = {'buyStockCodeHandle':'0',
               'buyPriceHandle':'0','buyAmountHandle':'0',
               'buyButtonHandle':'0','buyLastPriceHandle':'0',
               'buyPrice1Handle':'0','buyPrice2Handle':'0'}

attrsSaleWindow =  {'saleStockCodeHandle':'0','salePriceHandle':'0',
                     'saleAmountHandle':'0','saleButtonHandle':'0',
                     'saleLastPriceHandle':'0','salePrice1Handle':'0','salePrice2Handle':'0',}

tradeWindowsProperties = {'mainWindowName':'广发证券核新网上交易系统7.56',
                    'stockCodeWindowXY':[291,114,1032], 
                    'stockPriceWindowXY':[291,150,1033], 
                    'stockAmountWindowXY':[291,186,1034], 
                    'buyWindowName':'买入[B]', 'buyWindowClass':'Button', 
                    'buySaleWindowXY':[315,210,1006],
                    'lastBuyPriceXY':[454,174,1024],
                    'lastSalePriceXY':[562,174,1027],
                    'buy1PriceXY':[454,192,1018], 
                    'buy2PriceXY':[454,206,1025],   
                    'sale1PriceXY':[454,156,1021],
                    'sale2PriceXY':[454,143,1022],
                    'stockHoldingXY':[211,182,1047],
                    'cashAvaliableXY':[274,108,1012],
                    'stockValueXY':[516,126,1014],
                    'totalAssetXY':[517,144,1015],
                    'saveAsEditXY':[271,372,1152]}

stockTrades = {'stockCode':'000003', 'tradeAmount':'1000', 'tradePrice':'1000',
               'buyLastPrice':'0', 'buyPrice1':'0', 'buyPrice2':'0',
               'saleLastPrice':'0','salePrice1':'0', 'salePrice2':'0'} 

assetInfor = {'cashAvaliable':0, 'stockValue':0, 'totalAsset':0}
                            
#tempwindow = win32gui.GetActiveWindow() 

#tempwindow = win32gui.GetWindow(7607276)


def EnumWindowProc(hwnd, tradeCode):    
    if hwnd == 133240 or hwnd == 133236:
        rect = win32gui.GetWindowRect(hwnd)
        x = rect[0]
        y = rect[1]
        #w = rect[2] - x
        #h = rect[3] - y
        print "%\n Windows Handle %d" %hwnd
        print "Windows X %d" %x
        print "Windows y %d" %y
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
        attrsWindow['mainWindowHanle'] = hwnd
    '''
def getHandleByXY(hwnd, x, y, attrs, sizeKey, handleKey):
    if (x == tradeWindowsProperties[sizeKey][0]) and (y == tradeWindowsProperties[sizeKey][1] and attrs[handleKey]=='0'):
        attrs[handleKey] = hwnd
        #print attrs
    else:
        pass

def EnumChildWindowProc(hwnd, tradeCode):    
 
    if tradeCode == 'Buy':
        if win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['stockCodeWindowXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'stockCodeWindowXY'
            handleKey = 'buyStockCodeHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsBuyWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['stockPriceWindowXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'stockPriceWindowXY'
            handleKey = 'buyPriceHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsBuyWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['stockAmountWindowXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'stockAmountWindowXY'
            handleKey = 'buyAmountHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsBuyWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['buySaleWindowXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'buySaleWindowXY'
            handleKey = 'buyButtonHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsBuyWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['lastBuyPriceXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'lastBuyPriceXY'
            handleKey = 'buyLastPriceHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsBuyWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['buy1PriceXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'buy1PriceXY'
            handleKey = 'buyPrice1Handle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsBuyWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['buy2PriceXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'buy2PriceXY'
            handleKey = 'buyPrice2Handle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsBuyWindow, sizeKey, handleKey)
        else:
            pass        
    elif tradeCode=='Sale':
        if win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['stockCodeWindowXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'stockCodeWindowXY'
            handleKey = 'saleStockCodeHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsSaleWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['stockPriceWindowXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'stockPriceWindowXY'
            handleKey = 'salePriceHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsSaleWindow, sizeKey, handleKey)
        
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['stockAmountWindowXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'stockAmountWindowXY'
            handleKey = 'saleAmountHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsSaleWindow, sizeKey, handleKey)
        
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['buySaleWindowXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'buySaleWindowXY'
            handleKey = 'saleButtonHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsSaleWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['lastSalePriceXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'lastSalePriceXY'
            handleKey = 'saleLastPriceHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsSaleWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['sale1PriceXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'sale1PriceXY'
            handleKey = 'salePrice1Handle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsSaleWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['sale2PriceXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'sale2PriceXY'
            handleKey = 'salePrice2Handle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsSaleWindow, sizeKey, handleKey)    
        else:
            pass
    elif tradeCode=='Infor':
        if win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['stockHoldingXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'stockHoldingXY'
            handleKey = 'stockHoldingHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsWindow, sizeKey, handleKey)            
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['cashAvaliableXY'][2]:
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'cashAvaliableXY'
            handleKey = 'cashAvaliableHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsWindow, sizeKey, handleKey)
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['stockValueXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'stockValueXY'
            handleKey = 'stockValueHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsWindow, sizeKey, handleKey)    
        elif win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['totalAssetXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'totalAssetXY'
            handleKey = 'totalAssetHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsWindow, sizeKey, handleKey)
        else:
            pass                 
    elif tradeCode == 'saveAs':
        if win32gui.GetDlgCtrlID(hwnd) == tradeWindowsProperties['saveAsEditXY'][2]: 
            winRec = win32gui.GetWindowRect(hwnd)
            sizeKey = 'saveAsEditXY'
            handleKey = 'saveAsEditHandle'
            getHandleByXY(hwnd, winRec[0], winRec[1],attrsWindow, sizeKey, handleKey)
        else:
            pass                 
    else:
        pass                      
                          
def windowsInitiation():
    
    
    #win32gui.EnumWindows(EnumWindowProc, None)
    #print "Parrent Window Hwnd is %d" %attrsWindow['mainWindowHandle']
    mainWindowHwnd = attrsWindow['mainWindowHandle']
    #secondWindowHwnd = getHandleByControlID(mainWindowHwnd, controlID)
    setForeGroundWindow(mainWindowHwnd)
    
    '''
    Initiate Buy/Sale window handles
    '''
    try: 
        buyPage()
        win32gui.EnumChildWindows(mainWindowHwnd, EnumChildWindowProc, 'Buy')
        print attrsBuyWindow
        
        salePage()
        win32gui.EnumChildWindows(mainWindowHwnd, EnumChildWindowProc, 'Sale')
        print attrsSaleWindow
        
        '''
        Initiate Stock Holding and Capital Information
        '''
        stockHoldingPage() 
        win32gui.EnumChildWindows(mainWindowHwnd, EnumChildWindowProc, 'Infor')
        print attrsWindow
        
        if saveStockInforToFile(mainWindowHwnd) == False: 
            logMesg = '\n Sorry, system can not initialize Trading Software at ' + timeTag()
        else:
            logMesg = '\n Yes, system initialized Trading Software successfully at ' + timeTag()
        
        updateAsset()
        print assetInfor
        
        updateQuotePrice()
        print stockTrades
                        
    except:
        logMesg = '\n Sorry, system can not initialize Trading Software at ' + timeTag()
    
    print logMesg
    writelog(logMesg) 

def closeStockWindow():
    
    win32api.keybd_event(VK_MENU,0,0,0)
    
    win32api.keybd_event(115,0,0,0)
    win32api.keybd_event(115,0,win32con.KEYEVENTF_KEYUP,0) #閲婃斁鎸夐敭
    win32api.keybd_event(VK_MENU,win32con.KEYEVENTF_KEYUP,0)
    
def buyPage():
    #win32gui.SetForeGroundWindow(mainWindowHwnd)
    # Active Buy Window
    win32api.keybd_event(VK_F1,0,0,0) #閲婃斁鎸夐敭
    win32api.keybd_event(VK_F1,0,win32con.KEYEVENTF_KEYUP,0)
    sleep(1)   

def salePage():
    #setForeGroundWindow(mainWindowHwnd)
    #win32gui.SetForegroundWindow(mainWindowHwnd)
    # Active Buy Window
    win32api.keybd_event(VK_F2,0,0,0) #閲婃斁鎸夐敭
    win32api.keybd_event(VK_F2,0,win32con.KEYEVENTF_KEYUP,0)
    sleep(1)  
    
def stockHoldingPage():
    #win32gui.SetForeGroundWindow(mainWindowHwnd)    
    # Active Infor Window
    win32api.keybd_event(VK_F4,0,0,0) #閲婃斁鎸夐敭
    win32api.keybd_event(VK_F4,0,win32con.KEYEVENTF_KEYUP,0)
    sleep(1) 
    
def buyStock(stockTrades):
    
    stockCode = stockTrades['stockCode']
    tradeAmount = stockTrades['tradeAmount']
    tradePrice = stockTrades['tradePrice']
    
    # Active Buy Window
    buyPage()
    
    try:
        #Send stockCode
        for char in stockCode:
            win32api.SendMessage(attrsBuyWindow['buyStockCodeHandle'], win32con.WM_CHAR, ord(char), None)
        print "message sent"
    
        #Send tradeAmount
        for char in tradeAmount:
            win32api.SendMessage(attrsBuyWindow['buyAmountHandle'], win32con.WM_CHAR, ord(char), None)
        print "Amount sent"
        
        #Send tradePrice
        for char in tradePrice:
            win32api.SendMessage(attrsBuyWindow['buyPriceHandle'], win32con.WM_CHAR, ord(char), None)
        print "Price sent"   
    
        #Send Sale Command
        sleep(1)    
        win32api.keybd_event(66,0,0,0)
        win32api.keybd_event(66,0,win32con.KEYEVENTF_KEYUP,0) #閲婃斁鎸夐敭
        
        #Send Y to Confirm the buy
        sleep(1)
        win32api.keybd_event(89,0,0,0)
        win32api.keybd_event(89,0,win32con.KEYEVENTF_KEYUP,0)
        
        #Send Enter to confirm any message
        sleep(1)
        win32api.keybd_event(VK_RETURN, 0, 0, 0)
        win32api.keybd_event(VK_RETURN,0,win32con.KEYEVENTF_KEYUP,0)
        logMesg = '\n Congratulation, system issue a BUY command of stock code %s, buy price %s, buy amount %s successfully.'%(stockCode, tradePrice, tradeAmount)
    except:
        logMesg = '\n Sorry, system cannot issue a BUY command of stock code %s, buy price %s, buy amount %s'%(stockCode, tradePrice, tradeAmount)
    #write to log file
    writelog(logMesg, conf['trLog'])
    
def saleStock(stockTrades):
    
    stockCode = stockTrades['stockCode']
    tradeAmount = stockTrades['tradeAmount']
    tradePrice = stockTrades['tradePrice']
    
    # Active Buy Window
    salePage()  
    
    try:
        #Send stockCode
        for char in stockCode:
            win32api.SendMessage(attrsSaleWindow['saleStockCodeHandle'], win32con.WM_CHAR, ord(char), None)
        print "message sent"
    
        #Send tradeAmount
        for char in tradeAmount:
            win32api.SendMessage(attrsSaleWindow['saleAmountHandle'], win32con.WM_CHAR, ord(char), None)
        print "Amount sent"
        
        #Send tradePrice
        for char in tradePrice:
            win32api.SendMessage(attrsSaleWindow['salePriceHandle'], win32con.WM_CHAR, ord(char), None)
        print "Price sent"   
        
        #Send Buy Command
        sleep(1)    
        win32api.keybd_event(83,0,0,0)
        win32api.keybd_event(83,0,win32con.KEYEVENTF_KEYUP,0) #閲婃斁鎸夐敭
        
        #Send Y to Confirm the Sale
        sleep(1)
        win32api.keybd_event(89,0,0,0)
        win32api.keybd_event(89,0,win32con.KEYEVENTF_KEYUP,0)
        
        #Send Enter to confirm any message
        sleep(1)
        win32api.keybd_event(VK_RETURN, 0, 0, 0)
        win32api.keybd_event(VK_RETURN,0,win32con.KEYEVENTF_KEYUP,0)
        
        logMesg = '\n Congratulation, system issue a SALE command of stock code %s, buy price %s, buy amount %s successfully.'%(stockCode, tradePrice, tradeAmount)
    except:
        logMesg = '\n Sorry, system cannot issue a SALE command of stock code %s, buy price %s, buy amount %s'%(stockCode, tradePrice, tradeAmount)
    #write to log file
    writelog(logMesg, conf['trLog'])
     
def setTradePrice(price, priceIndex, trade = 'b'):
    
    price = str(price)
    if trade == 's':
        salePage()
    else:
        buyPage()
        
    if priceIndex == 'b1':
        stockTrades['tradePrice'] = stockTrades['buyPrice1']
    elif priceIndex == 'b2':
        stockTrades['tradePrice'] = stockTrades['buyPrice2']
    elif priceIndex == 'b0':
        stockTrades['tradePrice'] = stockTrades['buyLastPrice']
    elif priceIndex == 's1':
        stockTrades['tradePrice'] = stockTrades['salePrice2']
    elif priceIndex == 's2':
        stockTrades['tradePrice'] = stockTrades['salePrice2']
    elif priceIndex == 's0':
        stockTrades['tradePrice'] = stockTrades['saleLastPrice']
    elif priceIndex == 'c0':
        stockTrades['tradePrice'] = price
    else:
        pass
        
def setStockCode(stockCode='000003', trade = 'b'):
    
    if trade == 's':
        salePage()
    else:
        buyPage()
        
    if len(stockCode) == 6:
        stockTrades['stockCode'] = stockCode
        logMesg = '\n Set stock code to %s at ' %stockCode , timeTag()
        for char in stockCode:
            win32api.SendMessage(attrsBuyWindow['buyStockCodeHandle'], win32con.WM_CHAR, ord(char), None)
    else:
        logMesg = '\n ERROR: Set a wrong stock code %s at ' %stockCode, timeTag()
    

        
    writelog(logMesg)    
    
def setTradeAmount(tradeAmount = '1000', trade ='b'):
    if trade == 's':
        salePage()
    else:
        buyPage()
    
    if int(tradeAmount) <= 0:
        logMesg = '\n ERROR: System cannot take a nagtive trade amount'
    else:
        stockTrades['tradeAmount'] = tradeAmount
        logMesg = '\n System set tradeAmount to %s' %tradeAmount
        #Send tradeAmount
        for char in tradeAmount:
            win32api.SendMessage(attrsBuyWindow['buyAmountHandle'], win32con.WM_CHAR, ord(char), None)


    writelog(logMesg)
    
def updateQuotePrice():
    buyPage()
    
    stockTrades['buyLastPrice'] = win32gui.GetWindowText(attrsBuyWindow['buyLastPriceHandle'])
    stockTrades['buyPrice1'] = win32gui.GetWindowText(attrsBuyWindow['buyPrice1Handle'])
    stockTrades['buyPrice2'] = win32gui.GetWindowText(attrsBuyWindow['buyPrice2Handle'])
    
    stockTrades['saleLastPrice'] = win32gui.GetWindowText(attrsSaleWindow['saleLastPriceHandle'])
    stockTrades['salePrice1'] = win32gui.GetWindowText(attrsSaleWindow['salePrice1Handle'])
    stockTrades['salePrice2'] = win32gui.GetWindowText(attrsSaleWindow['salePrice2Handle'])

def updateAsset():
    '''
    Read Asset information from the external file
    '''
    stockHoldingPage()
    assetInfor['cashAvaliable'] = win32gui.GetWindowText(attrsWindow['cashAvaliableHandle'])
    assetInfor['stockValue'] = win32gui.GetWindowText(attrsWindow['stockValueHandle'])
    assetInfor['totalAsset'] = win32gui.GetWindowText(attrsWindow['totalAssetHandle'])
    

def getMenu(hwnd):
    print win32gui.GetMenu(hwnd)

def setForeGroundWindow(hwnd):
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys('%')
    win32gui.SetForegroundWindow(hwnd)

def setActiveWindow2():
    #win32gui.MoveWindow(hwnd, 0,0,800,600, True)
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys('%')
    win32gui.SetForegroundWindow(hwnd)

def setWindowTop(hwnd):

   print hwnd, " Move Window"
   win32gui.MoveWindow(hwnd, 0,0,800,600, True)
   #win32gui.BringWindowToTop(hwnd)
   
def findWindowByNameEx(parentHWND, childAfter, windowClass, windowName):
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys('%')
    hwnd = win32gui.FindWindowEx(parentHWND, childAfter, windowClass, windowName)

    print "find Window Finished",windowName, " " ,hwnd
    
def findMainWindow(windowName = tradeWindowsProperties['mainWindowName']):
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys('%')
    hwnd = win32gui.FindWindow(None, windowName)
    win32gui.MoveWindow(hwnd, 0,0,800,600, True)
    attrsWindow['mainWindowHandle'] = hwnd

def findWindowByName(windowName):
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys('%')
    hwnd = win32gui.FindWindow(None, windowName)
    return hwnd
        
def getActiveWindow():
    hwnd = win32gui.GetForegroundWindow()
    print hwnd
    return hwnd
    
def getHandleByControlID(parentHwnd, controlID):
    #print win32gui.GetDlgCtrlID(controlID)
    print win32gui.GetDlgItem(parentHwnd, controlID)
    
def writelog(logMesg = logMesg, logPath = conf['opLog']):
    fullPath = conf['outputDir'] + logPath
    with open(fullPath, 'a') as log:
        log.writelines(logMesg)
    
def saveStockInforToFile(hwnd):
    
    filename = conf['stockInHandFile']    
    #windowName = 'Save As'
    #dirWindowClass = 'Edit'
    
    assetInfor['cashAvaliable'] = win32gui.GetWindowText(attrsWindow['cashAvaliableHandle'])
    assetInfor['stockValue'] = win32gui.GetWindowText(attrsWindow['stockValueHandle'])
    assetInfor['totalAsset'] = win32gui.GetWindowText(attrsWindow['totalAssetHandle'])
    print assetInfor
    #stockHoldingPage()
    try:
        win32api.keybd_event(VK_CONTROL,0,0,0)
        sleep(1)
        win32api.keybd_event(83,0,0,0)
        win32api.keybd_event(83,0,win32con.KEYEVENTF_KEYUP,0)
        sleep(1)
        win32api.keybd_event(VK_CONTROL,0,win32con.KEYEVENTF_KEYUP,0)
    
        sleep(1)
        hwndPop = findWindowByName('Save As')
        #print hwndPop
        
        win32gui.EnumChildWindows(hwndPop, EnumChildWindowProc,'saveAs')
        #dirWindowHandle = findWindowByNameEx(hwndPop, None, None, dirWindowName)
        #dirWindowHandle = findWindowByName(dirWindowName)
        dirWindowHandle = attrsWindow['saveAsEditHandle']
        #print dirWindowHandle
        
        for char in filename:
            win32api.SendMessage(dirWindowHandle, win32con.WM_CHAR, ord(char), None)
        
        sleep(1)
        
        win32api.keybd_event(VK_MENU,0,0,0)
        sleep(1)
        win32api.keybd_event(83,0,0,0)
        win32api.keybd_event(83,0,win32con.KEYEVENTF_KEYUP,0)
        sleep(1)
        win32api.keybd_event(VK_MENU,0,win32con.KEYEVENTF_KEYUP,0)
        
        sleep(1)
        win32api.keybd_event(VK_MENU,0,0,0)
        win32api.keybd_event(89,0,0,0)
        win32api.keybd_event(89,0,win32con.KEYEVENTF_KEYUP,0)
        win32api.keybd_event(VK_MENU,0,win32con.KEYEVENTF_KEYUP,0)
        
        logMesg = '\n Congratulation, system saved stocks in hand message successfully at ' + timeTag()
        saved = True
    except:
        logMesg = '\n Sorry, system cannot save stocks in hand message at \n' + timeTag()
        saved = False
    
    writelog(logMesg)
    print logMesg
    return saved        

    
def timeTag():
    return time.asctime(time.localtime(time.time()))

def main():
    

    '''
    Initiate the window handles of Stock Sale Page and Stock Buy Page
    After the initiation, all handles had been retrieved and saved in two Dictionaries
   '''     
    # Find the main window handle and reposition the window   
    findMainWindow() 
    #Initiate window handles
    windowsInitiation()
    
    windowName = '参考总资产（含港股通）'
    #findStockWindowByName(2495344, 0, None, windowName)
    
    #win32gui.EnumChildWindows(attrsWindow['mainWindowHandle'], callbacktest, None)
    sleep(1)
    
    setStockCode('300218')
    setTradeAmount('10000')
    sleep(3)
    
    print "waiting for prices"
    
    updateQuotePrice()
    print 'test of setStockCode and setTradeAmount', stockTrades
    
    sleep(1)
    setTradePrice(price = 20, priceIndex='c0', trade= 'b')
    
    print 'test of setTradePrice', stockTrades   
       
    '''
    Buy/Sale Stock
    buyStock(stockTrades)
    saleStock(stockTrades)
    '''
    #sleep(1)
    #buyPage()
    #sleep(1)
    #saveStockInfor(None)
    
    #hwndMainWindowStock = attrsWindow.get('mainWindowHandle',0)
      
    #print "Stock Holding Information: ", win32gui.GetWindowText(857134)
        
    
if __name__ == '__main__':
    main()
