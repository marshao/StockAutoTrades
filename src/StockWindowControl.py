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
from lib2to3.fixer_util import Attr
from wx.html import HtmlWindowNameStr
from wx._dataview import DataViewVirtualListModel_GetAttrByRow
WM_CHAR = 0x0102


attrsWindow = {'mainWindowHandle':'0'}

attrBuyWindow = {'buyStockCodeHandle':'0',
               'buyPriceHandle':'0','buyAmoutHandle':'0',
               'buyButtonHandle':'0','buyLastPriceHandle':'0',
               'buyPrice1Handle':'0','buyPrice2Handle':'0'}

attrsSalesWindow =  {'saleStockCodeHandle':'0','salePriceHandle':'0',
                     'saleAmountHandle':'0','saleButtonHandle':'0',
                     'saleLastPriceHandle':'0','salePrice1Handle':'0','salePrice2Handle':'0',}

tradeWindows = {'mainWindowName':['广发证券核新网上交易系统7.56','Afx:400000:b:10005:6:3095b'],
                    'stockCodeWindowXY':['291','114'], 
                    'stockPriceWindowXY':['291','150'], 
                    'stockAmountWindowXY':['291','186'], 
                    'buyWindowName':'买入[B]', 'buyWindowClass':'Button', 
                    'buySaleWindowXY':['315','210'],
                    'lastPriceXY':['454','174'],
                    'buy1PriceXY':['454','192'], 
                    'buy2PriceXY':['454','206'], 
                    'sale2PriceXY':['454','156'],
                    'sale2PriceXY':['454','143']}

#tempwindow = win32gui.GetActiveWindow() 

#tempwindow = win32gui.GetWindow(7607276)


def callback(hwnd, tradeCode):
    
    rect = win32gui.GetWindowRect(hwnd)
    x = rect[0]
    y = rect[1]
    #w = rect[2] - x
    #h = rect[3] - y
    print "Windows Handle %d" %hwnd
    print "Windows X %d" %x
    print "Windows y %d" %y
    print "Windows Title ", win32gui.GetWindowText(hwnd)
    
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
    
    
           
def windowsInitiation():
    buyStock(None)
    win32gui.EnumChildWindows(attrsWindow.get('mainWindowHandle'), callback, 'Buy')
    
    # Initiate Buy Window
    
    
    


def closeStockWindow():
    
    win32api.keybd_event(VK_MENU,0,0,0)
    
    win32api.keybd_event(115,0,0,0)
    win32api.keybd_event(115,0,win32con.KEYEVENTF_KEYUP,0) #閲婃斁鎸夐敭
    win32api.keybd_event(VK_MENU,win32con.KEYEVENTF_KEYUP,0)
    

    print "window closed"
    
def saleStock(stockTrades):
    
    #win32api.keybd_event(115,0,0,0)
    win32api.keybd_event(VK_F2,0,0,0) #閲婃斁鎸夐敭
    win32api.keybd_event(VK_F2,win32con.KEYEVENTF_KEYUP,0)
    if stockTrades != None:
        pass
    print "Sales Stock"

def buyStock(stockTrades):
    
    # Active Buy Window
    win32api.keybd_event(VK_F1,0,0,0) #閲婃斁鎸夐敭
    win32api.keybd_event(VK_F1,win32con.KEYEVENTF_KEYUP,0)   
     
    if stockTrades != None:
        #win32api.keybd_event(115,0,0,0)    
        for char in stockTrades[0][0]:
            pass
            #win32api.SendMessage()
               
        win32api.keybd_event(VK_TAB,0,0,0)
        win32api.keybd_event(VK_TAB, win32con.KEYEVENTF_KEYUP,0)
        print "Buy Stock"

def getMenu(hwnd):
    print win32gui.GetMenu(hwnd)

def setActiveWindow(hwnd):
    win32gui.MoveWindow(hwnd, 0,0,800,600, True)
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
   

  
def findStockWindow(windowClass, windowName):
    hwnd = win32gui.FindWindow(windowClass, windowName)
    win32gui.MoveWindow(hwnd, 0,0,800,600, True)
    attrsWindow['mainWindowHandle'] = hwnd
    
def getActiveWindow():
    hwnd = win32gui.GetForegroundWindow()
    print hwnd
    return hwnd
    

def getHandleByControlID(parentHwnd, controlID):
    print win32gui.GetDlgCtrlID(controlID)
    print win32gui.GetDlgItem(parentHwnd, controlID)

def main():
    

    
    #windowsName = "广发证券核新网上交易系统7.56"
    windowName = tradeWindows.get('mainWindowName')[0]
    #windowClass = "Afx:400000:b:10005:6:3095b"
    windowClass = tradeWindows.get('mainWindowName')[1]
          
    stockTrades = ['300226', '1000'] 
    
    findStockWindow(windowClass, windowName)
    
    controlID = '1032'
    
    getHandleByControlID(attrsWindow.get('mainWindowHandle'), controlID)
    
   # windowsInitiation()
    
    print attrBuyWindow
    
    
    #win32gui.EnumWindows(callback, windowsName)
    #win32gui.EnumWindows(callback, None)
    #hwnd = 133778
    #callback(hwnd, None)
    
    #hwndMainWindowStock = attrsWindow.get('mainWindowHandle',0)
      
    
    #setActiveWindow(hwndMainWindowStock)
    #getActiveWindow()
    #saleStock(stockTrades)
    #buyStock(stockTrades)
    
  
    
    
if __name__ == '__main__':
    main()
