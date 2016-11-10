#coding=GBK 

import win32gui, win32com.client, win32api, win32con
from time import sleep
from win32con import *

refreshButton=[418,299,32790,0,0]    

def findStockWindow(windowClass, windowName):
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys('%')
    refreshButton[4] = win32gui.FindWindow(windowClass, windowName)
    win32gui.MoveWindow(refreshButton[4], 0,0,800,600, True)
    print 'main window handle is: ', refreshButton[4]
    
    

def hitButton(buttonHwnd):
    win32gui.SetActiveWindow(refreshButton[4])
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys('%')
    win32api.SendMessage(buttonHwnd, win32con.BM_CLICK, 0, 0)
    print 'Refreshed on button ', buttonHwnd

    #win32api.keybd_event(VK_TAB,0,0,0)
    #win32api.keybd_event(VK_TAB,win32con.KEYEVENTF_KEYUP,0)    

def EnumChildWindowProc(buttonHwnd, code):
    if win32gui.GetDlgCtrlID(buttonHwnd) == refreshButton[2]:
        winRec = win32gui.GetWindowRect(buttonHwnd)
        if winRec[0] == refreshButton[0] and winRec[1] == refreshButton[1]:
            refreshButton[3] = buttonHwnd
            print 'Found Refresh Button: ', buttonHwnd
            if refreshButton[3]==0:
                refreshButton[3] = buttonHwnd
                print 'assigned refresh button handle'

def main():
    
    windowName = "广发证券核新网上交易系统7.56"
    findStockWindow(None, windowName)
    win32gui.EnumChildWindows(refreshButton[4], EnumChildWindowProc, None)
                
    while True:
        sleep(20)   
        hitButton(refreshButton[3])
    
    #windowName = tradeWindows.get('mainWindowName')[0]
    
  
    
    
if __name__ == '__main__':
    main()