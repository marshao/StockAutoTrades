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
from win32con import VK_MENU, HWND_TOP


attrsWindow = []


#tempwindow = win32gui.GetActiveWindow() 

#tempwindow = win32gui.GetWindow(7607276)


def callback(hwnd, windowsName):
    
    rect = win32gui.GetWindowRect(hwnd)
    x = rect[0]
    y = rect[1]
    w = rect[2] - x
    #w = 973
    h = rect[3] - y
    #h = 565
    hwndStock = 0    
    if win32gui.GetWindowText(hwnd) == windowsName:
        #print "Window %s:" % win32gui.GetWindowText(hwnd), "Window hwnd %i:" %hwnd
        #print "\tLocation: (%d, %d)" % (x, y)
        #print "\t    Size: (%d, %d)" % (w, h)
        #win32gui.MoveWindow(hwnd, 0,0,w,h, True)
        attrsWindow.extend([hwnd, x, y, w, h])
        #print "move Window"
        #win32gui.MoveWindow(hwnd, 500,500,500,500,True)
           
    
def closeStockWindow():
    #win32api.keybd_event(VK_MENU,0,0,0)聽
    win32api.keybd_event(VK_MENU,0,0,0)
    #win32api.keybd_event(0x73,0,0,0)聽
    win32api.keybd_event(115,0,0,0)
    win32api.keybd_event(115,0,win32con.KEYEVENTF_KEYUP,0) #閲婃斁鎸夐敭
    win32api.keybd_event(18,0,win32con.KEYEVENTF_KEYUP,0)
    print "window closed"
    
def getMenu(hwnd):
    print win32gui.GetMenu(hwnd)

def setActiveWindow(hwnd):
    win32gui.MoveWindow(hwnd, 0,0,800,600, True)
    shell = win32com.client.Dispatch("WScript.Shell")
    shell.SendKeys('%')
    win32gui.SetForegroundWindow(hwnd)

def setWindowTop(hwnd):

   print hwnd, " Move Window"
   win32gui.MoveWindow(hwnd, 0,0,800,600, True)
   #win32gui.BringWindowToTop(hwnd)
   

  
def findStockWindow(windowsName):
    hwnd = win32gui.FindWindow('TdxW_MainFrame_Class', windowsName)
    rect = win32gui.GetWindowRect(hwnd)
    x = rect[0]
    y = rect[1]
    w = rect[2] - x
    #w = 973
    h = rect[3] - y
    attrsWindow.append(['mainWindow',hwnd, x, y, w, h])
    
def getActiveWindow():
    hwnd = win32gui.GetForegroundWindow()
    print hwnd
    return hwnd
    

def main():
    
    
    windowsName = "方正证券泉友通专业版V6.44 - [平台首页]"
    
    findStockWindow(windowsName)
    #win32gui.EnumWindows(callback, windowsName)
    #hwnd = 133778
    #callback(hwnd, None)
    
    hwndStock = attrsWindow[0][1]
      
    #print hwndStock
    #getMenu(hwndStock)
    
    setActiveWindow(hwndStock)
    #setWindowTop(hwndStock)
    getActiveWindow()
    closeStockWindow()
    getActiveWindow()
    
    
if __name__ == '__main__':
    main()
