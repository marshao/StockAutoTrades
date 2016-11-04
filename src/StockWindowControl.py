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

import win32gui


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
        print "Window %s:" % win32gui.GetWindowText(hwnd), "Window hwnd %i:" %hwnd
        print "\tLocation: (%d, %d)" % (x, y)
        print "\t    Size: (%d, %d)" % (w, h)
        win32gui.MoveWindow(hwnd, 0,0,w,h, True)
        attrsWindow = [x, y, w, h, hwnd]   
    return 

def main():
    
    windowsName = "方正证券泉友通专业版V6.44 - [平台首页]"
    attrsWindow = []
    
    win32gui.EnumWindows(callback, windowsName)
    #hwnd = 133778
    #callback(hwnd, None)

if __name__ == '__main__':
    main()
