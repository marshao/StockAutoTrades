#import autopy
import wx
#import win32gui



'''
def hello_world():
    autopy.alert.alert("Hellow World")
'''

#autopy.mouse.smooth_move(1, 1)

class windowClass(wx.Frame):
    
    #def __init__(self, parent, title):
    # *args = arguments, **kwargs = key word arguments
    def __init__(self, *args, **kwargs):
        
        #super(windowClass, self).__init__(parent, title = title, size =(200,200))
        super(windowClass, self).__init__(*args, **kwargs)
        
        self.basicGUI()
        #self.Move((800,250))
        #self.Center()
        #self.Show()
        
    def basicGUI(self):
        menuBar = wx.MenuBar()
        fileButton = wx.Menu()
        exitItem = fileButton.Append(wx.ID_EXIT, 'Exit', 'status message ...')
        
        menuBar.Append(fileButton, 'File')
        
        self.SetMenuBar(menuBar)
        
        self.Bind(wx.EVT_MENU, self.Quit, exitItem)
        
        self.SetTitle('Epic Window')
        self.Show()
        
    def Quit(self, e):
        self.Close()



def main():
    app = wx.App()
    windowClass(None, size = (300, 300))
    app.MainLoop()
    
    
#app = wx.App()

#windowClass(None, title='test')

'''
frame = wx.Frame(None, -1, 'Windows Title', style =wx.MAXIMIZE_BOX | wx.SYSTEM_MENU | wx.CLOSE_BOX | wx.CAPTION)
frame.Show()
'''
#app.MainLoop() # keep main programe on working to show the window.



'''
Win32gui
Get current active windows title

'''

#tempwindow = win32gui.GetWindowText(win32gui.GetForegroundWindow())

#print tempwindow