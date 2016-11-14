#coding=GBK

import DBConnector
import io, os, time

__metclass__ = type

class StockFileManagement:
    
    def __init__(self):
        self._outputDir__ = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\'
        self._installDir__ = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\'
        self._stockRecordDir__ = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._stockInhandFile__ = 'stockInhand.csv'
        self._logMesg__ = ''
        self._opLog__ = 'operLog.txt'
    
    def __isFileExist__(self, path):
        return os.path.isfile(path)
        
    
    def readStockInhandToDB(self):
        fullPath = self._outputDir__ + self._stockInhandFile__
        if self.__isFileExist__(fullPath):
            with open(fullPath, 'r') as file:
                lines = file.readlines()
                print lines
        else:
            self._logMesg = 'There is no stockInhandFileExist at ', self.__timeTag__()
        
        self.__writelog__(self._opLog__)

    def __timeTag__(self):
        return time.asctime(time.localtime(time.time()))
    
    def __writelog__(self, logPath):
        fullPath = self._outputDir__ + logPath
        with open(fullPath, 'a') as log:
            log.writelines(self._logMesg)
            

def main():
    fm = StockFileManagement()
    
    fm.readStockInhandToDB()

if __name__ == '__main__':
    main()