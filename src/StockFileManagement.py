#coding=GBK

import DBConnector
import io, os, time

__metclass__ = type

class StockFileManagement:
    
    def __init__(self):
        self._outputDir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\'
        self._installDir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\'
        self._stockRecordDir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._stockInhandFile = 'stockInhand.csv'
        self._logMesg = ''
        self._opLog = 'operLog.txt'
    
    def __isFileExist__(self, path):
        return os.path.isfile(path)
        
    
    def readStockInhandToDB(self):
        fullPath = self._outputDir + self._stockInhandFile
        if self.__isFileExist__(fullPath):
            with open(fullPath, 'r') as file:
                lines = file.readlines()
                print lines
        else:
            self._logMesg = 'There is no stockInhandFileExist at ', __timeTag()
        
        self.__writelog(self._opLog)

    def __timeTag(self):
        return time.asctime(time.localtime(time.time()))
    
    def __writelog(self, logPath):
        fullPath = self._outputDir + logPath
        with open(fullPath, 'a') as log:
            log.writelines(self._logMesg)
            

def main():
    fm = StockFileManagement()
    
    fm.readStockInhandToDB()

if __name__ == '__main__':
    main()