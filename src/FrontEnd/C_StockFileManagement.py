#!/usr/local/bin/python
# coding: UTF-8

import MySQLdb
import os, time


__metclass__ = type


class C_StockFileManagement:
    def __init__(self):
        self._outputDir__ = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\'
        self._installDir__ = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\'
        self._stockRecordDir__ = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._stockInhandFile__ = 'stockInhand.csv'
        self.__logMesg__ = ''
        self.__opLog__ = 'operLog.txt'
        self.__stockCode__ = '证券代码'
        self.__db__ = MySQLdb.connect(host='10.175.10.231',
                                 port=3306,
                                 user='marshao',
                                 passwd='123',
                                 db='DB_StockDataBackTest')

    def __del__(self):
        self.__db__.close()

    def __isFileExist__(self, path):
        return os.path.isfile(path)


    def readStockInhandToDB(self):
        fullPath = self._outputDir__ + self._stockInhandFile__
        data =[]
        colCount = 12 # 12 item in each line
        add_StockInHand = ("INSERT INTO tb_StockInhand ('stockCode','stockName','stockRemain',"
                           "'stockAvaliable','baseCost','currentCost','currentValue','profit',"
                           "'profitRate','stockMarket','averageBuyPrice','datetime') "
                           "VALUES (%s, %s, %INT, %INT , %DECIMAL, %DECIMAL ,%DECIMAL ,%DECIMAL ,%DECIMAL "
                           "%s, %DECIMAL , %DATE)")
        if self.__isFileExist__(fullPath):
            lines = open(fullPath, 'r').read().split()
            #cursor = self.__getDBConnector__()
            for word in lines:
                data.append(self.__convert_encoding__(word, 'UTF-8'))

            print

            #print type(data)
            #df = pandas.DataFrame(data)


        else:
            self.__logMesg__ = 'There is no stockInhandFileExist at ', self.__timeTag__()

        self.__writelog__(self.__opLog__)

    def __getDBConnector__(self):
        #cursor = self.__db__.cursor()
        #return cursor

    def __timeTag__(self):
        return time.asctime(time.localtime(time.time()))

    def __writelog__(self, logPath):
        fullPath = self._outputDir__ + logPath
        with open(fullPath, 'a') as log:
            log.writelines(self.__logMesg__)

    def __convert_encoding__(self, lines, new_coding='UTF-8'):
        encoding = 'GB2312'
        data = lines.decode(encoding)
        data = data.encode(new_coding)
        return data

def main():
    fm = C_StockFileManagement()

    fm.readStockInhandToDB()



if __name__ == '__main__':
    main()