# coding=GBK

__metclass__ = type

import MySQLdb


class C_DBConnector:
    __db__ = MySQLdb.connect(host='10.175.10.231',
                         port=3306,
                         user='marshao',
                         passwd='123',
                         db='DB_StockDataBackTest')

    def __init__(self):
        return self.__db__

    def cursor(self):
        cursor = self.__db__.cursor()
        return cursor

    def __del__(self):
        self.cursor().close()
        self.__db__.close()

    def cursorSelect(self, querry):
        self.cursor().execute(querry)
        dataSet = self.cursor.fetchall()
        return dataSet

    def cursorAdd(self, querry):
        self.cursor().execute()