#coding=GBK

__metclass__ = type

import MySQLdb


class DBConnector:

    db = MySQLdb.connect(host = '10.175.10.231',
                         port = 3306,
                         user = 'marshao',
                         passwd = '123',
                         db = 'DB_StockDataBackTest')
    def __init__(self):        
        return db
    
    def cursor(self):
        cursor = db.cursor()
        return cursor
    
    def __del__(self):
        cursor().close()
        db.close()
        
    def cursorSelect(self, querry):
        
        cursor().execute(querry)
        dataSet = cursor.fetchall()
        return dataSet
    
    def cursorAdd(self,querry):
        cursor().execute()