#! /usr/bin/python
# -*- coding:utf-8 -*-
"""

The function is try to use the MySQL DB

"""

import MySQLdb

db = MySQLdb.connect(host='localhost',
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

cursor = db.cursor()

#cursor.execute("INSERT INTO tb_StockCodes (stock_name, stock_market, stock_code, stock_trad) VALUES('KJHG','SH','600893','1')")
cursor.execute("SELECT * FROM tb_StockCodes")


for row in cursor.fetchall():
	print row[1]

db.close()