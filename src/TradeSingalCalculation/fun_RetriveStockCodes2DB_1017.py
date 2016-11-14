#! /usr/bin/python
# -*- coding:utf-8 -*-
"""
This file is for processing the downloaded Stock Daily Price data.

The function is to retrieve all stock names and saved them into the MySQL DB

20161015: Edit the insert statement for adding StockName into the DB, but the chinese charators encoding utf8 in Ubuntu
is not compatibal with chinese charactor in MySQL utf8mb4.

20161017: Edit the insert statement for avoiding from adding dupicate row.
20161018: Fixed the bug of avoiding inserting duplicate rows.

"""

import re
import os
import csv
import MySQLdb



# Delair varables for reading stock codes
inputDir = '/home/marshao/DataMiningProjects/Input/'
outputDir = '/home/marshao/DataMiningProjects/Output/'
stockNameFile = outputDir + 'StockNames.csv'
stockCodes = []
stockCode =""
stockMarket = ""

# Delcair valables for setting up connection to local MySQL Server 

db = MySQLdb.connect(host='127.0.0.1',
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

cursor = db.cursor()


add_StockCodes = ("INSERT INTO tb_StockCodes "
				"(stock_code, stock_name, stock_market, stock_trad)"
				" Select %(stockCode)s,%(stockName)s, %(stockMarket)s, %(stockTrad)s"
				" WHERE NOT EXISTS"
				" (SELECT * FROM tb_StockCodes WHERE stock_code = %(oldstockCode)s)")

'''
Insert new stock code and name into DB, do not insert duplicate value.

add_StockCodes = ("INSERT INTO tb_StockCodes "
				"(stock_code, stock_name, stock_market, stock_trad) "
				"SELECT * FROM (SELECT %(stockCode)s,%(stockName)s, %(stockMarket)s, %(stockTrad)s ) AS tmp"
				"WHERE NOT EXISTS"
				"(SELECT stock_code FROM tb_StockCodes WHERE stock_code = %(stockCode)s)")
'''

#"Read Stock Codes from the source folder"
for root, dirs, files in os.walk("/home/marshao/DataMiningProjects/Input"):
	for eachFile in files:

		file = inputDir + eachFile
		#print eachFile[2:8]
		with open (file, 'r') as f:
			content = f.readline()
			#print content[7:20]

		data_StockCodes = {
			'stockCode':eachFile[2:8],
			'stockName':content[7:20],
			'stockMarket':eachFile[0:2],
			'stockTrad':1,
			'oldstockCode':eachFile[2:8],	
		}
		# Write Stock codes into Database
		
		try:
			cursor.execute(add_StockCodes, data_StockCodes)
			db.commit()
		except:
			print "SQL Statment error!"
			db.rollback()
			

#Close Database connection
cursor.close()
db.close()


print("Task finished")
			

