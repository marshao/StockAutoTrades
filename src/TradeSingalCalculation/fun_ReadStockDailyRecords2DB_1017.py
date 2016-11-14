#! /usr/bin/python
# -*- coding:utf-8 -*-
"""
This file is for processing the downloaded Stock Daily Price data.

The function is to retrieve all stock daily records and saved them into the MySQL DB

20161018: Disable the table TRUNCATE at the begining of the program.

"""

import re
import os
import csv
import MySQLdb
from datetime import datetime
import time



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
#cursor.execute("TRUNCATE tb_StockDailyRecords")


add_StockDailyRecords = ("INSERT INTO tb_StockDailyRecords "
						"(record_date, open_price, high_price,low_price, close_price, total_volume, total_amount, id_tb_StockCodes)"
						" VALUES (%(tradeDate)s,%(oPrice)s,%(hPrice)s,%(lPrice)s,%(cPrice)s,%(volume)s,%(amount)s,%(id_tb_StockCode)s)")

select_id_tb_StockCodes = ("SELECT id_tb_StockCodes from tb_StockCodes where stock_code = %(stockCode)s")

#"Read Stock Records from the source folder"
for root, dirs, files in os.walk("/home/marshao/DataMiningProjects/Input"):
	for eachFile in files:
		file=inputDir + eachFile
		stockCode = eachFile[2:8]
		print stockCode
		data_id_tb_StockCodes = {'stockCode':stockCode}
		try:
			cursor.execute(select_id_tb_StockCodes,data_id_tb_StockCodes)
			id_tb_StockCode = cursor.fetchone() # Get one element from all returned touple.
			#print id_tb_StockCode[0]
		except:
			pass
		
		with open(file, 'r') as f:
			content = f.readlines()[2:len(f.readlines())-1] # read lines start from the 3rd line
			#content=''.join(content).strip('\n')
			for line in content:
				elements = line.split("\t")
				#print elements[6]
				t = time.strptime(elements[0],"%m/%d/%Y")
				y,m,d = t[0:3]
				tradeDate = datetime(y,m,d)

				data_StockDailyRecords ={
					'tradeDate':tradeDate,
					#'tradeDate':'2009/09/30',
					'oPrice':elements[1],
					'hPrice':elements[2],
					'lPrice':elements[3],
					'cPrice':elements[4],
					'volume':elements[5],
					'amount':''.join(elements[6]).strip('\r\n'),
					'id_tb_StockCode':id_tb_StockCode[0],
				}
				
				#print data_StockDailyRecords

				# Write Stock codes into Database	
				try:
					
					cursor.execute(add_StockDailyRecords, data_StockDailyRecords)
					#print "cursor executed"
					db.commit()
				except:
					db.rollback()

		
		#print stockCode,fileName
#for eachItem in cursor.fetchall():
#	print eachItem[1]



#Close Database connection
cursor.close()
db.close()



print("Task finished")
			

