#! /usr/bin/python
# -*- coding:utf-8 -*-
"""
This file is for processing the downloaded Stock Daily Price data.

The function is to retrieve all stock daily records and saved them into the MySQL DB

Use PANDAS to calculate the MACD




"""

import re
import os
import csv
import MySQLdb
from datetime import datetime
import time
import pandas as pd

def MACD(df_StockRecords, short_window, long_window, dif_window):
	numRecords = len(df_StockRecords)
	df_StockRecords['EMA_short'] = pd.ewma(df_StockRecords.close_price, span=short_window)
	df_StockRecords['EMA_long'] = pd.ewma(df_StockRecords.close_price, span=long_window)
	df_StockRecords['DIF'] = df_StockRecords.EMA_short - df_StockRecords.EMA_long
	df_StockRecords['DEA'] = pd.rolling_mean(df_StockRecords.DIF, window = dif_window)
	df_StockRecords['MACD']=2.0*(df_StockRecords.DIF - df_StockRecords.DEA)		
	df_StockRecords['Signal'] = 0
	for i in range(numRecords):
		if (i > 0): 
			if ((df_StockRecords.DIF[i] > df_StockRecords.DEA[i]) & (df_StockRecords.DIF[i-1] <= df_StockRecords.DEA[i-1])):
				df_StockRecords.Signal[i] = 1
			if ((df_StockRecords.DIF[i] < df_StockRecords.DEA[i]) & (df_StockRecords.DIF[i-1] >= df_StockRecords.DEA[i-1])):
				df_StockRecords.Signal[i] = -1
			else: 
				pass
		else:
			pass

	return df_StockRecords

# Delair varables for reading stock codes
inputDir = '/home/marshao/DataMiningProjects/Input/'
outputDir = '/home/marshao/DataMiningProjects/Output/'
stockNameFile = outputDir + 'StockNames.csv'
stockCodes = []
stockCode =""
stockMarket = ""


# Delcair valables for setting up connection to local MySQL Server 
db = MySQLdb.connect(host='127.0.0.1',
	port=3306,
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

cursor = db.cursor()
cursor.execute("Truncate tb_StockIndex_MACD" )
#Select all existing StockCodes from DB
select_id_tb_StockCodes = ("SELECT id_tb_StockCodes, stock_code from tb_StockCodes")

#Select closing prices of all transactions for dedicated stock code
#select_StockRecords = ("SELECT close_price from tb_StockDailyRecords"
#						" where id_tb_StockCodes = %(id_tb_stockCodes)s ")

select_StockRecords = ("SELECT record_date, id_tb_StockCodes, close_price from tb_StockDailyRecords"
						" where id_tb_StockCodes = %(id_tb_stockCodes)s")

#Fetch out all exisiting stock codes
cursor.execute(select_id_tb_StockCodes)
numRows = int(cursor.rowcount)
id_tb_StockCodes = cursor.fetchall()

#Fetch out closing prices for dedicated stock code
for each_id_tb_StockCode in id_tb_StockCodes:
	#print id_tb_StockCode[0], id_tb_StockCode[1]
	#data_StockRecords = {'id_tb_stockCodes':id_tb_StockCode[0]}
	#data_StockRecords = {'id_tb_stockCodes':'18'}
	
	df_StockRecords = pd.read_sql(select_StockRecords, con=db, params={'id_tb_stockCodes':each_id_tb_StockCode[0]})
	#MACD(df_StockRecords,12,26,9)
	MACD(df_StockRecords,3,51,6)
	df_StockRecords.drop('close_price', axis=1, inplace=True)
	#print df_StockRecords

	'''
		Wirte MACD index data into database
	'''

	df_StockRecords.to_sql('tb_StockIndex_MACD', con=db, flavor='mysql', if_exists='append', index = False)

#Close Database connection
cursor.close()
db.close()




print("Task finished")
			





