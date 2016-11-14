#! /usr/bin/python
# -*- coding:utf-8 -*-
"""
This file is for processing the downloaded Stock Daily Price data.

The function is to retrieve all stock daily records and saved them into the MySQL DB

Use PANDAS to calculate the MACD

20161018 Try multi processing.

"""

import re
import os
import csv
import MySQLdb
from datetime import datetime
import time
import pandas as pd
import multiprocessing as mp

'''
Function to calculate trading sigal for each pattern
'''
def MACD_Cal(df_StockRecords, short_window, long_window, dif_window, MACD_Pattern_Number):
	numRecords = len(df_StockRecords)
	df_StockRecords['EMA_short'] = pd.ewma(df_StockRecords.close_price, span=short_window)
	df_StockRecords['EMA_long'] = pd.ewma(df_StockRecords.close_price, span=long_window)
	df_StockRecords['DIF'] = df_StockRecords.EMA_short - df_StockRecords.EMA_long
	df_StockRecords['DEA'] = pd.rolling_mean(df_StockRecords.DIF, window = dif_window)
	df_StockRecords['MACD']=2.0*(df_StockRecords.DIF - df_StockRecords.DEA)		
	df_StockRecords['Signal'] = 0
	df_StockRecords['EMA_short_window'] = short_window
	df_StockRecords['EMA_long_window'] = long_window
	df_StockRecords['DIF_window'] = dif_window
	df_StockRecords['MACD_Pattern_Number'] = MACD_Pattern_Number
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

	# Add data into DB		

	db = MySQLdb.connect(host='127.0.0.1',
	port=3306,
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

	df_StockRecords.drop('close_price', axis=1, inplace=True)
	df_StockRecords.to_sql('tb_StockIndex_MACD', con=db, flavor='mysql', if_exists='append', index = False)

	print ("work end:{0}".format(time.ctime()))

	return





# Delair varables for reading stock codes
inputDir = '/home/marshao/DataMiningProjects/Input/'
outputDir = '/home/marshao/DataMiningProjects/Output/'
stockNameFile = outputDir + 'StockNames.csv'
stockCodes = []
stockCode ="300216"
stockMarket = ""
window = '10'

# Delcair valables for setting up connection to local MySQL Server 
db = MySQLdb.connect(host='127.0.0.1',
	port=3306,
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

cursor = db.cursor()

'''
#Clear the table
cursor.execute("Truncate tb_StockIndex_MACD" )
'''


#Select all existing StockCodes from DB
select_id_tb_StockCode = ("SELECT id_tb_StockCodes from tb_StockCodes where stock_code = %(stockCode)s")
data_id_tb_StockCode = {'stockCode':stockCode}

#Select closing prices of all transactions for dedicated stock code
#select_StockRecords = ("SELECT close_price from tb_StockDailyRecords"
#						" where id_tb_StockCodes = %(id_tb_stockCodes)s ")

#Fetch out all trascations
select_StockRecords = ("SELECT record_date, id_tb_StockCodes, close_price from tb_StockDailyRecords"
						" where id_tb_StockCodes = %(id_tb_stockCodes)s")

#Select MACD Indexes with dedicated EMA_long_window
select_MACDIndex_window = ("SELECT EMA_long_window, EMA_short_window, DIF_window from tb_MACDIndex where EMA_long_window = %(window)s")
data_MACDIndex_window = {'window':window}


#Select MACD Indexes with all avalible EMA_long_window
select_MACDIndex_all = ("SELECT EMA_long_window, EMA_short_window, DIF_window from tb_MACDIndex")


#Fetch out PK id of StockCode
cursor.execute(select_id_tb_StockCode, data_id_tb_StockCode)
id_tb_StockCodes = cursor.fetchall()

#print id_tb_StockCodes[0]

#Fetch out required MACD_Index
#cursor.execute(select_MACDIndex_window, data_MACDIndex_window)
cursor.execute(select_MACDIndex_all)
MACD_Indexes = cursor.fetchall()
MACD_Pattern_Number = 1

print "The number of CPU is: "+str(mp.cpu_count())

for index in MACD_Indexes:
	
	print index[1],index[0],index[2]
	
	'''
	For loop breaker
	
	if MACD_Pattern_Number == 9:
		break
	'''
	df_StockRecords = pd.read_sql(select_StockRecords, con=db, params={'id_tb_stockCodes':id_tb_StockCodes[0]})
	#df_StockRecords = df_StockRecords_Cal
	
	print ("work start:{0}".format(time.ctime()))
	
	# Call MACD calculation funtion in multiprocessing
	#MACD(df_StockRecords,index[1],index[0],index[2], MACD_Pattern_Number)
	
	p = mp.Process(target = MACD_Cal, args=(df_StockRecords,index[1],index[0],index[2], MACD_Pattern_Number) )
	p.daemon = True
	p.start()
	p.join()
	print ("Child p.name: " + p.name + "\t p.id: "+ str(p.pid))

	'''
	# Move the two sentences of add data2DB into function 

	df_StockRecords.drop('close_price', axis=1, inplace=True)
	df_StockRecords.to_sql('tb_StockIndex_MACD', con=db, flavor='mysql', if_exists='append', index = False)
	'''
	MACD_Pattern_Number = MACD_Pattern_Number + 1

#Close Database connection
cursor.close()
db.close()




print("Task finished")
			





