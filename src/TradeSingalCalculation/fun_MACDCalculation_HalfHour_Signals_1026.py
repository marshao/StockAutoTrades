#! /usr/bin/python
# -*- coding:utf-8 -*-
"""
This file is for processing the downloaded Stock Daily Price data.

The function is to retrieve all stock daily records and saved them into the MySQL DB

Use PANDAS to calculate the MACD

20161024: Enhanced function to calculate trade signals for both Daily Records and HalfHour Records
* Modified codes
* Added a new table tb_StockIndex_MACD_HalfHour into DB.
* Differents trade signals will be saved into different tables.

20161026:
1: Added some constrint to limit trasactions in each trading day.
2: Since the T+1 policy, for every day, the system only do sales -> buy
but not buy -> sales, so once there is buy happened, then set tradable = False, 
until the next day to set tradable = True  

20161027:
1. Added progressbar to monitor the progress of calculation.
2. 	If the Signal = 0, means there is no transaction at that time.
	Then there is no need to save the no transaction row into DB.

"""

import re
import os
import csv
import MySQLdb
from datetime import datetime
import time
import pandas as pd
import progressbar

def MACD(df_StockRecords, short_window, long_window, dif_window, MACD_Pattern_Number):
	numRecords = len(df_StockRecords)
	numDailyTrade = 0
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
	df_StockRecords.set_index(df_StockRecords.record_time, inplace = True)
	#print df_StockRecords
	df_StockRecords['Date'] = df_StockRecords.index.date
	df_StockRecords['Time'] = df_StockRecords.index.time

	indexDate = df_StockRecords.Date[0]
	tradable = True 


	for i in range(numRecords):
		'''
		#Since T+1 policy, There trade should be disabled if there is a buy happend in each trading day.
		#If the indexDate is not equal to record date, means diffrent day, set the tradable = True 
		'''
		if indexDate != df_StockRecords.Date[i]:
			tradable = True
			indexDate = df_StockRecords.Date[i]
		
		if (i > 0): 
			if ((df_StockRecords.DIF[i] > df_StockRecords.DEA[i]) & (df_StockRecords.DIF[i-1] <= df_StockRecords.DEA[i-1])):
				if tradable:
					df_StockRecords.Signal[i] = 1
					tradable = False
				else:
					continue
			elif ((df_StockRecords.DIF[i] < df_StockRecords.DEA[i]) & (df_StockRecords.DIF[i-1] >= df_StockRecords.DEA[i-1])):
				if tradable:
					df_StockRecords.Signal[i] = -1
				else:
					continue
			else: 
				pass
				#df_StockRecords.drop(df_StockRecords.index[i], axis = 0, inplace = True)
		else:
			pass

	df_StockRecords.drop(['Date', 'Time'],  axis = 1, inplace = True)

	# Remove the no transaction record from the DB.
	df_StockRecords = df_StockRecords[df_StockRecords.Signal != 0]
	
	#print len(df_StockRecords)
	#df_StockRecords.drop('Time', axis = 1)
	#print df_StockRecords.groupby('Signal').count()

	return df_StockRecords





# Delair varables for reading stock codes
inputDir = '/home/marshao/DataMiningProjects/Input/'
outputDir = '/home/marshao/DataMiningProjects/Output/'
stockNameFile = outputDir + 'StockNames.csv'
stockCodes = []
stockCode ="300226"
stockMarket = ""
window = '10'

# Delcair valables for setting up connection to local MySQL Server 
db = MySQLdb.connect(host='127.0.0.1',
	port=3306,
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

cursor = db.cursor()

#Clear the table
cursor.execute("Truncate tb_StockIndex_MACD" )

#Select all existing StockCodes from DB
select_id_tb_StockCode = ("SELECT id_tb_StockCodes from tb_StockCodes where stock_code = %(stockCode)s")
data_id_tb_StockCode = {'stockCode':stockCode}

#Select closing prices of all transactions for dedicated stock code
#select_StockRecords = ("SELECT close_price from tb_StockDailyRecords"
#						" where id_tb_StockCodes = %(id_tb_stockCodes)s ")

'''
Fetch Stock Daily, Hourly, HalfHour Transactions
'''
#Fetch Dailly trascations
select_StockDailyRecords = ("SELECT record_date, id_tb_StockCodes, close_price from tb_StockDailyRecords"
						" where id_tb_StockCodes = %(id_tb_stockCodes)s")

#Fetch Half Hour transactions
select_StockHalfHourRecords = ("SELECT record_time, id_tb_StockCodes, close_price from tb_StockHalfHourRecords"
						" where id_tb_StockCodes = %(id_tb_stockCodes)s")


'''
Fetch all or dedicated windows MACD Indexes 
'''
#Select MACD Indexes with dedicated EMA_long_window
select_MACDIndex_window = ("SELECT EMA_long_window, EMA_short_window, DIF_window from tb_MACDIndex where EMA_long_window = %(window)s")
data_MACDIndex_window = {'window':window}


#Select MACD Indexes with all avalible EMA_long_window
select_MACDIndex_all = ("SELECT EMA_long_window, EMA_short_window, DIF_window from tb_MACDIndex")

'''
Fetch Stock Code
'''

#Fetch out PK id of StockCode
cursor.execute(select_id_tb_StockCode, data_id_tb_StockCode)
id_tb_StockCodes = cursor.fetchall()

#print id_tb_StockCodes[0]

#Fetch out closing prices for dedicated stock code
#df_StockRecords = pd.read_sql(select_StockRecords, con=db, params={'id_tb_stockCodes':id_tb_StockCodes[0]})
#df_StockRecords_Cal = pd.read_sql(select_StockRecords, con=db, params={'id_tb_stockCodes':id_tb_StockCodes[0]})

'''
Setting up progress bar to monitor the progress of whole program.
'''
widgets = ['MACD_Pattern_BackTest: ', 
			progressbar.Percentage(), ' ', 
			progressbar.Bar(marker='0',left='[',right=']'),' ', 
			progressbar.ETA()]

progress = progressbar.ProgressBar(widgets = widgets)


#Fetch out required MACD_Index
#cursor.execute(select_MACDIndex_window, data_MACDIndex_window)
cursor.execute(select_MACDIndex_all)
MACD_Indexes = cursor.fetchall()
MACD_Pattern_Number = 1
loopBreaker = 0

for index in progress(MACD_Indexes):

	'''
	#loop Breaker
	
	if loopBreaker == 4:
		break
	else:
		loopBreaker = loopBreaker + 1
	
	#print index[1],index[0],index[2]
	'''
	'''
	#Read Stock Records, Daily: tb_StockDailyRecords or HalfHour:tb_StockHalfHourRecords
	'''
	#df_StockRecords = pd.read_sql(select_StockDailyRecords, con=db, params={'id_tb_stockCodes':id_tb_StockCodes[0]})
	df_StockRecords = pd.read_sql(select_StockHalfHourRecords, con=db, params={'id_tb_stockCodes':id_tb_StockCodes[0]})
	
	df_Transactions = MACD(df_StockRecords,index[1],index[0],index[2], MACD_Pattern_Number)
	df_Transactions.drop('close_price', axis=1, inplace=True)
	
	'''
	Save into Database Daily:tb_StockIndex_MACD; HalfHour: tb_StockIndex_MACD_HalfHour
	'''
	#df_Transactions.to_sql('tb_StockIndex_MACD', con=db, flavor='mysql', if_exists='append', index = False)
	df_Transactions.to_sql('tb_StockIndex_MACD_HalfHour', con=db, flavor='mysql', if_exists='append', index = False)
	#print len(df_Transactions)
	
	MACD_Pattern_Number = MACD_Pattern_Number + 1

#Close Database connection
cursor.close()
db.close()




print("Task finished")
			





