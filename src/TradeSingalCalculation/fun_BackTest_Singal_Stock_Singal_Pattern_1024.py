#! /usr/bin/python
# -*- coding:utf-8 -*-
"""
This file is for processing the downloaded Stock Daily Price data.

The function is to retrieve all stock daily records and saved them into the MySQL DB

Use PANDAS to calculate the MACD

20161017: 
1: Added detailed commends into the program.
2: Fixed the Step_5.5 bug, the orignal Numpy.delete() will return a copy of orignal MACD_TradArray. 
	to use the empty array, must assign the empty array back to the orginal MACD_TradeArray to make it really empty.
	Or, I can just create the new MACD_TradeArray everytime.
3: Bug Fix in step_5.3: For some pattern without trading signal, a empty MACD_TradeArray will be returned,
	then MACD_TradeArray[-1] will be out of the index. Need to identify whether the MACD_TradeArray is empty or not.

20161019:
1. Because started to insert multiple id_tb_StockCodes value into the table tb_StockIndex_MACD. The SQL query

select_StockIndex_MACD = ("SELECT * from tb_StockIndex_MACD 
where MACD_pattern_number = %(eachPattern)s") 

has been changed into 

select_StockIndex_MACD = ("SELECT * from tb_StockIndex_MACD 
where MACD_pattern_number = %(eachPattern)s and id_tb_StockCodes = %(id_tb_StockCodes)s")  

to filter different stocks.

2. Add loop breaker into the testing loop, to enable to control of test for dedicate pattern.

"""

import re
import os
import csv
import MySQLdb
from datetime import datetime
import time
import pandas as pd
import math
import numpy as np




# Delair varables for reading stock codes
inputDir = '/home/marshao/DataMiningProjects/Input/'
outputDir = '/home/marshao/DataMiningProjects/Output/'
stockNameFile = outputDir + 'StockNames.csv'
stockCodes = []
stockCode =""
stockMarket = ""

global stockVolume_Begin
global stockVolume_Current 
global stockVolume_End
global cash_Begin
global cash_Current
global cash_End
global totalValue_Begin
global totalValue_Current
global totalValue_End
global profitRate
#tradePercent = 0.3
global tradeCost
global tradeRecords
global tradeVolume

'''
Functions to calculate the profits of each trading signals of MACD index pattern
	Parameters:
		df_StockIndex_MACD: Panda_DataFrame to store trading signals of eaching MACD pattern
		df_StockClosePrices: Panda_DF to store closing prices of stock
		MACD_TradeArray: Empty Numpy_Array for storing profit records

	Return:
		A Numpy_Array with profit detail of each trading steps will be returned.

'''
def MACD_Profit(df_StockIndex_MACD, df_StockClosePrices, MACD_TradeArray, MACD_Tradetype, EMA_short_windows, EMA_long_windows, DIF_windows, eachPattern, patternCount):
	stockVolume_Begin = 10000
	cash_Begin = 200000
	tradeVolume = 10000
	cash_Current = cash_Begin
	stockVolume_Current = stockVolume_Begin
	
	numPrices = len(df_StockClosePrices)
	#print "numPrcies:  ",  numPrices
	j = 0
	numMACD = len(df_StockIndex_MACD)

	#print "df_StockIndex_MACD    ##############", df_StockIndex_MACD
	#print "df_StockClosePrices.close_price[0] ", df_StockClosePrices.close_price[0]
	#print "stockVolume_Begin * df_StockClosePrices.close_price[0] ",stockVolume_Begin * df_StockClosePrices.close_price[0]
	#print "Cash Begin  ", cash_Begin

	totalValue_Begin = (stockVolume_Begin * df_StockClosePrices.close_price[0] + cash_Begin)
	totalValue_Current = totalValue_Begin
	profit_Rate = 0

	for i in range(numMACD):

		tradeCost = df_StockClosePrices.close_price[j] * tradeVolume
		
		i = i + patternCount*numPrices
		#print "i = ", i
		'''
		Evaluate the trading signals and calculate the profits 
		'''
		if df_StockIndex_MACD.Signal[i] == 1: # Positive Signal, buy more stocks
			
			if cash_Current >= tradeCost:
				#tradeVolume = stockVolume_Current * tradePercent
				stockVolume_Current = tradeVolume + stockVolume_Current
				cash_Current = cash_Current - tradeCost
				totalValue_Current = stockVolume_Current * df_StockClosePrices.close_price[j] + cash_Current
				profit_Rate = math.log(totalValue_Current/totalValue_Begin)
				#MACD_TradeArray[numTrades] = np.array([(df_StockIndex_MACD.id_tb_StockCodes[0],df_StockIndex_MACD.record_date[i-1],
				#	tradeVolume,tradeCost,stockVolume_Current,cash_Current,totalValue_Current,profit_Rate)],dtype = MACD_Tradetype)
				MACD_TradeArray = np.append(MACD_TradeArray, np.array([(df_StockIndex_MACD.id_tb_StockCodes[i],df_StockIndex_MACD.record_date[i],tradeVolume,tradeCost,stockVolume_Current,cash_Current,totalValue_Current,profit_Rate, EMA_short_windows, EMA_long_windows,DIF_windows, eachPattern)], dtype = MACD_Tradetype))

				#numTrades = numTrades + 1
				#print totalValue_Current
			else:
				pass	


		elif df_StockIndex_MACD.Signal[i]==(-1): # NEgative Signal, sell stocks
			if stockVolume_Current >= tradeVolume:
				stockVolume_Current = stockVolume_Current - tradeVolume
				cash_Current = cash_Current + tradeCost
				totalValue_Current = stockVolume_Current * df_StockClosePrices.close_price[j] + cash_Current
				profit_Rate = math.log(totalValue_Current/totalValue_Begin)
				#MACD_TradeArray[numTrades] = np.array([(df_StockIndex_MACD.id_tb_StockCodes[0],df_StockIndex_MACD.record_date[i-1],
				#	tradeVolume,tradeCost,stockVolume_Current,cash_Current,totalValue_Current,profit_Rate)],dtype = MACD_Tradetype)
				MACD_TradeArray = np.append(MACD_TradeArray, np.array([(df_StockIndex_MACD.id_tb_StockCodes[i],df_StockIndex_MACD.record_date[i],tradeVolume,tradeCost,stockVolume_Current,cash_Current,totalValue_Current,profit_Rate, EMA_short_windows, EMA_long_windows, DIF_windows, eachPattern)], dtype = MACD_Tradetype))
				#numTrades = numTrades + 1
			else:
				pass
		else:	
			pass
			
		# close_prices has diffrent number count with numMACD, but numMACD must be X times of numPrices	
		if (i%numPrices == 0) & (i != 0):  ###### mod
			#print "current i: ", i
			#print "current j: ", j
			j = 0
			#print "Reset j:   ", j
		else:
			j = j+1

		

	#totalValue_End = totalValue_Current 
	#profit_Rate = math.log(totalValue_Current/totalValue_Begin)
	#profitRate = (totalValue_End - totalValue_Begin)/totalValue_Begin
	#print  df_StockClosePrices.id_tb_StockCodes[0], totalValue_Begin, EMA_short_windows, EMA_long_windows, DIF_windows, eachPattern
	print  df_StockClosePrices.id_tb_StockCodes[0], totalValue_Begin, totalValue_Current, profit_Rate, EMA_short_windows, EMA_long_windows, DIF_windows, eachPattern
	'''
	# function to return the ending total_value and profits
	MACD_TradeArray = np.append(MACD_TradeArray, np.array([(df_StockIndex_MACD.id_tb_StockCodes[i],df_StockIndex_MACD.record_date[i],tradeVolume,tradeCost,stockVolume_Current,cash_Current,totalValue_Current,profit_Rate, EMA_short_windows, EMA_long_windows, DIF_windows, eachPattern)], dtype = MACD_Tradetype))
	'''
	#print numMACD, numPrices 
	return MACD_TradeArray


# Delcair valables for setting up connection to local MySQL Server 
db = MySQLdb.connect(host='127.0.0.1',
	port=3306,
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

#cursor = db.cursor()

'''
Step_1: Retrive all MACD trade pattern numbers from DB into a Panda array
'''
select_MACD_Patterns = ("SELECT Distinct MACD_pattern_number from tb_StockIndex_MACD")
MACD_pattern_number = pd.read_sql(select_MACD_Patterns, con = db)['MACD_pattern_number']
#print MACD_pattern_number

'''
Step_2: Use the pattern number to retrive corresponding MACD indexs according to trading policy
'''
select_StockIndex_MACD = ("SELECT * from tb_StockIndex_MACD where MACD_pattern_number = %s and id_tb_StockCodes = %s")


'''
Step_3.1: Find out the unic index id of stock
Step_3.2: Use the unic index id to retrieve all history closing price from DB
'''

#Read stock ID, ['id_tb_StockCodes'][0] Assigned the first stock code as selection
#id_tb_StockCodes = pd.read_sql("SELECT Distinct id_tb_StockCodes from tb_StockIndex_MACD", con = db)['id_tb_StockCodes'][0]

id_tb_StockCodes = 16

#Retrive close_price for calculating the profit 
select_StockClosePrices = ("SELECT record_date, id_tb_StockCodes, close_price from tb_StockDailyRecords"
						" where id_tb_StockCodes = %(id_tb_stockCodes)s")

# Read Closing price and MACD Signal from DB
df_StockClosePrices = pd.read_sql(select_StockClosePrices, con=db, params={'id_tb_stockCodes':id_tb_StockCodes})
#print df_StockClosePrices


'''
Step_4.1: Define a numpy structured data array to store MACD trade detail of every steps of each pattern. 
'''
MACD_trade_column_names=['id_tb_StockCodes','record_date', 'tradeVolume','tradeCost',
		'stockVolume_Current', 'cash_Current','totalValue_Current', 'profit_Rate',
		 'EMA_short_window', 'EMA_long_window', 'DIF_window', 'MACD_pattern_number']
MACD_trade_column_formats = ['i','S16', 'i','f','i', 'f','f','f','i','i','i','i']

MACD_Tradetype = np.dtype({'names':MACD_trade_column_names, 'formats':MACD_trade_column_formats})

MACD_TradeArray = np.array([('0','2000/01/01','220','0','0','0','0','0','0','0','0','0')], dtype = MACD_Tradetype)


'''
Step_4.2: Define a numpy structured data array to store ending_profit rates for each MACD index pattern. 
'''

MACD_trade_column_names=['id_tb_StockCodes','record_date', 'tradeVolume','tradeCost',
		'stockVolume_Current', 'cash_Current','totalValue_Current', 'profit_Rate',
		 'EMA_short_window', 'EMA_long_window', 'DIF_window', 'MACD_pattern_number']
MACD_trade_column_formats = ['i','S16', 'i','f','i', 'f','f','f','i','i','i','i']

MACD_Tradetype = np.dtype({'names':MACD_trade_column_names, 'formats':MACD_trade_column_formats})

MACD_EndingProfit = np.array([('0','2000/01/01','220','0','0','0','0','0','0','0','0','0')], dtype = MACD_Tradetype)


#df_StockProfit = pd.DataFrame(columns=['record_date','id_tb_StockCodes','stockVolume_Current','totalValue_Current','cash_Current','tradeVolume','tradeCost'])
'''
#Fetch out closing prices for dedicated stock code
for EMA_long_windows in EMA_long_windows:
	for EMA_short_windows in EMA_short_windows:
		for DIF_windows in DIF_windows:
			MACD_TradeArray = MACD_Profit(df_StockIndex_MACD,df_StockClosePrices, MACD_TradeArray, MACD_Tradetype, EMA_short_windows, EMA_long_windows, DIF_windows)
'''
'''
Step_5: Calculate profits using each pattern. The trading singal aggregation list for all MACD patterns is too huge(200MB) for computing,
 instead of reading all data in one time, the program split the trading singal list into smaller piece for each single pattern,
 Then use the smaller trading list to calculate the profit rates.  

Step_5.1: Use each pattern number to retrieve MACD trading signal and index detail(EMA windows of short, long, DIF) and store the data into a Panda_DataFrame 
Step_5.2: Use the Panda_DF as parameter, Call MACD_Profit function to calculate the profits, and return a numpy_array with data back to main program
Step_5.3: Use a different numpy_array to store ending profit records for each pattern
Step_5.4: Covert the returned numpy_array into a Panda_DataFrame, use the DataFrame to store trading records into DB.
Step_5.5: Empty the Numpy_array for result storing to prevent the array getting too big to slow down the calculation
'''
patternCount = 0

loopBreaker = 0 # loop breaker 

for eachPattern in MACD_pattern_number:
	
	'''
	Loop breaker and dedicate pattern assginer.
	'''
	if loopBreaker == 0:
		eachPattern = 713
		loopBreaker = loopBreaker + 1
	else:
		break
	

	'''
	Step_5.1:
	'''
	
	df_StockIndex_MACD = pd.read_sql(select_StockIndex_MACD, con=db, params=(eachPattern, id_tb_StockCodes))
	
	#df_StockIndex_MACD = df_StockIndex_MACD[df_StockIndex_MACD.MACD_pattern_number == eachPattern]
	EMA_short_windows = df_StockIndex_MACD["EMA_short_window"][0]
	EMA_long_windows = df_StockIndex_MACD["EMA_long_window"][0]
	DIF_windows = df_StockIndex_MACD["DIF_window"][0]

	'''
	Step_5.2:
	Create a Numpy_array to carry the trade detail of each Trading signal.

	Parameters:
		df_StockIndex_MACD: Panda_DataFrame to store trading signals of eaching MACD pattern
		df_StockClosePrices: Panda_DF to store closing prices of stock
		MACD_TradeArray: Empty Numpy_Array for storing profit records

	'''
	#MACD_TradeArray = np.array([('0','2000/01/01','220','0','0','0','0','0','0','0','0','0')], dtype = MACD_Tradetype)
	
	MACD_TradeArray = MACD_Profit(df_StockIndex_MACD,df_StockClosePrices, MACD_TradeArray, MACD_Tradetype, EMA_short_windows, EMA_long_windows, DIF_windows, eachPattern, patternCount)
	
	'''
	Step_5.3: Use a np array to store the ending profit of each MACD pattern
	'''
	if len(MACD_TradeArray) != 0:
		MACD_EndingProfit = np.append(MACD_EndingProfit, MACD_TradeArray[-1])

	'''
	Step_5.4: Use a Panda Data Frame to store the huge MACD_TradeArray and save it into the Database
	'''
	#print "Length of MACD_TradArray:    ", len(MACD_TradeArray)
	df_MACD_TradeArray = pd.DataFrame(data = MACD_TradeArray)

	#df_MACD_TradeArray.to_sql('tb_MACD_Trades', con=db, flavor='mysql', if_exists='append', index = False)

	'''
	Step_5.5: Empty the MACD_TradeArray to speed up the calculation
	'''
	#MACD_TradeArray = np.delete(MACD_TradeArray, np.s_[0:len(MACD_TradeArray)])
	

#Close Database connection
db.close()


'''
Step_6: Saving the ending profit for each pattern into a csv file.
'''
outputDir = '/home/marshao/DataMiningProjects/Output/'
MACD_Trade_File = outputDir + 'MACD_Trades_Signal_Pattern.csv'
#np.savetxt(MACD_Trade_File, MACD_EndingProfit, fmt="%s", delimiter=',', newline='\n', header="MACD Algrithem Trade")
np.savetxt(MACD_Trade_File, MACD_TradeArray, fmt="%s", delimiter=',', newline='\n', header="MACD Algrithem Trade")

print("Task finished")
			





