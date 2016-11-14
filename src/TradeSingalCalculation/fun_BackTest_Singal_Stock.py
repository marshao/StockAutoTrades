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


def MACD_Profit(df_StockIndex_MACD, df_StockClosePrices, MACD_TradeArray, MACD_Tradetype, EMA_short_windows, EMA_long_windows, DIF_windows, eachPattern, patternCount):
	stockVolume_Begin = 10000
	cash_Begin = 100000
	tradeVolume = 8000
	cash_Current = cash_Begin
	stockVolume_Current = stockVolume_Begin
	
	numPrices = len(df_StockClosePrices)
	#print "numPrcies:  ",  numPrices
	j = 0
	numMACD = len(df_StockIndex_MACD)

	totalValue_Begin = (stockVolume_Begin * df_StockClosePrices.close_price[0] + cash_Begin)
	totalValue_Current = totalValue_Begin
	profit_Rate = 0

	for i in range(numMACD):

		tradeCost = df_StockClosePrices.close_price[j] * tradeVolume
		
		i = i + patternCount*numPrices
		#print "i = ", i

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


#Retrive all MACD trade pattern from DB
select_MACD_Patterns = ("SELECT Distinct MACD_pattern_number from tb_StockIndex_MACD")
MACD_pattern_number = pd.read_sql(select_MACD_Patterns, con = db)['MACD_pattern_number']
#print MACD_pattern_number

#Retrive MACD index to determine trading policy
select_StockIndex_MACD = ("SELECT * from tb_StockIndex_MACD where MACD_pattern_number = %(eachPattern)s")

#Read MACD all trade Singal
#df_StockIndex_MACD = pd.read_sql(select_StockIndex_MACD, con=db)

#id_tb_StockCodes = df_StockIndex_MACD.id_tb_StockCodes[0]
id_tb_StockCodes = pd.read_sql("SELECT Distinct id_tb_StockCodes from tb_StockIndex_MACD", con = db)['id_tb_StockCodes'][0]

# Find out unique pattern numbers
#MACD_pattern_number = df_StockIndex_MACD.MACD_pattern_number.unique()

#print MACD_pattern_number



#Retrive close_price for calculating the profit 
select_StockClosePrices = ("SELECT record_date, id_tb_StockCodes, close_price from tb_StockDailyRecords"
						" where id_tb_StockCodes = %(id_tb_stockCodes)s")

# Read Closing price and MACD Signal from DB
df_StockClosePrices = pd.read_sql(select_StockClosePrices, con=db, params={'id_tb_stockCodes':id_tb_StockCodes})
#print df_StockClosePrices

#Fetch out all exisiting stock codes
#cursor.execute(select_id_tb_StockCodes)
#id_tb_StockCodes = cursor.fetchall()
#print id_tb_StockCodes

'''
Define a numpy structured data array to store MACD profit data
'''
MACD_trade_column_names=['id_tb_StockCodes','record_date', 'tradeVolume','tradeCost',
		'stockVolume_Current', 'cash_Current','totalValue_Current', 'profit_Rate',
		 'EMA_short_window', 'EMA_long_window', 'DIF_window', 'MACD_pattern_number']
MACD_trade_column_formats = ['i','S16', 'i','f','i', 'f','f','f','i','i','i','i']

MACD_Tradetype = np.dtype({'names':MACD_trade_column_names, 'formats':MACD_trade_column_formats})

MACD_TradeArray = np.array([('0','2000/01/01','220','0','0','0','0','0','0','0','0','0')], dtype = MACD_Tradetype)

'''
Define a numpy structured data array to store Max 
'''

MACD_trade_column_names=['id_tb_StockCodes','record_date', 'tradeVolume','tradeCost',
		'stockVolume_Current', 'cash_Current','totalValue_Current', 'profit_Rate',
		 'EMA_short_window', 'EMA_long_window', 'DIF_window', 'MACD_pattern_number']
MACD_trade_column_formats = ['i','S16', 'i','f','i', 'f','f','f','i','i','i','i']

MACD_Tradetype = np.dtype({'names':MACD_trade_column_names, 'formats':MACD_trade_column_formats})

MACD_EndingProfit = np.array([('0','2000/01/01','220','0','0','0','0','0','0','0','0','0')], dtype = MACD_Tradetype)

#print MACD_TradeArray.shape
#print MACD_TradeArray['tradeVolume'][0]

#print MACD_TradeArray



#df_StockProfit = pd.DataFrame(columns=['record_date','id_tb_StockCodes','stockVolume_Current','totalValue_Current','cash_Current','tradeVolume','tradeCost'])
'''
#Fetch out closing prices for dedicated stock code
for EMA_long_windows in EMA_long_windows:
	for EMA_short_windows in EMA_short_windows:
		for DIF_windows in DIF_windows:
			MACD_TradeArray = MACD_Profit(df_StockIndex_MACD,df_StockClosePrices, MACD_TradeArray, MACD_Tradetype, EMA_short_windows, EMA_long_windows, DIF_windows)
'''
patternCount = 0
for eachPattern in MACD_pattern_number:
	
	df_StockIndex_MACD = pd.read_sql(select_StockIndex_MACD, con=db, params={'eachPattern':eachPattern})
	
	#df_StockIndex_MACD = df_StockIndex_MACD[df_StockIndex_MACD.MACD_pattern_number == eachPattern]
	EMA_short_windows = df_StockIndex_MACD["EMA_short_window"][0]
	EMA_long_windows = df_StockIndex_MACD["EMA_long_window"][0]
	DIF_windows = df_StockIndex_MACD["DIF_window"][0]


	MACD_TradeArray = MACD_Profit(df_StockIndex_MACD,df_StockClosePrices, MACD_TradeArray, MACD_Tradetype, EMA_short_windows, EMA_long_windows, DIF_windows, eachPattern, patternCount)
	
	'''
	Creat a new np array to store the ending profit of each MACD pattern
	'''
	MACD_EndingProfit = np.append(MACD_EndingProfit, MACD_TradeArray[-1])

	'''
	Create a Panda Data Frame to store the huge MACD_TradeArray and save it into the Database
	'''
	df_MACD_TradeArray = pd.DataFrame(data = MACD_TradeArray)
	df_MACD_TradeArray.to_sql('tb_MACD_Trades', con=db, flavor='mysql', if_exists='append', index = False)

	'''
	Empty the MACD_TradeArray to speed up the calculation
	'''
	np.delete(MACD_TradeArray, np.s_[:])

	#patternCount = patternCount + 1 

#print MACD_TradeArray
#print MACD_TradeArray['tradeVolume'][1]
	

#Close Database connection
#cursor.close()
#db.close()

outputDir = '/home/marshao/DataMiningProjects/Output/'
MACD_Trade_File = outputDir + 'MACD_Trades.csv'
np.savetxt(MACD_Trade_File, MACD_EndingProfit, fmt="%s", delimiter=',', newline='\n', header="MACD Algrithem Trade")


print("Task finished")
			





