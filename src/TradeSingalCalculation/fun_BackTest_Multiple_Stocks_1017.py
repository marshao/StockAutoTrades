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


def MACD_Profit(df_StockIndex_MACD, df_StockClosePrices, MACD_TradeArray, MACD_Tradetype):
	stockVolume_Begin = 10000
	cash_Begin = 100000
	tradeVolume = 8000
	cash_Current = cash_Begin
	stockVolume_Current = stockVolume_Begin
	
	numPrices = len(df_StockClosePrices)
	numMACD = len(df_StockIndex_MACD)
	#numTrades = len(MACD_TradeArray)

	totalValue_Begin = (stockVolume_Begin * df_StockClosePrices.close_price[0] + cash_Begin)
	
	for i in range(numMACD):
		tradeCost = df_StockClosePrices.close_price[i] * tradeVolume
		if df_StockIndex_MACD.Signal[i] == 1: # Positive Signal, buy more stocks
			
			if cash_Current >= tradeCost:
				#tradeVolume = stockVolume_Current * tradePercent
				stockVolume_Current = tradeVolume + stockVolume_Current
				cash_Current = cash_Current - tradeCost
				totalValue_Current = stockVolume_Current * df_StockClosePrices.close_price[i] + cash_Current
				profit_Rate = math.log(totalValue_Current/totalValue_Begin)
				#MACD_TradeArray[numTrades] = np.array([(df_StockIndex_MACD.id_tb_StockCodes[0],df_StockIndex_MACD.record_date[i-1],
				#	tradeVolume,tradeCost,stockVolume_Current,cash_Current,totalValue_Current,profit_Rate)],dtype = MACD_Tradetype)
				MACD_TradeArray = np.append(MACD_TradeArray, np.array([(df_StockIndex_MACD.id_tb_StockCodes[0],df_StockIndex_MACD.record_date[i-1],tradeVolume,tradeCost,stockVolume_Current,cash_Current,totalValue_Current,profit_Rate)], dtype = MACD_Tradetype))

				#numTrades = numTrades + 1
			else:
				pass	


		elif df_StockIndex_MACD.Signal[i]==(-1): # NEgative Signal, sell stocks
			if stockVolume_Current >= tradeVolume:
				stockVolume_Current = stockVolume_Current - tradeVolume
				cash_Current = cash_Current + tradeCost
				totalValue_Current = stockVolume_Current * df_StockClosePrices.close_price[i] + cash_Current
				profit_Rate = math.log(totalValue_Current/totalValue_Begin)
				#MACD_TradeArray[numTrades] = np.array([(df_StockIndex_MACD.id_tb_StockCodes[0],df_StockIndex_MACD.record_date[i-1],
				#	tradeVolume,tradeCost,stockVolume_Current,cash_Current,totalValue_Current,profit_Rate)],dtype = MACD_Tradetype)
				MACD_TradeArray = np.append(MACD_TradeArray, np.array([(df_StockIndex_MACD.id_tb_StockCodes[0],df_StockIndex_MACD.record_date[i-1],tradeVolume,tradeCost,stockVolume_Current,cash_Current,totalValue_Current,profit_Rate)], dtype = MACD_Tradetype))
				#numTrades = numTrades + 1
			else:
				pass	
		else:	
			pass

	#totalValue_End = totalValue_Current 
	#profit_Rate = math.log(totalValue_Current/totalValue_Begin)
	#profitRate = (totalValue_End - totalValue_Begin)/totalValue_Begin
	print  df_StockIndex_MACD.id_tb_StockCodes[0], totalValue_Begin, totalValue_Current, profit_Rate
	#print numMACD, numPrices 
	return MACD_TradeArray


# Delcair valables for setting up connection to local MySQL Server 
db = MySQLdb.connect(host='127.0.0.1',
	port=3306,
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

cursor = db.cursor()

#Select all existing StockCodes from DB
select_id_tb_StockCodes = ("SELECT id_tb_StockCodes, stock_code from tb_StockCodes")

#Retrive close_price for calculating the profit 
select_StockClosePrices = ("SELECT record_date, id_tb_StockCodes, close_price from tb_StockDailyRecords"
						" where id_tb_StockCodes = %(id_tb_stockCodes)s")

#Retrive MACD index to determine trading policy
select_StockIndex_MACD = ("SELECT * from tb_StockIndex_MACD where id_tb_StockCodes = %(id_tb_stockCodes)s")

#Fetch out all exisiting stock codes
cursor.execute(select_id_tb_StockCodes)
id_tb_StockCodes = cursor.fetchall()
#print id_tb_StockCodes

'''
Define a numpy structured data array to store MACD profit data
'''
MACD_trade_column_names=['id_tb_StockCodes','record_date', 'tradeVolume','tradeCost',
		'stockVolume_Current', 'cash_Current','totalValue_Current', 'profit_Rate']
MACD_trade_column_formats = ['i','S16', 'i','f','i', 'f','f','f']

MACD_Tradetype = np.dtype({'names':MACD_trade_column_names, 'formats':MACD_trade_column_formats})

MACD_TradeArray = np.array([('0','2000-00-00','220','0','0','0','0','0')], dtype = MACD_Tradetype)
#print MACD_TradeArray.shape
#print MACD_TradeArray['tradeVolume'][0]

#print MACD_TradeArray


#df_StockProfit = pd.DataFrame(columns=['record_date','id_tb_StockCodes','stockVolume_Current','totalValue_Current','cash_Current','tradeVolume','tradeCost'])

#Fetch out closing prices for dedicated stock code
for each_id_tb_StockCode in id_tb_StockCodes:
	# Read Closing price and MACD Signal from DB
	df_StockClosePrices = pd.read_sql(select_StockClosePrices, con=db, params={'id_tb_stockCodes':each_id_tb_StockCode[0]})
	df_StockIndex_MACD = pd.read_sql(select_StockIndex_MACD, con=db, params={'id_tb_stockCodes':each_id_tb_StockCode[0]})	
	#print df_StockIndex_MACD.Signal
	MACD_TradeArray = MACD_Profit(df_StockIndex_MACD,df_StockClosePrices, MACD_TradeArray, MACD_Tradetype)

#print MACD_TradeArray
print MACD_TradeArray['tradeVolume'][1]
	

#Close Database connection
cursor.close()
db.close()

outputDir = '/home/marshao/DataMiningProjects/Output/'
MACD_Trade_File = outputDir + 'MACD_Trades.csv'
np.savetxt(MACD_Trade_File, MACD_TradeArray, fmt="%s", delimiter=',', newline='\n', header="MACD Algrithem Trade")


print("Task finished")
			





