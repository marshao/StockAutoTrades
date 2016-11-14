#! /usr/bin/python
# -*- coding:utf-8 -*-
"""
This file will try to calculate the Statistic Identities(Mean, Standard Diviation, Max, Min, Ending) of eahc pattern 

Step_1: Read some patterns (Top 30) trade detail data into a Pandas_DF.
Step_2: Calculate the statistic identities and save into a new Pandas_DF.
Step_3: Save the new Pandas_DF into a file or DB 

top_30 patterns: the top 30 patterns with largest ending profit.

20161019: Find out the profit_rates has only 2 digist after decimal, not enough for calculation. Changed it to 4 in tb_MACD_Trades.

20161026: 
1. Added function to calculate statistics for HalfHour MACD Data
2. Added an new view vw_MACD_EndingProfit_HalfHour in DB


"""

import re
import os
import csv
import MySQLdb
import pandas as pd
import math
from sklearn import datasets, linear_model
import matplotlib.pyplot as plt
import numpy as np
import scipy


'''
Function to calculate Statistic Identities for each pattern
'''
def Statistic_Cal(df_pattern_tradeDetails):
	numRecords = len(df_pattern_tradeDetails)
	#print df_pattern_tradeDetails[-1:]
	#print df_pattern_tradeDetails.tail(1)
	df_pattern_statisic = df_pattern_tradeDetails.tail(1)
	df_pattern_statisic['count'] = numRecords
	df_pattern_statisic['mean'] = df_pattern_tradeDetails['profit_Rate'].mean()
	df_pattern_statisic['max'] = df_pattern_tradeDetails['profit_Rate'].max()
	df_pattern_statisic['min'] = df_pattern_tradeDetails['profit_Rate'].min()
	df_pattern_statisic['median'] = df_pattern_tradeDetails['profit_Rate'].median()
	df_pattern_statisic['var'] = df_pattern_tradeDetails['profit_Rate'].var()
	#


	'''
	Using the profit_rate as y and record_date as x to do the linear regression and draw a plot line.
	'''

	x = np.arange(0,numRecords, dtype=np.float)
	y = df_pattern_tradeDetails.profit_Rate.values
	#x = df_pattern_tradeDetails.record_date.values
	'''
	Solution_1: Using sklearn model to do linear regression.

	#Reshape data from 1 row to 1 colume
	x = x.reshape(numRecords, 1)
	y = y.reshape(numRecords, 1)

	regr = linear_model.LinearRegression()
	regr.fit(x,y)

	df_pattern_statisic['intercept']=regr.intercept_
	df_pattern_statisic['Slope'] = regr.coef_
	'''
	'''
	Solution_2: Using scipy to do linear regression. Simple and easy
	'''
	Slope,Intercept,df_pattern_statisic['R_Value'],df_pattern_statisic['P_Value'], df_pattern_statisic['Std_err'] = scipy.stats.linregress(x,y)
	df_pattern_statisic['Slope'] = Slope
	df_pattern_statisic['Intercept'] = Intercept
	line = Intercept + Slope * x
	
	'''
	# Using the matplot to show the graphs
	
	
	plt.scatter(x,y, color = 'black')
	#plt.plot(x, regr.predict(x), color = 'blue', linewidth = 3)
	plt.title(df_pattern_statisic.MACD_pattern_number)
	plt.plot(line, 'red')
	plt.xticks(())
	plt.yticks(())
	plt.title(df_pattern_statisic.MACD_pattern_number)
	plt.show()
	'''

	print df_pattern_statisic
	
	return df_pattern_statisic





# Delair varables for reading stock codes
inputDir = '/home/marshao/DataMiningProjects/Input/'
outputDir = '/home/marshao/DataMiningProjects/Output/'
MACD_pattern_ending_statistic = outputDir + 'MACD_pattern_ending_statistic.csv'
MACD_pattern_ending_statistic_HalfHour = outputDir + 'MACD_pattern_ending_statistic_HalfHour.csv'
stockCode ="300216"
stockMarket = ""
tops = 300

# Delcair valables for setting up connection to local MySQL Server 
db = MySQLdb.connect(host='127.0.0.1',
	port=3306,
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

cursor = db.cursor()

'''
#Select Daily MACD_pattern_number of top_30 patterns from vw_MACD_EndingProfits 
select_MACD_pattern_number_Daily = ("SELECT MACD_pattern_number from vw_MACD_EndingProfits order by profit_Rate DESC limit %(tops)s")
cursor.execute(select_MACD_pattern_number_Daily,{'tops':tops})
'''

#Select HalfHour MACD_pattern_number of top_30 patterns from vw_MACD_EndingProfits 
select_MACD_pattern_number_HalfHour = ("SELECT MACD_pattern_number from vw_MACD_EndingProfits_HalfHour order by profit_Rate DESC limit %(tops)s")
cursor.execute(select_MACD_pattern_number_HalfHour,{'tops':tops})
MACD_pattern_number = cursor.fetchall()

'''
#Fetch out all Daily MACD pattern transactions for top_30 patterns
select_patttern_tradeDetails = ("SELECT * from tb_MACD_Trades"
						" where MACD_pattern_number = %(eachPattern)s")
'''

#Fetch out all HalHour MACD pattern transactions for top_30 patterns
select_patttern_tradeDetails_HalfHour = ("SELECT * from tb_MACD_Trades_HalfHour"
						" where MACD_pattern_number = %(eachPattern)s")

df_pattern_statisic = pd.DataFrame()

i = 0

for eachPattern in MACD_pattern_number:
	# loop breaker
	if i == -1:
		break
	i = i + 1


	#print eachPattern
	df_pattern_tradeDetails = pd.read_sql(select_patttern_tradeDetails_HalfHour, con=db, params = {'eachPattern':eachPattern})
	#print df_pattern_tradeDetails
	df_data = Statistic_Cal(df_pattern_tradeDetails)

	df_pattern_statisic = df_pattern_statisic.append(df_data)



#Close Database connection
cursor.close()
db.close()

'''
Write to file
'''

df_pattern_statisic.to_csv(MACD_pattern_ending_statistic_HalfHour)



print("Task finished")
			





