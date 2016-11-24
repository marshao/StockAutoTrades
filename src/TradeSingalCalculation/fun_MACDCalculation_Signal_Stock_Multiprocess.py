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
from functools import partial
import pyprind
import progressbar
import copy


'''
Function to calculate trading sigal for each pattern
'''
def MACD_Cal(df_StockRecords, MACD_Indexes, pattern_loop):
	#print "process id =  ", os.getpid()
	print "process ", pattern_loop, " started"
	bar = pyprind.ProgBar(100, track_time=False, title='MACD Transaction Signal Calculation')
	#os.system("taskset -cp %d %d" %(pattern_loop, os.getpid()))
	numRecords = len(df_StockRecords)
	j = 0
	for index in MACD_Indexes:
		short_window = index[1]
		long_window = index[0]
		dif_window = index[2]
		df_StockRecords['EMA_short'] = pd.ewma(df_StockRecords.close_price, span=short_window)
		df_StockRecords['EMA_long'] = pd.ewma(df_StockRecords.close_price, span=long_window)
		df_StockRecords['DIF'] = df_StockRecords.EMA_short - df_StockRecords.EMA_long
		df_StockRecords['DEA'] = pd.rolling_mean(df_StockRecords.DIF, window = dif_window)
		df_StockRecords['MACD']=2.0*(df_StockRecords.DIF - df_StockRecords.DEA)		
		df_StockRecords['Signal'] = 0
		df_StockRecords['EMA_short_window'] = short_window
		df_StockRecords['EMA_long_window'] = long_window
		df_StockRecords['DIF_window'] = dif_window
		df_StockRecords['MACD_Pattern_Number'] = j + pattern_loop*len(MACD_Indexes)
		j += 1
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

		#print df_StockRecords
		#df_TransactionResult = copy.deepcopy(df_StockRecords)
		#df_TransactionResult.drop('close_price', axis=1, inplace=True)

		cols_to_write = ['record_date','id_tb_StockCodes','close_price','EMA_short','EMA_long','DIF','DEA',
						'MACD','Signal','EMA_short_window','EMA_long_window','DIF_window','MACD_Pattern_Number']
		'''
		Database Writer
		'''
		df_StockRecords.iloc[:,[0,1,3,4,5,6,7,8,9,10,11,12]].to_sql('tb_StockIndex_MACD', con=db, flavor='mysql', if_exists='append', index = False)
		bar.update()

	print(bar)
	return





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
select_MACDIndex_count = ("SELECT count(*) as num from tb_MACDIndex")

#Fetch out PK id of StockCode
cursor.execute(select_id_tb_StockCode, data_id_tb_StockCode)
id_tb_StockCodes = cursor.fetchall()

#print id_tb_StockCodes[0]



'''
Setting up progress bar with progressbar module to monitor the progress of whole program.

widgets = ['MACD_Pattern_BackTest: ', 
			progressbar.Percentage(), ' ', 
			progressbar.Bar(marker='0',left='[',right=']'),' ', 
			progressbar.ETA()]

progress = progressbar.ProgressBar(widgets = widgets)
'''
'''
Setting up progress bar with pyprind module to monitor the progress of whole program
'''
#bar = pyprind.ProgBar(100, track_time=False, title='MACD Transaction Signal Calculation')

#Fetch out required MACD_Index
#cursor.execute(select_MACDIndex_window, data_MACDIndex_window)

#MACD_Indexes = cursor.fetchall()

cursor.execute(select_MACDIndex_count)

MACD_Total = cursor.fetchone()
#print MACD_Total
i = MACD_Total[-1]/7

#print i

pattern_loop = 0

#print ("work start:{0}".format(time.ctime()))

cursor.execute(select_MACDIndex_all)
'''
Test
'''
'''
df_StockRecords = pd.read_sql(select_StockRecords, con=db, params={'id_tb_stockCodes':id_tb_StockCodes[0]})
MACD_Indexes = cursor.fetchmany(i)
p1 = mp.Process(target = MACD_Cal, args=(df_StockRecords,MACD_Indexes, pattern_loop) )

pattern_loop += 1
MACD_Indexes = cursor.fetchmany(i)
p2 = mp.Process(target = MACD_Cal, args=(df_StockRecords,MACD_Indexes, pattern_loop) )

pattern_loop += 1
MACD_Indexes = cursor.fetchmany(i)
p3 = mp.Process(target = MACD_Cal, args=(df_StockRecords,MACD_Indexes, pattern_loop) )

p1.start()
p2.start()
p3.start()
p1.join()
p2.join()
p3.join()
'''
task_args = []

for pattern_loop in range(7):
	MACD_Indexes = cursor.fetchmany(i)


	if MACD_Indexes == ():
		
		print 'Break'
		break
	

	df_StockRecords = pd.read_sql(select_StockRecords, con=db, params={'id_tb_stockCodes':id_tb_StockCodes[0]})
	task_args.append((df_StockRecords,MACD_Indexes, pattern_loop,))

	pattern_loop += 1	

	# Call MACD calculation funtion in singal processing
	
	#MACD_Cal(df_StockRecords,MACD_Indexes, pattern_loop)


	
# Call Multiprocess with Pool

pool = mp.Pool(7)
		
for t in task_args:
	pool.apply_async(MACD_Cal, t)

#result= [pool.apply_async(MACD_Cal, t) for t in task_args]

pool.close()

pool.join()
	
	
	#print "process id =  ", os.getpid()
	#

	

# Call Multiprocess without Pool

	
#	p = mp.Process(target = MACD_Cal, args=(df_StockRecords,MACD_Indexes, pattern_loop) )
#	p.daemon = True
#	p.start()
#	p.join()
	

	




#Close Database connection
cursor.close()
db.close()

#print(bar)
#print ("work finish:{0}".format(time.ctime()))
print("Task Finished")
			





