#! /usr/bin/python
# -*- coding:utf-8 -*-

'''
Use TA_Lib package to calculate Parabolic SAR indicator

'''

import numpy as np
import pandas as pd
import talib as ta
import MySQLdb
import matplotlib.pyplot as plt


# Delcair valables for setting up connection to local MySQL Server 
db = MySQLdb.connect(host='127.0.0.1',
	port=3306,
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')



# Select half hour trading records
select_StockClosePrices_HalfHour = ("SELECT record_time, id_tb_StockCodes, close_price, high_price, low_price from tb_StockHalfHourRecords"
						" where id_tb_StockCodes = %(id_tb_stockCodes)s")

id_tb_stockCodes = 16

df_HalHourRecords = pd.read_sql(select_StockClosePrices_HalfHour, con=db, params={'id_tb_stockCodes':id_tb_stockCodes})

df_Analysis = pd.DataFrame(index = df_HalHourRecords.record_time)



df_Analysis['Current'] = df_HalHourRecords.close_price.as_matrix() 

high_price = df_HalHourRecords.high_price.as_matrix()

low_price = df_HalHourRecords.low_price.as_matrix()


df_Analysis['SAR_1'] = abs(ta.SAREXT(high=high_price, low=low_price, 
					startvalue = 51.99,
					offsetonreverse = 0,
					accelerationinitlong            = 0.02,
        			accelerationlong                = 0.02,
        			accelerationmaxlong             = 0.1,
        			accelerationinitshort           = 0.02,
        			accelerationshort               = 0.02,
        			accelerationmaxshort            = 0.1))

df_Analysis['SAR_2'] = abs(ta.SAREXT(high=high_price, low=low_price, 
					startvalue = 51.99,
					offsetonreverse = 0,
					accelerationinitlong            = 0.02,
        			accelerationlong                = 0.02,
        			accelerationmaxlong             = 0.2,
        			accelerationinitshort           = 0.02,
        			accelerationshort               = 0.02,
        			accelerationmaxshort            = 0.2))

df_Analysis['SAR_3'] = abs(ta.SAREXT(high=high_price, low=low_price, 
					startvalue = 51.99,
					offsetonreverse = 0,
					accelerationinitlong            = 0.02,
        			accelerationlong                = 0.01,
        			accelerationmaxlong             = 0.1,
        			accelerationinitshort           = 0.02,
        			accelerationshort               = 0.01,
        			accelerationmaxshort            = 0.1))
df_Analysis['SAR_4'] = abs(ta.SAREXT(high=high_price, low=low_price, 
					startvalue = 51.99,
					offsetonreverse = 0,
					accelerationinitlong            = 0.02,
        			accelerationlong                = 0.01,
        			accelerationmaxlong             = 0.2,
        			accelerationinitshort           = 0.02,
        			accelerationshort               = 0.01,
        			accelerationmaxshort            = 0.2))


outputDir = '/home/marshao/DataMiningProjects/Output/'
SAR_Trade_File = outputDir + 'SAR_Trades_HalfHour.csv'
df_Analysis.to_csv(SAR_Trade_File, spe=',')


'''
plt.scatter(x,y, color = 'black')
plt.plot(df_Analysis.SAR, color = 'Red')
plt.plot(df_Analysis.Current, color = 'Blue')
'''
print df_Analysis

db.close()


