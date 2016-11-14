#! /usr/bin/python
# -*- coding:utf-8 -*-
"""
This file is for processing the downloaded Stock Daily Price data.

The function is to retrieve all stock names and saved them into the Access DB

"""

import re
import os
import csv

from com.ziclix.python.sql import zxJDBC


# Delair varables for reading stock codes
inputDir = '/home/marshao/DataMiningProjects/Input/'
outputDir = '/home/marshao/DataMiningProjects/Output/'
stockNameFile = outputDir + 'StockNames.csv'
stockCodes = []
stockCode =""
stockMarket = ""

# Delcair valables for setting up connection with Database 
jdbc_url = "jdbc:ucanaccess:///home/marshao/DataMiningProjects/Codes/DB_StockDataBackTest.mdb"
username = ""
password = ""
driver = "net.ucanaccess.jdbc.UcanloadDriver"
db = zxJDBC.connect(jdbc_url, username, password, driver)
cursor = db.cursor()


#"Read Stock Codes from the source folder"
for root, dirs, files in os.walk("/home/marshao/DataMiningProjects/Input"):
	for eachFile in files:
		stockMarket = eachFile[0:2]
		#print	stockMarket
		stockCode = eachFile[2:9]
		# Write Stock codes into Database
		cursor.execute("INSERT INTO tb_StockCodes (Stock_Code, Stock_Name, Stock_Market, Stock_Tradeble) VALUES ('000000','SSSSSS', 'SZ', 'True')")
		print stockCode
#for eachItem in cursor.fetchall():
#	print eachItem[1]


#cursor.execute("Create Table tb_Test (FName char(5), LName char(5))")
#Close Database connection
cursor.close()
db.close()



# Write Stock codes into a CSV file 
'''
with open(stockNameFile, "w") as f:
	for item in stockCodes:
		f.write(item)

for item in stockCodes:
	print item
'''

print("Task finished")
			

