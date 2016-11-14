#! /usr/bin/python
# -*- coding:utf-8 -*-
"""

The function is try to use the Access DB

"""

#import re
#import os, sys, getpass, shlex, subprocess
#import csv
#import pyodbc

from com.ziclix.python.sql import zxJDBC


jdbc_url = "jdbc:ucanaccess:///home/marshao/DataMiningProjects/Codes/DB_StockDataBackTest.mdb"
username = ""
password = ""
driver = "net.ucanaccess.jdbc.UcanloadDriver"

db = zxJDBC.connect(jdbc_url, username, password, driver)
cursor = db.cursor()

cursor.execute("SELECT * FROM tb_StockCodes")
for eachItem in cursor.fetchall():
	for item in eachItem:
		print item

cursor.close()
db.close()

#dbDir = '/home/marshao/DataMiningProjects/Codes/'
#dbFile = 'DB_StockDataBackTest.mdb'
#dbPath = os.path.join(dbDir, dbFile)
#print dbPath
#odbc_connection_str = 'DRIVER={MDBTools};DBQ=%s;'%(dbPath)
#connection = pyodbc.connect(odbc_connection_str)
#cursor = connection.cursor()

#table_names = subprocess.Popen(["mdb-tables","-1", dbPath], stdout=subprocess.PIPE).communicate()[0]
#create_table = subprocess.Popen(["mdb-sql", dbPath])
#print table_names