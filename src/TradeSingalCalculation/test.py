import MySQLdb

db = MySQLdb.connect(host='127.0.0.1',
	user='root', 
	passwd='123', 
	db='DB_StockDataBackTest')

cursor = db.cursor()

cursor.execute("SELECT stock_name from tb_StockCodes where stock_code = '300226'")

name = cursor.fetchall()

print name

cursor.close()
db.close()