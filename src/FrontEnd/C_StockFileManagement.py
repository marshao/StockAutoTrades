#!/usr/local/bin/python
# coding: GBK

import MySQLdb
import os, time, pandas
from datetime import datetime
from sqlalchemy import create_engine


__metclass__ = type


class C_StockFileManagement:
    def __init__(self):
        self._output_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\'
        self._input_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._install_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\'
        self._stock_record_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._stock_inhand_file = 'stockInhand.csv'
        self._log_mesg = ''
        self._op_log = 'operLog.txt'
        self._stock_code = ''
        self._engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')

    def __del__(self):
        pass

    def _is_file_exist(self, path):
        return os.path.isfile(path)


    def read_stock_inhand_to_db(self):
        fullPath = self._output_dir + self._stock_inhand_file
        data =[]
        line = []
        col_count = 12
        i = 0

        if self._is_file_exist(fullPath):
            lines = open(fullPath, 'r').read().split()
            cursor = self._get_cursor()
            for word in lines:
                if i > 11:
                    if (i % col_count) == 0 and i == 12:
                        line.append(self._convert_encoding(word, 'UTF-8'))
                        i += 1
                    elif(i % col_count) == 0 and i != 12:
                        data.append(line)
                        line = []
                        line.append(self._convert_encoding(word, 'UTF-8'))
                        i += 1
                    elif (i % col_count) == 2:
                        line.append(int(self._convert_encoding(word, 'UTF-8')))
                        i += 1
                    elif (i % col_count) == 3:
                        line.append(int(self._convert_encoding(word, 'UTF-8')))
                        i += 1
                    elif (i % col_count) == 4:
                        line.append(float(self._convert_encoding(word, 'UTF-8')))
                        i += 1
                    elif (i % col_count) == 5:
                        line.append(float(self._convert_encoding(word, 'UTF-8')))
                        i += 1
                    elif (i % col_count) == 6:
                        line.append(float(self._convert_encoding(word, 'UTF-8')))
                        i += 1
                    elif (i % col_count) == 7:
                        line.append(float(self._convert_encoding(word, 'UTF-8')))
                        i += 1
                    elif (i % col_count) == 8:
                        line.append(float(self._convert_encoding(word, 'UTF-8')))
                        i += 1
                    elif (i % col_count) == 11:
                        line.append(float(self._convert_encoding(word, 'UTF-8')))
                        i += 1
                    else:
                        i += 1
                else:
                    i += 1
            i = 0

            df = pandas.DataFrame(data)
            df.columns = ['stockCode','stockRemain','stockAvaliable', 'baseCost','currentCost','currentValue','profit','profitRate','averageBuyPrice']
            df.to_sql('tb_StockInhand', self._engine, if_exists='append', index=False)

            self._log_mesg = 'Stock inhand information saved successfully at ', self._time_tag()
        else:
            self._log_mesg = 'There is no stock inhand file exist at ', self._time_tag()

        self._write_log(self._log_mesg, self._op_log)

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local

    def _write_log(self,log_mesg, logPath):
        fullPath = self._output_dir + logPath
        with open(fullPath, 'a') as log:
            log.writelines(log_mesg)

    def _convert_encoding(self, lines, new_coding='UTF-8'):
        try:
            encoding = 'GB2312'
            data = lines.decode(encoding)
            data = data.encode(new_coding)
        except:
            data = 'DecodeError'
        return data

    def read_stock_code_to_db(self):
        add_stock_code = ("INSERT INTO tb_StockCodes "
                          "(stock_code, stock_name, stock_market, stock_trad) "
                          "SELECT * FROM (SELECT %(stockCode)s,%(stockName)s, %(stockMarket)s, %(stockTrad)s ) AS tmp "
                          "WHERE NOT EXISTS "
                          "(SELECT stock_code FROM tb_StockCodes WHERE stock_code = %(stockCode)s) LIMIT 1 ")
        connection = self._engine.connect()
        trans = connection.begin()
        # "Read Stock Codes from the source folder"
        for root, dirs, files in os.walk(self._input_dir):
            for eachFile in files:

                file = self._input_dir + eachFile
                # rint eachFile[2:8]
                with open(file, 'r') as f:
                    content = f.readline()
                # print content[7:20]

                data_stock_code = {
                    'stockCode': eachFile[2:8],
                    'stockName': self._convert_encoding(content[7:16]),
                    'stockMarket': eachFile[0:2],
                    'stockTrad': 1,
                    'stockCode': eachFile[2:8],
                }
                # Write Stock codes into Database
                try:
                    connection.execute(add_stock_code, data_stock_code)
                    trans.commit()
                    print "stock code added"
                except:
                    print "stock add failed"
                    trans.rollback()


def main():
    fm = C_StockFileManagement()

    #fm.read_stock_inhand_to_db()
    fm.read_stock_code_to_db()


if __name__ == '__main__':
    main()