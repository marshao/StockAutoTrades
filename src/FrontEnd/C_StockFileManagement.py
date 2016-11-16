#!/usr/local/bin/python
# coding: GBK

import os, time, pandas, shutil
from datetime import datetime
from sqlalchemy import create_engine


__metclass__ = type


class C_StockFileManagement:


    '''
    This class is a file management class: it is mainly do 3 things:
    1. Read stock in hand information into DB with function read_stock_inhand_to_db
    2. Read stock codes into DB with function read_stock_code_to_db
        2.1 This function has a option to decide whether process records with stock code or not. If yes, it will
        call read_stocks_to_db after processing stock code.
    3. Read stock records into DB with funtion read_stock_records_to_db
        3.1 This function take either directory or file as source
        3.2 When the source is a file, it can be called from other functions

    '''
    def __init__(self):
        self._output_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\output\\'
        self._input_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._install_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\\'
        self._stock_record_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\input\\'
        self._processed_dir = 'D:\Personal\DataMining\\31_Projects\\01.Finance\\03.StockAutoTrades\processed\\'
        self._stock_inhand_file = 'stockInhand.csv'
        self._log_mesg = ''
        self._op_log = 'operLog.txt'
        self._stock_code = ''
        self._engine = create_engine('mysql+mysqldb://marshao:123@10.175.10.231/DB_StockDataBackTest')

    def __del__(self):
        pass

    def _is_file_exist(self, path):
        return os.path.isfile(path)


# read stock in hand information into database
    def read_stock_inhand_to_db(self):
        fullPath = self._output_dir + self._stock_inhand_file
        data =[]
        line = []
        col_count = 12
        i = 0

        if self._is_file_exist(fullPath):
            lines = open(fullPath, 'r').read().split()
            # cursor = self._get_cursor()
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
            self._move_processed_file(self._stock_inhand_file)
            self._log_mesg = 'Stock inhand information saved successfully at ', self._time_tag()
        else:
            self._log_mesg = 'There is no stock inhand file exist at ', self._time_tag()

        self._write_log(self._log_mesg, self._op_log)

    def _time_tag(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.now()
        only_date = time_stamp.date()
        return time_stamp_local


    def _time_tag_dateonly(self):
        time_stamp_local = time.asctime(time.localtime(time.time()))
        time_stamp = datetime.now()
        only_date = time_stamp.date()
        return only_date

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

    def read_stock_code_to_db(self, with_data=False):
        '''
    
        :param with_data: is a True/False value, if True, this function will call read_stock_records_to_db to process srock records into DB
        :return:
        '''
        add_stock_code = ("INSERT INTO tb_StockCodes "
                            "(stock_code, stock_name, stock_market, stock_trad) "
                            "SELECT * FROM (SELECT %(stockCode)s,%(stockName)s, %(stockMarket)s, %(stockTrad)s ) AS tmp "
                            "WHERE NOT EXISTS "
                            "(SELECT stock_code FROM tb_StockCodes WHERE stock_code = %(stockCode)s) LIMIT 1 ")
    
        # "Read Stock Codes from the source folder"
        for root, dirs, files in os.walk(self._input_dir):
            if len(files) != 0:  # If the input dir is not a empty folder
                connection = self._engine.connect()
                trans = connection.begin()
                for eachFile in files:
    
                    file = self._input_dir + eachFile
                    # print eachFile[2:8]
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
                        self._log_mesg = "Stock code %s is added successfully at %s" % (eachFile[2:8], self._time_tag())
                    except:
                        self._log_mesg = "Stock code %s is failed to be added at %s" % (eachFile[2:8], self._time_tag())
                        trans.rollback()
                    if with_data:
                        self.read_stock_records_to_db(eachFile)
    
                    self._move_processed_file(eachFile)
                    self._write_log(self._log_mesg, self._op_log)
     
    def read_stock_records_to_db(self, source, dest):
        add_StockDailyRecords = ("INSERT INTO tb_StockDailyRecords "
                                 "(record_date, open_price, high_price,low_price, close_price, total_volume, total_amount, id_tb_StockCodes)"
                                 " VALUES (%(tradeDate)s,%(oPrice)s,%(hPrice)s,%(lPrice)s,%(cPrice)s,%(volume)s,%(amount)s,%(id_tb_StockCode)s)")
    
        select_id_tb_StockCodes = ("SELECT id_tb_StockCodes from tb_StockCodes where stock_code = %(stockCode)s LIMIT 1")
    
        connection = self._engine.connect()
        trans = connection.begin()
        file = self._input_dir + source
        if dest == 'Daily':
            table = 'tb_StockDailyRecords'
        elif dest == 'HalfHour':
            table = 'tb_StockHalfHourRecords'
        else:
            self._log_mesg = "Please indicate the correct record time sequence, either Daily or HalfHour, read records failed at ", self._time_tag()
            print self._log_mesg
            self._write_log(self._log_mesg, self._op_log)
            return
    
        if os.path.isdir(source):
            # "Read Stock Records from the source folder"
            for root, dirs, files in os.walk(self._input_dir):
                if len(files) != 0:
                    for eachFile in files:
                        file = self._input_dir + eachFile
                        stockCode = eachFile[2:8]
                        data_id_tb_StockCodes = {'stockCode': stockCode}

                        con = connection.execute(select_id_tb_StockCodes, data_id_tb_StockCodes)
                        for rows in con:
                            id_tb_StockCode = rows['id_tb_StockCodes']                              
                        con.close()
                        
                        list_StockDailyRecords=[]
                        
                        with open(file, 'r') as f:
                            content = f.readlines()[2:len(f.readlines()) - 1]  # read lines start from the 3rd line
                            # content=''.join(content).strip('\n')
                            for line in content:
                                elements = line.split("\t")
                                # print elements[6]
                                t = time.strptime(elements[0], "%m/%d/%Y")
                                y, m, d = t[0:3]
                                tradeDate = datetime(y, m, d)
                                oPrice = elements[1],
                                hPrice = elements[2],
                                lPrice = elements[3],
                                cPrice = elements[4],
                                volume = elements[5],
                                amount = ''.join(elements[6]).strip('\r\n'),                               
                                list_StockDailyRecords.append([tradeDate,oPrice, hPrice, lPrice,cPrice, volume,amount, id_tb_StockCode])
                                
                        df_StockDailyRecords = pandas.DataFrame(list_StockDailyRecords)
                        df_StockDailyRecords.columns = ['record_date','open_price', 'high_price', 'low_price','close_price', 'total_volume' ,'total_amount', 'id_tb_StockCodes']
                        df_StockDailyRecords.to_sql('tb_StockDailyRecords', self._engine, if_exists='append', index=False)
                        self._log_mesg = "Stock records of stock %s is added successfully at %s" % (id_tb_StockCode, self._time_tag())
                        self._move_processed_file(eachFile)
                        self._write_log(self._log_mesg, self._op_log)
                        print self._log_mesg
                        return 
        elif os.path.isfile(file):
            # file = self._input_dir + source
            stockCode = source[2:8]
            data_id_tb_StockCodes = {'stockCode': stockCode}
            con = connection.execute(select_id_tb_StockCodes, data_id_tb_StockCodes)
            
            for rows in con:
                id_tb_StockCode = rows['id_tb_StockCodes']  
            con.close()
            
            list_StockDailyRecords=[]
                        
            with open(file, 'r') as f:
                content = f.readlines()[2:len(f.readlines()) - 1]  # read lines start from the 3rd line
                # content=''.join(content).strip('\n')
                for line in content:
                    elements = line.split("\t")
                    # print elements[6]
                    t = time.strptime(elements[0], "%m/%d/%Y")
                    y, m, d = t[0:3]
                    tradeDate = datetime(y, m, d)
                    oPrice = elements[1],
                    hPrice = elements[2],
                    lPrice = elements[3],
                    cPrice = elements[4],
                    volume = elements[5],
                    amount = ''.join(elements[6]).strip('\r\n'),                               
                    list_StockDailyRecords.append([tradeDate,oPrice, hPrice, lPrice,cPrice, volume,amount, id_tb_StockCode])
                                
            df_StockDailyRecords = pandas.DataFrame(list_StockDailyRecords)
            df_StockDailyRecords.columns = ['record_date','open_price', 'high_price', 'low_price','close_price', 'total_volume' ,'total_amount', 'id_tb_StockCodes']
            df_StockDailyRecords.to_sql('tb_StockDailyRecords', self._engine, if_exists='append', index=False)
            self._log_mesg = "Stock records of stock %s is added successfully at %s" % (id_tb_StockCode, self._time_tag())
            self._write_log(self._log_mesg, self._op_log)
            print self._log_mesg    
            return
        else:
            self._log_mesg = "The provided source is not valid at %s" % self._time_tag()
            print self._log_mesg
            self._write_log(self._log_mesg, self._op_log)
            return
    
    
    def _move_processed_file(self, processed_file_name):
        '''
        Rename the processed file with a time stamp, copy it to the processed folder and remove the source file.
        '''
        now_day = self._time_tag_dateonly()
        new_file = self._processed_dir + str(now_day) + processed_file_name
        processed_file = self._input_dir + processed_file_name
        shutil.copyfile(processed_file, new_file)
        time.sleep(2)
        os.remove(processed_file)



def main():
    fm = C_StockFileManagement()
    path = fm._input_dir
    fm.read_stock_records_to_db(path)
    #fm.read_stock_inhand_to_db()
    #fm._move_processed_file('new.csv')


if __name__ == '__main__':
    main()