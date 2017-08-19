# -*- coding: utf-8 -*-

import urllib.request
import urllib3
import io
import csv
import os
import psutil
import subprocess
import re
import pprint
from datetime import datetime
from datetime import timedelta
from datetime import date
from pymongo import MongoClient, collection
import logging

class TwStock:
    twTwseUrl = 'http://www.twse.com.tw'
    twOtcUrl = 'http://www.otc.org.tw'
    
    twStockUrl = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
    twStockTradeUrl = 'http://www.twse.com.tw/ch/trading/trading_days.php'
    
    mongodbDirName = os.getcwd() + '\data\mongodb'
    
    dbTitle = 'twStcok'
    collectTitle = 'stockDaily'
    stopTradeDateTitle = 'noTrade'
    
    mongodbServer = None
    db = None
    client = None
    
    timeZone = 8  
    stockDataDuringYear = 1
 
    def __init__(self, dataDuringYear = 1):
        self.stockDataDuringYear = dataDuringYear
        
        if self.__startMongoDbServer() == False:
            logging.error('mongoDB server can\'t start')
                       
    def __getDailyTradeDataFromTwse(self):
        if self.urlTwseLive() != True:
            return False
        
        startTime = date(self.getTwTime().year - self.stockDataDuringYear, self.getTwTime().month, 1)
        twseConn = urllib3.connection_from_url(self.twTwseUrl)

        while startTime != self.getTwTime().date():
            if startTime.strftime("%A") == 'Saturday' or startTime.strftime("%A") == 'Sunday':                
                data = {'time':startTime.strftime("%Y%m%d")}
                
                collection = self.db[self.stopTradeDateTitle]
                if collection.count() == 0:
                    noTradeDate ={'type':'noTrade','date':[data]}
                    collection.insert(noTradeDate)
                else:
                    collection.update({'type':'noTrade'}, {'$addToSet':{'date':data}})
                    
                startTime = startTime + timedelta(days = 1)
                continue
                                   
            result = twseConn.request('GET',
                    '/exchangeReport/MI_INDEX',
                    fields={'response': 'csv',
                            'date': startTime.strftime("%Y%m%d"),
                            'type': 'ALL'})
            
            if result.status != 200:
                data = {'time':startTime.strftime("%Y%m%d")}
                
                collection = self.db[self.stopTradeDateTitle]
                if collection.count() == 0:
                    noTradeDate ={'type':'noTrade','date':[data]}
                    collection.insert(noTradeDate)
                else:
                    collection.update({'type':'noTrade'}, {'$addToSet':{'date':data}})
                                                                            
                continue

            utfCsv = result.data.decode('big5', 'ignore').encode('utf-8', 'ignore')           
            reader = csv.reader(io.StringIO(utfCsv.decode('utf-8', 'ignore')))
            
            startRowFlag = False
            
            logging.debug(startTime,'-csv downing')             
            
            for row in reader:
                if startRowFlag == False:                   
                    for colume in row:                                   
                        if '證券代號' in colume:
                            startRowFlag = True
                            break                            
                else:   
                    if '備註' in row[0]:
                        break
                    
                    fixedRow = [w.replace(' ', '').replace('=','').replace('\"','') for w in row]                                                          
                    stId = fixedRow[0]              #證券代號
                    
                    ret = re.match(r'^\d{4}$', stId) #只需要4位數的股票,權證之類不用.
                    if ret == None:
                        continue
                                                              
                    stChName = fixedRow[1]          #證券名稱
                    stOverShares = fixedRow[2]      #成交股數
                    stTradingVol = fixedRow[3]      #成交筆數
                    stOverMoney = fixedRow[4]       #成交金額
                    stOpenPrice = fixedRow[5]       #開盤價
                    stDayHighPrice = fixedRow[6]    #最高價
                    stDayLowPrice = fixedRow[7]     #最低價
                    stClosePrice = fixedRow[8]      #收盤價
                    stPER = fixedRow[15]            #本益比
                    
                    timeData = {  'time':startTime.strftime("%Y%m%d"),
                                  'overShares':stOverShares,
                                  'tradingVol':stTradingVol,
                                  'overMoney':stOverMoney,
                                  'openPrice':stOpenPrice,
                                  'dayHightPrice':stDayHighPrice,
                                  'dayLowPrice':stDayLowPrice,
                                  'closePrice':stClosePrice,
                                  'per':stPER}
                    
                    collection = self.db[self.collectTitle]
                    
                    if collection.find({'id':stId}).count() == 0:
                        stockDailyData = {'id':stId,
                                 'name':stChName,
                                 'date':[timeData]
                        }
                        collection.insert(stockDailyData)
                    else:
                        collection.update({'id':stId}, {'$addToSet':{'date':timeData}})
                              
            startTime = startTime + timedelta(days = 1)
                     
    def __startMongoDbServer(self):
        for pid in psutil.pids():
            p = psutil.Process(pid)
            if p.name() == "mongod.exe":
                p.terminate()
            
        self.mongodbServer = subprocess.Popen(
            "mongod --dbpath {0} --logpath {1}".format(self.mongodbDirName, self.mongodbDirName + '\log'),
            shell=True
        )
        
        if(self.mongodbServer == None):
            logging.error('mongodb server can\'t be started')
            return False
        
        self.client = MongoClient()
        self.db = self.client[self.dbTitle]          
        return True
    
    def __stopMongoDbServer(self):
        if(self.mongodbServer != None):
            self.mongodbServer.terminate()
            self.client.close()
                  
    def updateDB(self):
        self.__getDailyTradeDataFromTwse()

    def urlTwseLive(self):
        with urllib.request.urlopen(self.twTwseUrl) as f:
            if f.getcode() == 200:
                return True
            else:
                return False
            
    def urlOtcLive(self):
        with urllib.request.urlopen(self.twOtcUrl) as f:
            if f.getcode() == 200:
                return True
            else:
                return False
            
    def getDailyDataFromDB(self, ID, date="00000000"):
        if date != "00000000":            
            data = self.db[self.stopTradeDateTitle].find_one({'date':{'$elemMatch':{'time':date}}})
            if data == None:
                return self.db[self.collectTitle].find_one({'id': ID},{'date':{'$elemMatch':{'time':date}}})  
            else:          
                return None;      
        else:
            return self.db[self.collectTitle].find_one({'id': ID})  
                
    def getTwTime(self):
        return (datetime.utcnow() + timedelta(hours = self.timeZone))

if __name__ == "__main__":
    test = TwStock()
    #start_time = test.updateDB()
    #test.getDailyDataFromDB('0050', '20160601')
