# -*- coding: utf-8 -*-

import urllib.request
import urllib3
import io
import csv
import pymongo
import json
import os
import psutil
import subprocess
from datetime import datetime
from datetime import timedelta
from datetime import date

class TwStock:
    twTwseUrl = 'http://www.twse.com.tw'
    twOtcUrl = 'http://www.otc.org.tw'
    timeZone = 8
    
    twStockUrl = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
    twStockTradeUrl = 'http://www.twse.com.tw/ch/trading/trading_days.php'
    
    dataDuringYear = 1
 
    mongodbDirName = os.getcwd() + '\mongodb'
    mongodbServer = None
    
    def __init__(self):
        pass

    def __twseDataExist(self, date):
        pass
    
    def __twseDataBaseExist(self):
        pass    
        
    def __getDailyTradeDataFromTwse(self):
        if self.urlTwseLive() != True:
            return False
        
        if False:#self.__twseDataBaseExist():
            pass
        else:
            startTime = date(self.getTwTime().year - self.dataDuringYear, self.getTwTime().month, 1)
            twseConn = urllib3.connection_from_url(self.twTwseUrl)

            while startTime != self.getTwTime().date():
                if startTime.strftime("%A") == 'Saturday' or startTime.strftime("%A") == 'Sunday':
                    startTime = startTime + timedelta(days = 1)
                    continue
                                
                twTimeFormat = date(startTime.year - 1911, startTime.month, startTime.day)
                print(twTimeFormat)        
                result = twseConn.request('POST',
                        '/ch/trading/exchange/MI_INDEX/MI_INDEX.php',
                        fields={'download': 'csv',
                                'qdate': twTimeFormat.strftime("%Y/%m/%d"),
                                'selectType': 'ALL'})
                
                if result.status != 200:
                    continue

                utfCsv = result.data.decode('big5', 'ignore').encode('utf-8', 'ignore')
                
                reader = csv.reader(io.StringIO(utfCsv.decode('utf-8', 'ignore')))
                startRowFlag = False
                breakNowFlag = 0
                
                for row in reader:
                    if startRowFlag == False:                   
                        for colume in row:                                   
                            if '查無資料' in colume:
                                breakNowFlag = 1
                                break
                            elif '證券代號' in colume:
                                startRowFlag = True
                                break
                        
                        if breakNowFlag == 1:
                            #need to tell database no data
                            break
                            
                        
                    else:
                        fixedRow = [w.replace(' ', '').replace('=','').replace('\"','') for w in row]
                        print(fixedRow)
                        
                        if fixedRow[0] == '':
                            break
                        
                                         
                        stId = fixedRow[0]              #證券代號
                        stChName = fixedRow[1]          #證券名稱
                        stOverShares = fixedRow[2]      #成交股數
                        stTradingVol = fixedRow[3]      #成交筆數
                        stOverMoney = fixedRow[4]       #成交金額
                        stOpenPrice = fixedRow[5]       #開盤價
                        stDayHighPrice = fixedRow[6]    #最高價
                        stDayLowPrice = fixedRow[7]     #最低價
                        stClosePrice = fixedRow[8]      #收盤價
                        stPER = fixedRow[15]            #本益比
                        

                        break
                        
                startTime = startTime + timedelta(days = 1)
                
            
                    
            

    def __parseRawData(self):
        pass
    
    def __saveDataToDb(self):
        pass
    
    def __startMongoDbServer(self):
        for pid in psutil.pids():
            p = psutil.Process(pid)
            if p.name() == "mongod.exe":
                p.terminate()
            
        self.mongodbServer = subprocess.Popen(
            "mongod --dbpath {0} --logpath {1}".format(self.mongodbDirName, self.mongodbDirName),
            shell=True
        )
        
        if(self.mongodbServer == None):
            print("mongo not install ?")
            return False
                 
        return True
    
    def __stopMongoDbServer(self):
        if(self.mongodbServer != None):
            self.mongodbServer.terminate()
                  
    def updateDB(self):
        #if self.__startMongoDbServer() == False:
        #    return False
        self.__getDailyTradeDataFromTwse()
        #self.__stopMongoDbServer()

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
            
    def getDataFromDB(self, ID, date):
        pass
    
    def delDataFromDB(self):
        pass
    
    def updateStockDB(self):
        pass
    
    def twseOpen(self, date):
        pass
    
    def getTwTime(self):
        return (datetime.utcnow() + timedelta(hours = self.timeZone))

    def getTodayTickData(self):
        pass
    
if __name__ == "__main__":
    test = TwStock()
    start_time = test.updateDB()
