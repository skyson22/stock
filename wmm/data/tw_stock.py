# -*- coding: utf-8 -*-

import urllib.request
import urllib3
import io
import csv
import os
import psutil
import subprocess
import re
from html.parser import HTMLParser
from dateutil.relativedelta import relativedelta
from datetime import datetime
from datetime import timedelta
from datetime import date
from pymongo import MongoClient, collection
import logging

class RevenueDataHTMLParser(HTMLParser):
    dataList = []
    total = []
    startFlag = False

    def handle_data(self, data):
        new_data = data.replace(' ','').replace(',','')   
        ret = re.match(r'^\d{4}$', new_data) #只需要4位數的股票,權證之類不用.
        if ret != None and self.startFlag == False:
            self.startFlag = True
            self.dataList.append(new_data)  
        elif len(self.dataList) < 10 and self.startFlag == True:
            self.dataList.append(new_data)
        elif len(self.dataList) == 10:
            copy_data = self.dataList[:]
            self.total.append(copy_data)
            self.startFlag = False
            self.dataList[:] = []
            
    def getParserData(self):
        return_list = self.total[:]
        self.total[:] = [] 
        return return_list
            
class TwStock:
    twTwseUrl = 'http://www.twse.com.tw'
    twTpexUrl = 'http://www.tpex.org.tw'
    twRevenueDataUrl = 'http://mops.twse.com.tw/nas/t21/sii/t21sc03_'
    
    mongodbDirName = os.getcwd() + '\data\mongodb'
    
    dbTitle = 'twStcok'
    collectTitle = 'stockData'
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
            
    def __updateNoTradeMongoDb(self, date):
        data = {'time':date}
        collection = self.db[self.stopTradeDateTitle]
        if collection.count() == 0:
            noTradeDate ={'type':'noTrade','date':[data]}
            collection.insert(noTradeDate)
        else:
            collection.update({'type':'noTrade'}, {'$addToSet':{'date':data}})
            
    def __getAllTradeFromUrl(self, twseConn, saveTimeFormat):
        '''
                    主要更新為每日交易資料
        '''
        result = twseConn.request('GET',
                    '/exchangeReport/MI_INDEX',
                    fields={'response': 'csv',
                            'date': saveTimeFormat,
                            'type': 'ALL'})
            
        if result.status != 200:    
            self.__updateNoTradeMongoDb(saveTimeFormat)
            raise Exception('the twse url can not connecting')                                                    
                
        reader = csv.reader(io.StringIO(result.data.decode('big5', 'ignore')))
        
        logging.debug('csv download ={}'.format(saveTimeFormat))
        
        startRowFlag = False          
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
                
                timeData = {  'time':saveTimeFormat,
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
                             'daily':[timeData]
                    }
                    collection.insert(stockDailyData)
                else:
                    collection.update({'id':stId}, {'$addToSet':{'daily':timeData}})
                    
        if startRowFlag == False:
            self.__updateNoTradeMongoDb(saveTimeFormat)
            raise Exception('the csv do not have useful data')
        
        return True
    def __isHoliday(self, startTime):
        if startTime.strftime("%A") == 'Saturday' or startTime.strftime("%A") == 'Sunday':                
            self.__updateNoTradeMongoDb(startTime.strftime("%Y%m%d"))
            raise Exception('{} is a holiday'.format(startTime.strftime("%Y%m%d")))
        return False
    
    def __getSellingStockShort(self, twseConn, saveTimeFormat):
        '''
                    當日融券賣出與借券賣出成交量值
        '''
        result = twseConn.request('GET',
                    '/exchangeReport/TWTASU',
                    fields={'response': 'csv',
                            'date': saveTimeFormat})
            
        if result.status != 200:    
            self.__updateNoTradeMongoDb(saveTimeFormat)
            raise Exception('the twse TWTASU url can not connecting')                                                    
                
        reader = csv.reader(io.StringIO(result.data.decode('big5', 'ignore')))
        
        logging.debug('TWTASU csv download ={}'.format(saveTimeFormat))
        
        startRowFlag = False    
        for row in reader:
            if startRowFlag == False:                   
                for colume in row:                                   
                    if '證券名稱' in colume:
                        startRowFlag = True
                        break                            
            else:   
                if '合計' in row[0]:
                    break
                
                fixedRow = [w.replace(',','').replace('=','').replace('\"','') for w in row]                                                         
                splitList = fixedRow[0].split()
                stId = splitList[0] #證券代號
                
                ret = re.match(r'^\d{4}$', stId) #只需要4位數的股票,權證之類不用.
                if ret == None:
                    continue
                                                                          
                stSellStockShortCount = fixedRow[1]          #融券賣出成交數量
                stSellStockShortMoney = fixedRow[2]      #融券賣出成交金額
                stStockLendCount = fixedRow[3]      #借券賣出成交數量
                stStockLendMoney = fixedRow[4]       #借券賣出成交金額
                                        
                collection = self.db[self.collectTitle]
                collection.update({'id':stId,'daily.time':saveTimeFormat}, {'$set':{'daily.$.sellStockShortCount':stSellStockShortCount,
                                                                                   'daily.$.sellStockShortMoney':stSellStockShortMoney,
                                                                                   'daily.$.stockLendCount':stStockLendCount,
                                                                                   'daily.$.stockLendMoney':stStockLendMoney}})     
    
    def __getInstitutionalInvestorsData(self, twseConn, saveTimeFormat):
        '''
                    三大法人買賣超日報
        '''
        result = twseConn.request('GET',
                    '/fund/T86',
                    fields={'response': 'csv',
                            'date': saveTimeFormat,
                            'selectType':'ALL'})
            
        if result.status != 200:    
            self.__updateNoTradeMongoDb(saveTimeFormat)
            raise Exception('the twse T86 url can not connecting')                                                    
                
        reader = csv.reader(io.StringIO(result.data.decode('big5', 'ignore')))
        
        logging.debug('T86 csv download ={}'.format(saveTimeFormat))
        
        startRowFlag = False    
        for row in reader:
            if startRowFlag == False:                   
                for colume in row:                                   
                    if '證券代號' in colume:
                        startRowFlag = True
                        break                            
            else:   
                if '說明' in row[0]:
                    break
                
                fixedRow = [w.replace(' ', '').replace('=','').replace('\"','') for w in row]
                                                           
                stId = fixedRow[0] #證券代號            
                ret = re.match(r'^\d{4}$', stId) #只需要4位數的股票,權證之類不用.
                if ret == None:
                    continue
                                                                          
                stForeignInvestorsBuyStockNum = fixedRow[2]          #外資買進股數
                stForeignInvestorsSellStockNum = fixedRow[3]      #外資賣出股數
                stForeignInvestorsBuyOrSellStockNum = fixedRow[4]      #外資買賣超股數
                
                stInvestmentTrustBuyStockNum = fixedRow[5]       #投信買進股數
                stInvestmentTrustSellStockNum = fixedRow[6]       #投信賣出股數
                stInvestmentTrustBuyOrSellStockNum = fixedRow[7]       #投信買賣超股數
                
                stDealerBuyOrSellStockNum = fixedRow[8]       #自營商買賣超股數
                
                stDealerBuyStockNumBySelf = fixedRow[9]       #自營商買進股數(自行買賣)
                stDealerSellStockNumBySelf = fixedRow[10]       #自營商賣出股數(自行買賣)
                stDealerBuyOrSellStockNumBySelf = fixedRow[11]       #自營商買賣超股數(自行買賣)
                
                stDealerBuyStockNumHedge = fixedRow[12]       #自營商買進股數(避險)
                stDealerSellStockNumHedge = fixedRow[13]       #自營商賣出股數(避險)
                stDealerBuyOrSellStockNumHedge = fixedRow[14]       #自營商買賣超股數(避險)
                
                stInstitutionalInvestorsBuyOrSell = fixedRow[15]       #三大法人買賣超股數
                                        
                collection = self.db[self.collectTitle]
                collection.update({'id':stId,'daily.time':saveTimeFormat}, {'$set':{'daily.$.foreignInvestorsBuyStockNum':stForeignInvestorsBuyStockNum,
                                                                                   'daily.$.foreignInvestorsSellStockNum':stForeignInvestorsSellStockNum,
                                                                                   'daily.$.foreignInvestorsBuyOrSellStockNum':stForeignInvestorsBuyOrSellStockNum,
                                                                                   'daily.$.investmentTrustBuyStockNum':stInvestmentTrustBuyStockNum,
                                                                                   'daily.$.investmentTrustSellStockNum':stInvestmentTrustSellStockNum,
                                                                                   'daily.$.investmentTrustBuyOrSellStockNum':stInvestmentTrustBuyOrSellStockNum,
                                                                                   'daily.$.dealerBuyOrSellStockNum':stDealerBuyOrSellStockNum,
                                                                                   'daily.$.dealerBuyStockNumBySelf':stDealerBuyStockNumBySelf,
                                                                                   'daily.$.dealerSellStockNumBySelf':stDealerSellStockNumBySelf,
                                                                                   'daily.$.dealerBuyOrSellStockNumBySelf':stDealerBuyOrSellStockNumBySelf,
                                                                                   'daily.$.dealerBuyStockNumHedge':stDealerBuyStockNumHedge,
                                                                                   'daily.$.dealerSellStockNumHedge':stDealerSellStockNumHedge,
                                                                                   'daily.$.dealerBuyOrSellStockNumHedge':stDealerBuyOrSellStockNumHedge,
                                                                                   'daily.$.institutionalInvestorsBuyOrSell':stInstitutionalInvestorsBuyOrSell}})
                
    def __getYieldRatePERPBR(self, twseConn, saveTimeFormat):
        '''
                    個股日本益比、殖利率及股價淨值比
        '''
        result = twseConn.request('GET',
                    '/exchangeReport/BWIBBU_d',
                    fields={'response': 'csv',
                            'date': saveTimeFormat,
                            'selectType':'ALL'})
            
        if result.status != 200:    
            self.__updateNoTradeMongoDb(saveTimeFormat)
            raise Exception('the twse PERPBR url can not connecting')                                                    
                
        reader = csv.reader(io.StringIO(result.data.decode('big5', 'ignore')))
        
        logging.debug('PERPBR csv download ={}'.format(saveTimeFormat))
        
        startRowFlag = False
        stYieldRateIndex = 0
        stPBRIndex = 0     
        for row in reader:
            if startRowFlag == False:
                index = 0                   
                for colume in row:                                   
                    if '證券代號' in colume:
                        startRowFlag = True
                    elif '殖利率' in colume:
                        stYieldRateIndex = index
                    elif '股價淨值比' in colume:
                        stPBRIndex = index             
                    index += 1
            else:
                if row[0] == "":
                    break
                
                fixedRow = [w.replace('-', '').replace(' ', '').replace('=','').replace('\"','') for w in row]                                                        
                stId = fixedRow[0] #證券代號
                
                ret = re.match(r'^\d{4}$', stId) #只需要4位數的股票,權證之類不用.
                if ret == None:
                    continue
                                                                          
                stYieldRate = fixedRow[stYieldRateIndex]      #殖利率(%)
                stPBR = fixedRow[stPBRIndex]       #股價淨值比
                                        
                collection = self.db[self.collectTitle]
                collection.update({'id':stId,'daily.time':saveTimeFormat}, {'$set':{'daily.$.yieldRate':stYieldRate,
                                                                                   'daily.$.pBR':stPBR}})  
                   
    def __getDailyTradeDataFromTwse(self):
        if self.urlTwseLive() != True:
            return False
        
        startTime = date(self.getTwTime().year - self.stockDataDuringYear, self.getTwTime().month, 1)
        twseConn = urllib3.connection_from_url(self.twTwseUrl)

        while startTime != self.getTwTime().date():            
            saveTimeFormat = startTime.strftime("%Y%m%d")
            if self.__dailyDataIsSavedInMongoDB('0050', saveTimeFormat) == True:
                    continue
            try:   
                self.__isStopTradeInMongoDB(saveTimeFormat)
                self.__isHoliday(startTime)
                self.__getAllTradeFromUrl(twseConn, saveTimeFormat)
                self.__getSellingStockShort(twseConn, saveTimeFormat)
                self.__getInstitutionalInvestorsData(twseConn, saveTimeFormat)
                self.__getYieldRatePERPBR(twseConn, saveTimeFormat)
            except Exception as mes:
                logging.debug(mes)
            finally:  
                startTime = startTime + timedelta(days = 1)
                
    def __startMongoDbServer(self):
        for pid in psutil.pids():
            p = psutil.Process(pid)
            if p.name() == "mongod.exe":
                p.terminate()
            
        self.mongodbServer = subprocess.Popen(
            "mongod --dbpath {0} --logpath {1} --wiredTigerCacheSizeGB=2".format(self.mongodbDirName, self.mongodbDirName + '\log'),
            shell=True
        )
        
        if(self.mongodbServer == None):
            logging.error('mongodb server can\'t be started')
            return False
        
        self.client = MongoClient()
        self.client.admin.command({'setParameter': 1, 'internalQueryExecMaxBlockingSortBytes': 536870912})  
        self.db = self.client[self.dbTitle]       
        return True
    
    def __getRevenueData(self, date):
        '''
                    營業收入, 累計營業收入
        '''
        twTimeFormat = '{}_{}'.format(date.year - 1911, date.month)
        saveTimeFormat = date.strftime("%Y%m%d") 
        url = '{}{}_0.html'.format(self.twRevenueDataUrl, twTimeFormat)
        http = urllib3.PoolManager()
        r = http.request('GET', url)
        parser = RevenueDataHTMLParser()
        parser.feed(r.data.decode('big5', 'ignore'))
        print(date)
        parserData = parser.getParserData()
        
        for w in parserData:           
            stId = w[0] #公司代號
            stThisMonthRevenue = w[2] #當月營收
            stLastMonthRevenue = w[3] #上月營收
            stLastYearTheSameMonthRevenue = w[4] #去年當月營收
            stCompareLastMonthPercent = w[5] #上月比較增減(%)
            stCompareLastYearTheSameMonthPerent = w[6] #去年同月增減(%)
            stThisMonthAccumulatedRevenue = w[7] #當月累計營收
            stLastYearTheSameMonthAccumulatedRevenue = w[8] #去年累計營收
            stCompareLastYearAccumulatedRevenuePercent = w[9] #前期比較增減(%)
            
            if self.__monthDataIsSavedInMongoDB(stId, saveTimeFormat) == True:
                raise Exception('month data have the {} data'.format(saveTimeFormat))
                  
            timeData = {  'time':saveTimeFormat,
                          'thisMonthRevenue':stThisMonthRevenue,
                          'lastMonthRevenue':stLastMonthRevenue,
                          'lastYearTheSameMonthRevenue':stLastYearTheSameMonthRevenue,
                          'compareLastMonthPercent':stCompareLastMonthPercent,
                          'compareLastYearTheSameMonthPerent':stCompareLastYearTheSameMonthPerent,
                          'thisMonthAccumulatedRevenue':stThisMonthAccumulatedRevenue,
                          'lastYearTheSameMonthAccumulatedRevenue':stLastYearTheSameMonthAccumulatedRevenue,
                          'compareLastYearAccumulatedRevenuePercent':stCompareLastYearAccumulatedRevenuePercent}

            collection = self.db[self.collectTitle]
            
            if collection.find({'$and':[{'id':stId}, {'month':{'$exists':'true'}}]}).count() == 0:   
                collection.update({'id':stId}, {'$set':{'month':[timeData]}})     
            else:
                collection.update({'id':stId}, {'$addToSet':{'month':timeData}})             
    
    def __getMonthData(self):
        startTime = date(self.getTwTime().year - self.stockDataDuringYear, self.getTwTime().month, 1)
        while startTime <= self.getTwTime().date():
            if self.__monthDataIsSavedInMongoDB('0050',startTime.strftime("%Y%m%d")) == True:
                continue
            try:
                self.__getRevenueData(startTime)
            except Exception as mes:
                logging.debug(mes)
            finally:
                startTime = startTime + relativedelta(months=1)
    
    def __stopMongoDbServer(self):
        if(self.mongodbServer != None):
            self.mongodbServer.terminate()
            self.client.close()
            
    def __isStopTradeInMongoDB(self, date):
        data = self.db[self.stopTradeDateTitle].find_one({'date':{'$elemMatch':{'time':date}}})
        if data == None:
            return False
        raise Exception('{} is stop trade'.format(date))
    
    def __dailyDataIsSavedInMongoDB(self, ID, date):
        data = self.db[self.collectTitle].find_one({'$and':[{'id': ID}, {'daily':{'$elemMatch':{'time':date}}}]})
        if data == None:
            return False  
        return True
    
    def __monthDataIsSavedInMongoDB(self, ID, date):
        data = self.db[self.collectTitle].find_one({'$and':[{'id': ID}, {'month':{'$elemMatch':{'time':date}}}]})
        if data == None:
            return False  
        return True
                  
    def updateDB(self):
        self.__getDailyTradeDataFromTwse()
        self.__getMonthData()
        return True

    def urlTwseLive(self):
        with urllib.request.urlopen(self.twTwseUrl) as f:
            if f.getcode() == 200:
                return True
            else:
                return False
            
    def urlTpexLive(self):
        with urllib.request.urlopen(self.twTpexUrl) as f:
            if f.getcode() == 200:
                return True
            else:
                return False
            
    def getDailyDataFromMongoDB(self, ID, date="00000000"):
        if date != "00000000":            
            data = self.db[self.stopTradeDateTitle].find_one({'date':{'$elemMatch':{'time':date}}})
            if data == None:
                return self.db[self.collectTitle].find_one({'$and':[{'id': ID}, {'daily':{'$elemMatch':{'time':date}}}]})
            else:          
                return None;      
        else:
            return self.db[self.collectTitle].find_one({'id': ID})  
                
    def getTwTime(self):
        return (datetime.utcnow() + timedelta(hours = self.timeZone))
    
    def getAllTradeDataFromMongoDB(self):
        return self.db[self.collectTitle].find()
    
if __name__ == "__main__":
    test = TwStock()
    #start_time = test.updateDB()
    #test.getDailyDataFromDB('0050', '20160601')
