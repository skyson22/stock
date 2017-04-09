# -*- coding: utf-8 -*-

import urllib.request
import urllib3
import io
import csv
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
                    continue
                                
                twTimeFormat = date(startTime.year - 1911, startTime.month, startTime.day)
                
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
                for row in reader:
                    if startRowFlag == False:
                        if '證券代號' in row:
                            startRowFlag = True
                            continue
                    else:
                        fixedRow = [w.replace(' ', '').replace('=','').replace('\"','') for w in row]
                        print(fixedRow)
                        
                        if fixedRow[0] == '':
                            break
                        
                startTime = startTime + timedelta(days = 1)
                
            
            
            

    def __parseRawData(self):
        pass

    def __saveDataToDb(self):
        pass
        
    def updateDB(self):
        self.__getDailyTradeDataFromTwse()
        pass

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
