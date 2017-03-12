# -*- coding: utf-8 -*-

import urllib.request
from datetime import datetime
from datetime import timedelta

class TwStock:
    twTwseUrl = 'http://www.twse.com.tw'
    twOtcUrl = 'http://www.otc.org.tw'
    timeZone = 8
    
    twStockUrl = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
    twStockTradeUrl = 'http://www.twse.com.tw/ch/trading/trading_days.php'
    
    def __init__(self):
        pass
    
    def __getRawDataFromUrl(self):
        pass

    def __parseRawData(self):
        pass

    def __saveDataToDb(self):
        pass

    def url_TwseIsLive(self):
        with urllib.request.urlopen(self.twTwseUrl) as f:
            if f.getcode() == 200:
                return True
            else:
                return False
            
    def url_OtcIsLive(self):
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
    
    def isOpenStock(self, date):
        pass
    
    def getTwTime(self):
        return (datetime.utcnow() + timedelta(hours = self.timeZone))

    def getTodayTickData(self):
        pass
    
if __name__ == "__main__":
    test = TwStock()
    print(test.getTwTime())
