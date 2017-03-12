# -*- coding: utf-8 -*-

import urllib.request

class TwStock:
    twTwseUrl = 'http://www.twse.com.tw'
    twOtcUrl = 'http://www.otc.org.tw'
    
    twStockUrl = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
    twStockTradeUrl = 'http://www.twse.com.tw/ch/trading/trading_days.php'
    saveStockDataDir = 'dataSave/stock/tw/daily/'
    csvFileName = 'twTwseStock'
    
    def __init__(self):
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
            
    def getRawDataFromUrl(self):
        pass
    
    def parseRawData(self):
        pass
    
    def saveDataToDb(self):
        pass
    
    def getDataFromDB(self, ID):
        pass
    
    def delDataFromDB(self, ID):
        pass
    
if __name__ == "__main__":
    test = TwStock()
    print(test.url_TwseIsLive(), test.url_OtcIsLive())
