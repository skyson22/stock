# -*- coding: utf-8 -*-

import logging
import pprint
import cProfile

from wmm.data.tw_stock import TwStock

def initLogging():
    FORMAT = '[%(asctime)s][%(levelname)s] %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, filename='log/system.log')
     
def main():
    initLogging()
    tw_stock = TwStock()
    if tw_stock.updateDB() == True:
        print('finish')
        pass
        allData = tw_stock.getAllTradeDataFromMongoDB()
        
        
    #tradeDatas = test.getAllTradeDataFromMongoDB()
    #for data in tradeDatas:
    #    pprint.pprint(data)

if __name__ == '__main__':
    #cProfile.run("main()")
    main()