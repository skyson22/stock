# -*- coding: utf-8 -*-

import logging

from wmm.data.tw_stock import TwStock

def initLogging():
    FORMAT = '[%(asctime)s][%(levelname)s] %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, filename='log/system.log')
     
def main():
    initLogging()
    test = TwStock()
    test.updateDB()

if __name__ == '__main__':
    main()