from logs import logDecorator as lD 
from lib.dbConnect import PostgresDB

import os
import pandas as pd
import numpy as np
import jsonref
from typing import List, Optional
import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
import itertools
import time

config      = jsonref.load(open('../config/config.json'))
logBase     = config['logging']['logBase'] + '.modules.warmStart'

@lD.log(logBase + '.readCSV')
def readCSV(logger):

    try:
        business_vertical = pd.read_csv( '../data/business_vertical.csv' )
        camp_data         = pd.read_csv( '../data/camp_data.csv' )
        channel_name      = pd.read_csv( '../data/channel_name.csv' )
        country           = pd.read_csv( '../data/country.csv' )

        dataInfo          = '''
                'business_vertical(shape): {bvShape}'
                'camp_data(shape)        : {cdShape}'
                'channel_name(shape)     : {cnShape}'
                'country(shape)          : {cShape}'
        '''.format(bvShape=business_vertical.shape, cdShape=camp_data.shape, cnShape=channel_name.shape, cShape=country.shape)

        print( 'Shapes of CSV files:' )
        print( dataInfo )

    except Exception as e:
        logger.error('Failed to read CSV \n{}'.format(str(e)))

@lD.log(logBase + '.testFetchData')
def testFetchData(logger):

    try:
        sqlString    = '''
                       SELECT *
                       FROM camp_data_tbl
        '''

        db           = PostgresDB()
        dataIterator = db.queryIterator( sqlString=sqlString )

        data         = []
        for d in dataIterator:
            data.extend(d)

        print('len(data)', len(data))

    except Exception as e:
        logger.error('Failed to fetch data \n{}'.format(str(e)))

@lD.log(logBase + '.testMakeChangesToDB')
def testMakeChangesToDB(logger):

    try:
        db           = PostgresDB()
        db.create_table(tablename='public.test', colnames=['a', 'b'], coltypes=['int', 'char'])

        data         = pd.DataFrame({'a': list(range(10000)), 'b': ['a'] * 10000})

        db           = PostgresDB()
        db.push_df_sequential( data, tablename='public.test', colnames=['a', 'b'], coltypes=['int', 'char'], 
                               batch_size=1000, drop_table_if_exist=True )
    
    except Exception as e:
        logger.error('Failed to make changes to db \n{}'.format(str(e)))

@lD.log(logBase + '.main')
def main(logger, resultsDict):

    try:
        readCSV()
        testFetchData()
        testMakeChangesToDB()

    except Exception as e:
        logger.error('Unable to run main \n{}'.format(str(e)))

if __name__ == '__main__':

    pass