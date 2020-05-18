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
logBase     = config['logging']['logBase'] + '.modules.dataProcessing'

@lD.log(logBase + '.getJoinedData')
def getJoinedData(logger, fileDict):

    cachedFile = fileDict['rawCombined']

    try:
        if not os.path.exists( cachedFile ):

            sqlString = '''
                SELECT ca.capturedDate, bu.business_vertical, co.country, ca.region, ca.city_code, ca.strategy_id, ch.channel_name, ca.goal_type, 
                    ca.total_spend_cpm, ca.impressions, ca.clicks, ca.conversions
                FROM 
                    camp_data_tbl ca
                        LEFT JOIN 
                    business_vertical_tbl bu ON ca.business_vertical_id = bu.business_vertical_id
                        LEFT JOIN
                    country_tbl co ON ca.country_id = co.country_id
                        LEFT JOIN
                    channel_name_tbl ch ON ca.channel_name_id = ch.channel_name_id
            '''

            db                   = PostgresDB()
            res                  = db.query( sqlString )
            data                 = pd.DataFrame( res, columns=['capturedDate', 'business_vertical', 'country', 'region', 'city_code', 
                                                               'strategy_id', 'channel_name', 'goal_type', 'total_spend_cpm', 'impressions', 
                                                               'clicks', 'conversions'] )

            if not os.path.exists( os.path.dirname(cachedFile) ):
                os.makedirs( os.path.dirname(cachedFile) )

            data.to_parquet( cachedFile, engine='pyarrow', compression='snappy' )

        else:
            data = pd.read_parquet( path=cachedFile, engine='pyarrow' )
        
        return data

    except Exception as e:
        logger.error('Failed to get joined data \n{}'.format(str(e)))

@lD.log(logBase + '.getCleanseData_handleNUll')
def getCleanseData_handleNUll(logger, fileDict):

    try:
        if not os.path.exists( fileDict['cleansed'] ):

            jumboData                 = getJoinedData( fileDict )

            ## Assuming the null values under goal_type column as 3rd goal type, 
            ## which is different from goal type 1 & 2. Hence, to fill null goal_type as 3.
            jumboData['goal_type']    = jumboData['goal_type'].fillna(3)
            jumboData['goal_type']    = jumboData['goal_type'].astype(np.int)

            ## Assuming the null values under conversions column as "converted"
            jumboData['conversions']  = jumboData['conversions'].fillna(1)

            ## Assuming the null values under impressions column follows the median of impressions by country and region
            impressionMapping         = jumboData[ jumboData['impressions'].notnull() ].groupby(['country', 'region'])['impressions'].agg(np.median)
            impressionMapping         = impressionMapping.reset_index()
            impressionMapping.columns = ['country', 'region', 'median_impressions']
            jumboData                 = pd.merge(jumboData, impressionMapping, on=['country', 'region'], how='left')
            jumboData['impressions']  = jumboData['impressions'].fillna( jumboData['median_impressions'] )

            ## Assuming the null values under clicks column follows the median of clicks by country and region
            clicksMapping             = jumboData[ jumboData['clicks'].notnull() ].groupby(['country', 'region'])['clicks'].agg(np.median)
            clicksMapping             = clicksMapping.reset_index()
            clicksMapping.columns     = ['country', 'region', 'median_clicks']
            jumboData                 = pd.merge(jumboData, clicksMapping, on=['country', 'region'], how='left')
            jumboData['clicks']       = jumboData['clicks'].fillna( jumboData['median_clicks'] )

            if not os.path.exists( os.path.dirname(fileDict['cleansed']) ):
                os.makedirs( os.path.dirname(fileDict['cleansed']) )

            jumboData.to_parquet( fileDict['cleansed'], engine='pyarrow', compression='snappy' )

        else:
            jumboData = pd.read_parquet( path=fileDict['cleansed'], engine='pyarrow' )
        
        return jumboData

    except Exception as e:
        logger.error('Unable to clean data \n{}'.format(str(e)))

@lD.log(logBase + '.generateETLResults')
def generateETLResults(logger, fileDict):

    try:
        jumboData       = getCleanseData_handleNUll( fileDict )

        ### Generate total impressions, total clicks, and total conversions by country
        aggByCountry    = jumboData.groupby(['country'])[['impressions', 'clicks', 'conversions']].agg(sum).reset_index()
        
        ### Insert the dataset into a database using batch insertion
        db              = PostgresDB()
        db.push_df_sequential( aggByCountry, tablename='public.aggByCountry', 
                               colnames=['country', 'impressions', 'clicks', 'conversions'], 
                               coltypes=['text', 'double precision', 'double precision', 'double precision'], 
                               batch_size=100, drop_table_if_exist=True )
        aggByCountry.to_csv('../results/aggByCountry.csv', index=False) 

        ### Generate total impressions, total clicks, and total conversions by channel_name
        aggByChannel    = jumboData.groupby(['channel_name'])[['impressions', 'clicks', 'conversions']].agg(sum).reset_index()

        ### Insert the dataset into a database using batch insertion
        db              = PostgresDB()
        db.push_df_sequential( aggByChannel, tablename='public.aggByChannel', 
                               colnames=['channel_name', 'impressions', 'clicks', 'conversions'], 
                               coltypes=['text', 'double precision', 'double precision', 'double precision'], 
                               batch_size=100, drop_table_if_exist=True )
        aggByChannel.to_csv('../results/aggByChannel.csv', index=False)  

        ### Generate total impressions, total clicks, and total conversions by business_vertical
        aggByBusinessVertical = jumboData.groupby(['business_vertical'])[['impressions', 'clicks', 'conversions']].agg(sum).reset_index()

        ### Insert the dataset into a database using batch insertion
        db                    = PostgresDB()
        db.push_df_sequential( aggByBusinessVertical, tablename='public.aggByBusinessVertical', 
                               colnames=['business_vertical', 'impressions', 'clicks', 'conversions'], 
                               coltypes=['text', 'double precision', 'double precision', 'double precision'], 
                               batch_size=100, drop_table_if_exist=True )
        aggByBusinessVertical.to_csv('../results/aggByBusinessVertical.csv', index=False)   

        ### Select strategy_id =3718750. 
        ### Show the cumulative sum of impressions and clicks partitioned by channel_name and region over date in ascending order
        specificStrategy = jumboData[ jumboData['strategy_id'] == 3718750 ] 
        cumulativeSumDF  = []
        for index, df in specificStrategy.groupby(['channel_name', 'region']):
            df_sorted                      = df.sort_values('capturedDate').reset_index(drop=True)[['capturedDate', 'impressions', 'clicks']]
            df_sorted['impressions_cumul'] = df_sorted['impressions'].cumsum()
            df_sorted['clicks_cumul']      = df_sorted['clicks'].cumsum()
            df_sorted['channel_name']      = index[0]
            df_sorted['region']            = index[1]
            cumulativeSumDF.append( df_sorted )

        cumulativeSumDF  = pd.concat( cumulativeSumDF )
        db               = PostgresDB()

        ### Insert the dataset into a database using batch insertion
        db.push_df_sequential( cumulativeSumDF, tablename='public.cumulativeSumDF_strat_3718750', 
                               colnames=['capturedDate', 'impressions', 'clicks', 'impressions_cumul', 'clicks_cumul', 'channel_name', 'region'], 
                               coltypes=['date', 'double precision', 'double precision', 'double precision', 'double precision', 'text', 'text'], 
                               batch_size=100, drop_table_if_exist=True )
        cumulativeSumDF.to_csv('../results/cumulativeSumDF_strat_3718750.csv', index=False)   

        results = {
            'aggByCountry'          : aggByCountry,
            'aggByChannel'          : aggByChannel,
            'aggByBusinessVertical' : aggByBusinessVertical,
            'cumulativeSumDF'       : cumulativeSumDF
        }

        return results

    except Exception as e:
        logger.error('Unable to generate ETL results \n{}'.format(str(e)))

@lD.log(logBase + '.getCreatedFeatures')
def getCreatedFeatures(logger, fileDict):

    try:
        if not os.path.exists( fileDict['engineered'] ):
            
            jumboData_clean                   = getCleanseData_handleNUll(fileDict)

            jumboData_clean['yearCaptured']   = jumboData_clean['capturedDate'].apply(lambda dt: dt.year)
            jumboData_clean['YYYYMMCaptured'] = jumboData_clean['capturedDate'].apply(lambda dt: '{}{:02d}'.format(dt.year, dt.month))

            jumboData_clean.to_csv( fileDict['engineered'], index=False )
        
        else:
            jumboData_clean = pd.read_parquet( path=fileDict['engineered'], engine='pyarrow' )

        return jumboData_clean

    except Exception as e:
        logger.error('Unable to create features \n{}'.format(str(e)))

@lD.log(logBase + '.main')
def main(logger, resultsDict):

    try:
        fileDict = {
            'rawCombined' : '../data/intermediate/jumboData.snappy.parquet',
            'cleansed'    : '../data/intermediate/jumboData_cleansed.snappy.parquet',
            'engineered'  : '../data/intermediate/jumboData_cleansed_engineered.snappy.parquet'
        }
        jumboData            = getJoinedData( fileDict )
        jumboData_clean      = getCleanseData_handleNUll( fileDict )
        
        generateETLResults(fileDict)
        jumboData_engineered = getCreatedFeatures( fileDict )

    except Exception as e:
        logger.error('Unable to run main \n{}'.format(str(e)))

if __name__ == '__main__':

    pass
    # jumboData = getJoinedData( cachedFile='../data/intermediate/jumboData_cleansed_engineered.snappy.parquet' )