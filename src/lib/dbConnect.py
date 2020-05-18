import psycopg2
import numpy as np
import jsonref
from tqdm import tqdm

from logs import logDecorator as lD 
from typing import List, Optional

config   = jsonref.load(open('../config/config.json'))
logBase  = config['logging']['logBase'] + '.lib.dbConnect'

class PostgresDB:

    @lD.log(logBase + '.PostgresDB.__init__')
    def __init__(logger, self, configFile: str ='../config/lib/dbConnect/dbConfig.json'):
        """
        Connect to the databse
        """

        try:
            dbConfig     = jsonref.load( open(configFile, 'r') )
            connInfo     = dbConfig['dbLogin']['connInfo']
            connStr      = "host={host} user={user} dbname={dbname} password={password} port={port}".format( **connInfo )

            self.conn    = psycopg2.connect( connStr )

        except Exception as e:
            logger.error('Unable to initialize PostgresDB \n{}'.format(str(e)))

    @lD.log(logBase + '.PostgresDB.disconnect')
    def disconnect(logger, self):
        """
        Disconnect from the database. If this fails, for instance
        if the connection instance doesn't exist, ignore the exception.
        """

        try:
            self.cursor.close()
            self.conn.close()
        
        except Exception as e:
            logger.error('Encounter disconnection error: {}'.format(str(e)))

    @lD.log(logBase + '.PostgresDB.exec_sql')
    def exec_sql(logger, self, sqlString, tuple_values: Optional[list] =None):
        '''Provide sqlString to perform changes onto postgres DataBase
        
        Decorators:
            lD.log
        
        Arguments:
            logger : {logging.Logger} :
                the logger function
            self : {instance of dbConnect} :
                current instance
            sqlString : {string} :
                string of SQL statement to be run
            tuple_values : {tuple} :
                tuple of input values SQL statement require
        '''

        try:
            if tuple_values is None:
                self.cursor.execute(sqlString)
            else:
                self.cursor.execute(sqlString, tuple_values)

        except Exception as e:
            logger.error('Unable to execute sql: {}'.format(str(e)))

    @lD.log(logBase + '.PostgresDB.commit_sql')
    def commit_sql(logger, self):
        '''Commit changes applied on postgres DataBase
        
        Decorators:
            lD.log
        
        Arguments:
            logger : {logging.Logger} :
                the logger function
            self : {instance of dbConnect} :
                current instance
        '''

        try:
            self.conn.commit()
            
        except Exception as e:
            logger.error('Unable to commit sql: {}'.format(str(e)))

    @lD.log(logBase + '.PostgresDB.query')
    def query(logger, self, sqlString: str, values: Optional[list] =None) -> list:
        
        try:
            
            self.cursor  = self.conn.cursor()
            self.exec_sql( sqlString, values )

            results      = self.cursor.fetchall()
            
            self.disconnect()
            
            return results

        except Exception as e:
            logger.error('Unable to query: {} \n{}'.format(query, str(e)))

    @lD.log(logBase + '.PostgresDB.queryIterator')
    def queryIterator(logger, self, sqlString: str, values: Optional[list] =None, numRows: float =1000) -> list:
        
        try:
            
            self.cursor  = self.conn.cursor()
            self.exec_sql( sqlString, values )

            while True:

                ### Escape from database-type error (internet interruption & etc).
                try:
                    res  = self.cursor.fetchmany(int(numRows))
                except Exception as e:
                    print('Error: {}'.format(str(e)))
                    break
                
                ### End of query to be fetched
                if len(res) == 0:
                    break
                
                yield res

            self.disconnect()
            
        except Exception as e:
            logger.error('Unable to create query iterator: {} \n{}'.format(query, str(e)))

    @lD.log(logBase + '.PostgresDB.create_table')
    def create_table(logger, self, tablename, colnames, coltypes):
        '''Create table in postgres database.
        
        Drop table if same table name exists in database. 
        Provided column names and types for created table.
        
        Decorators:
            lD.log
        
        Arguments:
            logger : {logging.Logger} :
                the logger function
            self : {instance of dbConnect} :
                current instance
            tablename : {string} :
                table name for table to be created. 
                if table under schema, include schema name in from of table name. (ie. schema.table_name)
            colnames : {list of string} :
                list of column names for the intended table to be created
            coltypes : {list of string} :
                list of column types for the intended table to be created
        '''

        try:
        
            self.cursor  = self.conn.cursor()   ## Initialise cursor

            self.exec_sql("""drop table if exists {} """.format(tablename))

            tablename_wo_schema = tablename.split('.')[1]
            cols_statement      = ', '.join(['"{}" {}'.format(pairs[0], pairs[1]) for pairs in zip(colnames, coltypes)])
            sql_string          = """
                                  create table {tablename}(
                                  {columns_statement}
                                  );
            """.format(tablename=tablename, columns_statement=cols_statement, tablename_wo_schema=tablename_wo_schema)

            self.exec_sql(sql_string)
            self.commit_sql()

            print('..created table: {}'.format(tablename))

        except Exception as e:

            logger.error('Unable to create table: {tablename}: \n{error}'\
                            .format(tablename   = tablename,
                                    error       = str(e)))

    @lD.log(logBase + '.PostgresDB.push_df')
    def push_df(logger, self, args):
        '''helper function for running self.push_df_sequential
        
            - this function accept args as input
            - args = [(index of iteration, data, tablename), ... ]
        
        Decorators:
            lD.log
        
        Arguments:
            logger : {logging.Logger} :
                the logger function
            args : {tuple} : 
                in the form of (index of iteration, data, tablename) 
                for each iteration to push data to database
        '''

        try:
            i, data, tablename  = args

            string_format       = "("
            string_format       = string_format + ','.join(['%s'] * len(data.T))
            string_format       = string_format + ")"
            
            ls_mogrify_string   = []
            for index, rows in data.iterrows():
                try: 
                    ls_mogrify_string.append(self.cursor.mogrify(string_format, tuple(rows.values)))
                except:
                    pass

            args_str          = b','.join(ls_mogrify_string)
            ls_mogrify_string = []
            sql_string        = 'INSERT INTO {} VALUES '.format(tablename).encode()

            self.cursor.execute(sql_string + args_str)
            self.conn.commit()

        except Exception as e:

            logger.error('Unable to push one df[index:{index}] to table: {tablename}: \n{error}'\
                            .format(index       = i,
                                    tablename   = tablename,
                                    error       = str(e)))

    @lD.log(logBase + '.PostgresDB.push_df_sequential')
    def push_df_sequential(logger, self, data, tablename, colnames, coltypes, batch_size='all', drop_table_if_exist=True):
        '''Given pandas.DataFrame, upload data to specified tablename, column names & column types
        
            - upload data to specified table by batch if batch_size is set to integer value
        
        Decorators:
            lD.log
        
        Arguments:
            logger : {logging.Logger} :
                the logger function
            self : {instance of dbConnect} :
                current instance
            data : {pandas.DataFrame} :
                structural data in dataframe 
            tablename : {string} : 
                name of table in postgres database
            colnames : {list of string} : 
                list of column names present in the database
            coltypes : {list of string} : 
                list of column types required in table
        
        Keyword Arguments:
            batch_size : {int or 'all'} : 
                push all data at once if batch_size set to 'all' otherwise based on integer value (default: {'all'})
            drop_table_if_exist : {bool, optional} :
                drop table if exist in database and recreate with above table attributes (default: {True})
        '''

        try:

            if drop_table_if_exist:
                
                '''Create table based on specified tablename and columnnames'''
                self.create_table(tablename, colnames, coltypes)

            if batch_size == 'all':   self.push_df((0, data, tablename))
            else:

                n_chunk      = int(len(data) / batch_size) + 1
                ls_data      = np.array_split(data, n_chunk)
                ls_tablename = [tablename] * n_chunk
                ls_index     = range(n_chunk)

                del data

                ls_data      = list(zip(ls_index, ls_data, ls_tablename))
                pbar         = tqdm(ls_data)
                pbar.set_description('Uploading data to: {}..'.format(tablename))

                for d_input in pbar:
                    self.push_df( d_input )

            self.disconnect()

            print('..uploaded data to table: {}'.format(tablename))

        except Exception as e:

            logger.error('Unable to push df (sequentially) to table: {tablename}: \n{error}'\
                            .format(tablename   = tablename,
                                    error       = str(e)))

