"""[Responsible for Geometry Update of Osm_history_table , It will be one time update for all elements , can be used to reconstruct geometry]

Raises:
    err: [Database Connection Error]
    err: [Null Query Error]
Returns:
    [result]: [geom column Populated]
"""

import time
import logging
from psycopg2 import *
from psycopg2.extras import *
import connection
import sys
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)

class Database :
    """[Database Class responsible for connection , query execution and time tracking, can be used from multiple funtion and class returns result ,connection and cursor]
    """
    def __init__(self, db_params):
        """Database class constructor"""

        self.db_params = db_params
        print('Database class object created...')

    def connect(self):
        """Database class instance method used to connect to database parameters with error printing"""

        try:
            self.conn = connect(**self.db_params)
            self.cur = self.conn.cursor(cursor_factory=DictCursor)
            logging.debug('Database connection has been Successful...')
            return self.conn, self.cur
        except OperationalError as err:
            """pass exception to function"""
            # set the connection to 'None' in case of error
            self.conn = None
            raise err


    def executequery(self, query):
        """ Function to execute query after connection """
        # Check if the connection was successful
        try:
            if self.conn != None:
                self.cursor = self.cur
                if query!= None:
                    # catch exception for invalid SQL statement

                    try:
                        logging.debug('Query sent to Database')
                        self.cursor.execute(query)
                        self.conn.commit()
                        # print(query)
                        try:
                            result = self.cursor.fetchall()     
                            logging.debug('Result fetched from Database')
                            return result
                        except:
                            return self.cursor.statusmessage
                    except Exception as err:
                        raise err

                else:
                    raise ValueError("Query is Null")
            else:
                print("Database is not connected")
        except Exception as err:
            print("Oops ! You forget to have connection first")
            raise err

    def close_conn(self):
        """function for clossing connection to avoid memory leaks"""

        # Check if the connection was successful
        try:
            if self.conn != None:
                if self.cursor:
                    self.cursor.close()
                    self.conn.close()
                    logging.debug("Connection closed")
        except Exception as err:
            raise err

class Insight:
    """This class connects to Insight database and responsible for Values derived from database"""

    def __init__(self, parameters=None):
        self.database = Database(connection.get_connection_param())
        self.con, self.cur = self.database.connect()
        self.params = parameters

    def getMax_osm_element_history_id(self):
        """Function to extract latest maximum osm element id and minimum osm element id present in Osm_element_history Table"""
        
        query = f'''
                select min(id) as minimum , max(id) as maximum from  public.osm_element_history;
                '''
        record=self.database.executequery(query)
        logging.debug(f"""Maximum Osm element history fetched is {record[0][1]} and minimum is  {record[0][0]}""")
        return record[0][1],record[0][0]

    def update_geom(self,start,end):
        """Function that updates geometry column of osm_element_history"""
        base_query = f"""update
            osm_element_history as oeh
                            set
            geom =
            case
                when oeh.type = 'node' then ST_MakePoint(oeh.lon,
                oeh.lat)
                when oeh.type = 'way' then public.construct_geometry(oeh.nds,
                oeh.id,
                oeh."timestamp")
                else 
                    Null
            end
        where
            oeh.action != 'delete'
            and oeh.type != 'relation'"""
        
        for i in range(int(start),int(end)):
            query =base_query+ f""" and oeh.id={i}"""
            result=self.database.executequery(query)
            logging.debug(f"""Done-{i}/{max_element_id} : {result}""")
                   
        
    
    def batch_update(self,start_batch,end_batch,batch_frequency):
        """Upadtes Geometry with batch , given start element id , end element id to look for along with batch frequency , Here Batch frequency means frequency that you want to run a batch with, This function is made with the purpose for future usage as well if we want to update specific osm id from and to only beside scanning whole table"""
        batch_start_time = time.time()
        logging.debug(f"""----------Update Geometry Function has been started for {start_batch} to {end_batch} with batch frequency {batch_frequency}----------""")
        updated_row=start_batch
        
        while updated_row < end_batch:
            start_time = time.time()
            start=updated_row
            end=start+batch_frequency
            self.update_geom(start,end) 
            logging.debug(f"""Batch Update from {start} Until {end} , Completed in {(time.time() - start_time)} Seconds""")
            updated_row=end
            
        #closing connection   
        self.database.close_conn()  
        logging.debug("-----Updating Geometry Took-- %s seconds -----" % (time.time() - batch_start_time))


            
try:

    connect=Insight()
    max_element_id,min_element_id= connect.getMax_osm_element_history_id()
    """Passing Whole Osm element with per 500000 Batch for now"""
    connect.batch_update(min_element_id,max_element_id,500000)
except Exception as e:
    logging.debug (e)
    sys.exit(1) 

