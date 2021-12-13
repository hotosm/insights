#!/usr/bin/env python3
'''
Hashtags.py should calculate the hashtags stats

'''

import config
import os
import sys
import argparse
import psycopg2
import psycopg2.extras
import queries

import urllib.request as urllib2


from datetime import datetime
from datetime import timedelta

class hashtags():
    def __init__(self):
        None
    def createTables(self, connection):
        print ('creating hashtags tables if not exists')
        cursor = connection.cursor()
        cursor.execute(queries.createHashtagsTables)
        connection.commit()

    def insertNewBatch(self, connection, data_arr):
        cursor = connection.cursor()
        
        sql = '''INSERT INTO public.osm_element_history
                (id, "type", tags, lat, lon, nds, members, changeset, "timestamp", uid, "version", "action",country,geom)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,st_setsrid ('POINT(%s %s)'::geometry,4326)) ON CONFLICT DO NOTHING'''
        
        psycopg2.extras.execute_batch(cursor, sql, data_arr)
        
        cursor.close()
    def checkIfExists(self,connection, start, end,hashtagId):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql = f'''
                select count(*) total
                from public.hashtag_stats s
                join public.hashtag h on h.id = s.hashtag_id 
                where h.id = {hashtagId}  
                and start_date = '{start}'
                and end_date = '{end}' 
                '''
        
        cursor.execute(sql)
        record = cursor.fetchone()
        cursor.close()
        # print('''record['total'] ''', record['total'] ,'record[0]',record[0],sql )
        if (record['total'] == 1):
            return True
        else:
            return False

    def buildWeeklyStats(self,connection,hashtag ,hashtagId, startDate,endDate,inputDate):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # We need to find first Friday after the start date

        
        fridayDate = startDate = datetime.combine(startDate, datetime.min.time()) 
        endDate = startDate if endDate is None else datetime.combine(endDate, datetime.min.time())

        while fridayDate.weekday() != 4:
            fridayDate = fridayDate - timedelta(days=1)
        
        fridayDate = fridayDate + timedelta(hours=12)      
        print('startDate',startDate,'endDate',endDate) 
        print(fridayDate,'is first Friday at noon')
        nextFridayDate = fridayDate + timedelta(days=7)
        beginTime = datetime.now()
        endTime = None
        
        while nextFridayDate < endDate:
            beginTime = datetime.now()

            if(not self.checkIfExists(connection,fridayDate,nextFridayDate,hashtagId)):
                sql = f'''
                        select sum (s.added_buildings) total_new_buildings,
		                sum (s.added_highway_meters) total_new_road_meters
                        from all_changesets_stats s join public.osm_changeset c on s.changeset  = c.id 
                        where c.created_at between  '{fridayDate}' and '{nextFridayDate}'
                        and (
                            (c.tags -> 'comment') ~~ '%{hashtag} %' or (c.tags -> 'hashtags') ~~ '%{hashtag};%' or
                            (c.tags -> 'comment') ~~ '%{hashtag}' or (c.tags -> 'hashtags') ~~ '%{hashtag}'	
                            );
                '''
                print (f'Calculating new buildings/highway meters for {hashtag} between {fridayDate} and {nextFridayDate}')
                # fridayDate = nextFridayDate
                # nextFridayDate = fridayDate + timedelta(days=7)
                # continue
                cursor.execute(sql)                
                values = cursor.fetchone()
                print('values',values)
                buildingCount = 0 if values['total_new_buildings'] is None else values['total_new_buildings']
                highwayMeters = 0 if values['total_new_road_meters'] is None else values['total_new_road_meters']
                print (f'Calculated {buildingCount} new buildings and {highwayMeters} meter(s) of roads for {hashtag} between {fridayDate} and {nextFridayDate} is done in {datetime.now() - beginTime}')

                # Do contributors
                beginTime = datetime.now()
                sql = f'''
                    select count(distinct c.user_id) total
                    from public.osm_changeset c
                    where c.created_at  between '{fridayDate}' and '{nextFridayDate}'
                    and (
                            (c.tags -> 'comment') ~~ '%{hashtag} %' or (c.tags -> 'hashtags') ~~ '%{hashtag};%' or
                            (c.tags -> 'comment') ~~ '%{hashtag}' or (c.tags -> 'hashtags') ~~ '%{hashtag}'
                        )
                    ;
                '''
                print (f'Calculating contrinutors for {hashtag} between {fridayDate} and {nextFridayDate}')
                # fridayDate = nextFridayDate
                # nextFridayDate = fridayDate + timedelta(days=7)
                # continue
                cursor.execute(sql)                
                values = cursor.fetchone()
                contrinutorsCount = 0 if values[0] is None else values[0]
                print (f'Calculated {contrinutorsCount} contrinutors for {hashtag} between {fridayDate} and {nextFridayDate} is done in {datetime.now() - beginTime}')
                

                # TODO: Calculate KM of roads

                insert = f'''
                INSERT INTO public.hashtag_stats
                        (hashtag_id, "type", start_date, end_date, total_new_buildings, total_uq_contributors, total_new_road_km, calc_date)
                        VALUES({hashtagId}, 'w', '{fridayDate}' , '{nextFridayDate}',{buildingCount} , {contrinutorsCount} , {highwayMeters}, now())  on conflict do nothing ;
                    ''' 
                cursor.execute(insert)
                connection.commit()
            else:
                print(f'Stats for {hashtag} between {fridayDate} and {nextFridayDate} is already calculated')
            fridayDate = nextFridayDate
            nextFridayDate = fridayDate + timedelta(days=7)
           

        print('Done weekly stats for', hashtag)
        cursor.close()
    def get_next_month(self,date):
        month = (date.month % 12) + 1
        year = date.year + (date.month + 1 > 12)
        return datetime(year, month, 1)
    def getNewEndDateForOldTMProjects(self,date):
        # this month date first date
        beginingOfThisMonth = datetime.now()

        while beginingOfThisMonth.day != 1:
            beginingOfThisMonth = beginingOfThisMonth - timedelta(days=1)
        if (beginingOfThisMonth > date):
            return date+ timedelta(days=31)
        else:
            return datetime.now()
    def buildMonthlyStats(self,connection,hashtag ,hashtagId, startDate,endDate,isTMProject):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # We need to find first day of the month

        
        beginingDate = startDate = datetime.combine(startDate, datetime.min.time()) 
        endDate = startDate if endDate is None else datetime.combine(endDate, datetime.min.time())

        print('startDate',startDate,'endDate',endDate) 
        if (isTMProject):
            endDate = self.getNewEndDateForOldTMProjects(endDate)

        while beginingDate.day != 1:
            beginingDate = beginingDate - timedelta(days=1)
        
       
        print(beginingDate,'is first day of the month')
        endOftheMonth = self.get_next_month(beginingDate)
        
        beginTime = datetime.now()
        endTime = None
        
        while endOftheMonth <= endDate:
            beginTime = datetime.now()

            if(not self.checkIfExists(connection,beginingDate,endOftheMonth,hashtagId)):
                sql = f'''
                        select sum (s.added_buildings) total_new_buildings
                        from all_changesets_stats s join public.osm_changeset c on s.changeset  = c.id 
                        where c.created_at between '{beginingDate}' and '{endOftheMonth}'
                        and (
                            (c.tags -> 'comment') ~~ '%{hashtag} %' or (c.tags -> 'hashtags') ~~ '%{hashtag};%' or
                            (c.tags -> 'comment') ~~ '%{hashtag}' or (c.tags -> 'hashtags') ~~ '%{hashtag}'	
                            );
                '''
                print (f'Calculating new buildings for {hashtag} between {beginingDate} and {endOftheMonth}')
                # beginingDate = endOftheMonth
                # endOftheMonth = self.get_next_month(beginingDate)
                # continue
                cursor.execute(sql)                
                values = cursor.fetchone()
                buildingCount = 0 if values[0] is None else values[0]
                print (f'Calculated {buildingCount} new buildings for {hashtag} between {beginingDate} and {endOftheMonth} is done in {datetime.now() - beginTime}')

                # Do contributors
                beginTime = datetime.now()
                sql = f'''
                    select count(distinct c.user_id) total
                    from public.osm_changeset c
                    where c.created_at  between '{beginingDate}' and '{endOftheMonth}'
                    and (
                            (c.tags -> 'comment') ~~ '%{hashtag} %' or (c.tags -> 'hashtags') ~~ '%{hashtag};%' or
                            (c.tags -> 'comment') ~~ '%{hashtag}' or (c.tags -> 'hashtags') ~~ '%{hashtag}'
                        )
                    ;
                '''
                print (f'Calculating contrinutors for {hashtag} between {beginingDate} and {endOftheMonth}')
                # beginingDate = endOftheMonth
                # endOftheMonth = self.get_next_month(beginingDate)
                # continue
                cursor.execute(sql)                
                values = cursor.fetchone()
                contrinutorsCount = 0 if values[0] is None else values[0]
                print (f'Calculated {contrinutorsCount} contrinutors for {hashtag} between {beginingDate} and {endOftheMonth} is done in {datetime.now() - beginTime}')
                

                # TODO: Calculate KM of roads

                insert = f'''
                INSERT INTO public.hashtag_stats
                        (hashtag_id, "type", start_date, end_date, total_new_buildings, total_uq_contributors, total_new_road_km, calc_date)
                        VALUES({hashtagId}, 'm', '{beginingDate}' , '{endOftheMonth}',{buildingCount} , {contrinutorsCount} , -1, now())  on conflict do nothing ;
                    ''' 
                cursor.execute(insert)
                connection.commit()
            else:
                print(f'Stats for {hashtag} between {beginingDate} and {endOftheMonth} is already calculated')
            beginingDate = endOftheMonth
            endOftheMonth = self.get_next_month(beginingDate)
           

        print('Done monthly stats for', hashtag)
        cursor.close()
    def calcHashtagStats(self,connection, startDate, frequency):
        self.createTables(connection)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql = '''
        SELECT id, "name", added_by, created_at, is_tm_project
        FROM public.hashtag;

        '''
        cursor.execute(sql)
       
        records = cursor.fetchall() 
        print("records", records )
        s = datetime.strptime(startDate, "%Y-%m-%d")
        for row in records:
            # select * 
            # from public.tm_project_details(10405)
            if (row['is_tm_project']):
                None
                # get start and end dates for TM project 
                cursor.execute(f'''select * 
                                 from public.tm_project_details({row['name']})''')
       
                project = cursor.fetchone()
                print ('hotosm-project-' + row['name'], 'project goes from' , project['created'],'to',project['last_activity'])
                self.buildWeeklyStats(connection, 'hotosm-project-' + row['name'],row['id'],project['created'],project['last_activity'],row['is_tm_project'])
                self.buildMonthlyStats(connection, 'hotosm-project-' + row['name'],row['id'],project['created'],project['last_activity'],row['is_tm_project'])
            else:
                None
                self.buildWeeklyStats(connection,row['name'],row['id'],s,datetime.now(),row['is_tm_project'])
                self.buildMonthlyStats(connection,row['name'],row['id'],s,datetime.now(),row['is_tm_project'])
           
        cursor.close()


beginTime = datetime.now()
endTime = None
timeCost = None


argParser = argparse.ArgumentParser(description="Parse OSM elements history into a PG database")
argParser.add_argument('-H', '--host', action='store', dest='dbHost', default=None, help='Database host FQDN or IP address')
argParser.add_argument('-P', '--port', action='store', dest='dbPort', default=None, help='Database port')
argParser.add_argument('-u', '--user', action='store', dest='dbUser', default=None, help='Database username')
argParser.add_argument('-p', '--password', action='store', dest='dbPass', default=None, help='Database password')
argParser.add_argument('-d', '--database', action='store', dest='dbName', default=None, help='Target database')
argParser.add_argument('-f', '--frequency', action='store', dest='frequency', default='w', help='The frequancy of calculating hashtags stats, w=weekly by default')
argParser.add_argument('-start', '--start', action='store', dest='start', default='2020-07-01', help='The start date of stats, default is 2020-07-01')

args = argParser.parse_args()

"""
Order of precedence for database credentials:
    1. Arguments passed to the program
    2. ENVVAR
    3. Hard-coded values in config.py
"""
database_connection_parameters = dict(
    database = config.DATABASE_NAME if args.dbName == None else args.dbName,
    user = config.DATABASE_USER if args.dbUser == None else args.dbUser,
    password = config.DATABASE_PASSWORD if args.dbPass == None else args.dbPass,
    host = config.DATABASE_HOST if args.dbHost == None else args.dbHost,
    port = config.DATABASE_PORT if args.dbPort == None else args.dbPort,
)
try:
    conn = psycopg2.connect(
        database=database_connection_parameters['database'],
        user=database_connection_parameters['user'],
        password=database_connection_parameters['password'],
        host=database_connection_parameters['host'],
        port=database_connection_parameters['port']
    )
    print("Connection is OK")
except psycopg2.OperationalError as err:
    print("Connection error: Please recheck the connection parameters")
    print("Current connection parameters:")
    database_connection_parameters['password'] = f"{type(database_connection_parameters['password'])}(**VALUE REDACTED**)"
    print(database_connection_parameters)
    sys.exit(1)



md = hashtags()

print('args.frequency ', args.frequency)
print('args.start ', args.start)

md.calcHashtagStats(conn,args.start,args.frequency)

endTime = datetime.now()
timeCost = endTime - beginTime

print( 'Processing time cost is ', timeCost)

print ('All done. Hashtag calculation is done')