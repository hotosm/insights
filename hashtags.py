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
    def getFirstLastUsed(self,connection, hashtag,firstUsed,lastUsed):
        beginTime = datetime.now()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
        if (firstUsed is None):
            sql = f'''
                    select  max(c.created_at) last_used,min(c.created_at) first_used
                from public.osm_changeset c
                where (
                        (c.tags -> 'comment') ~~* '%#{hashtag} %' or (c.tags -> 'hashtags') ~~* '%#{hashtag};%' or
                        (c.tags -> 'comment') ~~* '%#{hashtag}' or (c.tags -> 'hashtags') ~~* '%#{hashtag}'
            );
                    '''
            print(f'''Getting first used & last used dates for #{hashtag}''')    
            cursor.execute(sql)
            record = cursor.fetchone()
            print(f'''#{hashtag} first used {record['first_used']} last used {record['last_used']} found in query time: {datetime.now() - beginTime}''')
            if (record['first_used'] is not None):
                sql = f'''
                        UPDATE public.hashtag
                                SET first_used='{record['first_used']}', last_used='{record['last_used']}'
                                WHERE "name"='{hashtag}';

                        '''
                cursor.execute(sql)
                connection.commit()
                cursor.close()
            return [record['first_used'],record['last_used']]
        else: # first used and last used already calculated, get the last used only
            sql = f'''
                    select  max(c.created_at) last_used
                from public.osm_changeset c
                where c.created_at >= '{lastUsed}'
                    and (
                        (c.tags -> 'comment') ~~* '%#{hashtag} %' or (c.tags -> 'hashtags') ~~* '%#{hashtag};%' or
                        (c.tags -> 'comment') ~~* '%#{hashtag}' or (c.tags -> 'hashtags') ~~* '%#{hashtag}'
                     );
                    '''
            print(f'''Getting last used dates for #{hashtag}''')    
            cursor.execute(sql)
            record = cursor.fetchone()
            print(f'''#{hashtag} first used {firstUsed} last_used {record['last_used']} found in query time: {datetime.now() - beginTime}''')
            sql = f'''
                    UPDATE public.hashtag
                            SET last_used='{record['last_used']}'
                            WHERE "name"='{hashtag}';

                    '''
            cursor.execute(sql)
            connection.commit()
            cursor.close()
            return [firstUsed,record['last_used']]
        
    def getTotalUniqueContributors(self,cursor,start,end,hashtag):
        beginTime = datetime.now()
        sql = f'''
                    select count(distinct c.user_id) total
                    from public.osm_changeset c
                    where c.created_at  between '{start}' and '{end}'
                    and (
                            (c.tags -> 'comment') ~~* '%#{hashtag} %' or (c.tags -> 'hashtags') ~~* '%#{hashtag};%' or
                            (c.tags -> 'comment') ~~* '%#{hashtag}' or (c.tags -> 'hashtags') ~~* '%#{hashtag}'
                        )
                    ;
                '''
        print (f'Calculating contributors for {hashtag} between {start} and {end}')
        cursor.execute(sql)                
        values = cursor.fetchone()
        contributorsCount = 0 if values[0] is None else values[0]
        print (f'Calculated {contributorsCount} contributors for {hashtag} between {start} and {end} is done in {datetime.now() - beginTime}')
                
        return contributorsCount
    def getTotalBuildingsHighways(self,cursor,start,end,hashtag):
        beginTime = datetime.now()
        sql = f'''
                        select sum (s.added_buildings) total_new_buildings,
		                sum (s.added_highway_meters) total_new_road_meters,
		                sum (s.added_amenity) total_new_amenity,
		                sum (s.modified_amenity) total_modified_amenity,
		                sum (s.added_places) total_new_places,
		                sum (s.modified_places) total_modified_places
                        from all_changesets_stats s join public.osm_changeset c on s.changeset  = c.id 
                        where c.created_at between '{start}' and '{end}'
                        and (
                            (c.tags -> 'comment') ~~* '%#{hashtag} %' or (c.tags -> 'hashtags') ~~* '%#{hashtag};%' or
                            (c.tags -> 'comment') ~~* '%#{hashtag}' or (c.tags -> 'hashtags') ~~* '%#{hashtag}'	
                            );
                '''
        print (f'Calculating new buildings/highway/Amenity/Places meters for {hashtag} between {start} and {end}')
        
        cursor.execute(sql)                
        values = cursor.fetchone()
        buildingCount = 0 if values[0] is None else values[0]
        highwayMeters = 0 if values['total_new_road_meters'] is None else values['total_new_road_meters']
        newAmenity = 0 if values['total_new_amenity'] is None else values['total_new_amenity']
        modifiedAmenity = 0 if values['total_modified_amenity'] is None else values['total_modified_amenity']
        newPlaces = 0 if values['total_new_places'] is None else values['total_new_places']
        modifiedPlaces = 0 if values['total_modified_places'] is None else values['total_modified_places']
        print (f'Calculated {buildingCount} new buildings and {highwayMeters} meter(s) of roads for {hashtag} between {start} and {end} is done in {datetime.now() - beginTime}')

        return [buildingCount,highwayMeters,newAmenity,modifiedAmenity ,newPlaces,modifiedPlaces]

    def buildWeeklyStats(self,connection,hashtag ,hashtagId, startDate,endDate):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # We need to find first Friday after the start date
        fridayDate = startDate = datetime.combine(startDate, datetime.min.time()) 
        endDate = startDate if endDate is None else datetime.combine(endDate, datetime.min.time())

        while fridayDate.weekday() != 4:
            fridayDate = fridayDate - timedelta(days=1)
        
        fridayDate = fridayDate + timedelta(hours=12)      
        endDate = self.getNewEndDateWeek(endDate)
        print('startDate',startDate,'endDate',endDate) 
        print(fridayDate,'is first Friday at noon')
        nextFridayDate = fridayDate + timedelta(days=7)
        
        while nextFridayDate <= endDate:
            if(not self.checkIfExists(connection,fridayDate,nextFridayDate,hashtagId)):
                [buildingCount,highwayMeters,newAmenity,modifiedAmenity ,newPlaces,modifiedPlaces] = self.getTotalBuildingsHighways(cursor,fridayDate,nextFridayDate,hashtag)
                # Do contributors
                contributorsCount = self.getTotalUniqueContributors(cursor,fridayDate,nextFridayDate,hashtag)
                insert = f'''
                INSERT INTO public.hashtag_stats
                        (hashtag_id, "type", start_date, end_date, total_new_buildings, total_uq_contributors, total_new_road_m, calc_date, total_new_amenity, total_modified_amenity, total_new_places, total_modified_places)
                        VALUES({hashtagId}, 'w', '{fridayDate}' , '{nextFridayDate}',{buildingCount} , {contributorsCount} , {highwayMeters}, now(),{newAmenity},{modifiedAmenity},{newPlaces},{modifiedPlaces})  on conflict do nothing ;
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
    def getNewEndDateMonth(self,p_date):
        # this month date first date
        monthEndDate = p_date
        while monthEndDate.day != 1:
            monthEndDate = monthEndDate + timedelta(days=1)
        
        monthStartDate = datetime(p_date.year,p_date.month,1)

        if (monthEndDate < datetime.now()):
            return monthEndDate
        else:
            return monthStartDate
            
    def getNewEndDateWeek(self,date):
        # find next friday after end date and in the past
        nextFriday = date + timedelta(hours=12)   
        while nextFriday.weekday() != 4:
            nextFriday = nextFriday + timedelta(days=1)

        if(nextFriday > datetime.now()):
           return date
        else:
            return nextFriday
    def getNewEndDateQuarter(self,date):
        nextQuarter = date 
        while nextQuarter.month != 10 and  nextQuarter.month != 7 and  nextQuarter.month != 4 and nextQuarter.month != 1 :
            nextQuarter = nextQuarter + timedelta(days=1)
        nextQuarter = datetime(nextQuarter.year,nextQuarter.month,1)
        if(nextQuarter > datetime.now()):
           return date
        else:
            return nextQuarter
    def buildMonthlyStats(self,connection,hashtag ,hashtagId, startDate,endDate):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # We need to find first day of the month

        
        beginingDate = startDate = datetime.combine(startDate, datetime.min.time()) 
        endDate = startDate if endDate is None else datetime.combine(endDate, datetime.min.time())

        print('startDate',startDate,'endDate',endDate) 
       
        endDate = self.getNewEndDateMonth(endDate)

        while beginingDate.day != 1:
            beginingDate = beginingDate - timedelta(days=1)
        
       
        print(beginingDate,'is first day of the month')
        endOftheMonth = self.get_next_month(beginingDate)
        
        while endOftheMonth <= endDate:
            if(not self.checkIfExists(connection,beginingDate,endOftheMonth,hashtagId)):
                [buildingCount,highwayMeters,newAmenity,modifiedAmenity ,newPlaces,modifiedPlaces] = self.getTotalBuildingsHighways(cursor,beginingDate,endOftheMonth,hashtag)
                # Do contributors
                contributorsCount = self.getTotalUniqueContributors(cursor,beginingDate,endOftheMonth,hashtag)
                insert = f'''
                INSERT INTO public.hashtag_stats
                        (hashtag_id, "type", start_date, end_date, total_new_buildings, total_uq_contributors, total_new_road_m, calc_date, total_new_amenity, total_modified_amenity, total_new_places, total_modified_places)
                        VALUES({hashtagId}, 'm', '{beginingDate}' , '{endOftheMonth}',{buildingCount} , {contributorsCount} , {highwayMeters}, now(),{newAmenity},{modifiedAmenity},{newPlaces},{modifiedPlaces})  on conflict do nothing ;
                    ''' 
                cursor.execute(insert)
                connection.commit()
            else:
                print(f'Stats for {hashtag} between {beginingDate} and {endOftheMonth} is already calculated')
            beginingDate = endOftheMonth
            endOftheMonth = self.get_next_month(beginingDate)
           

        print('Done monthly stats for', hashtag)
        cursor.close()
    def getNextQuarter(self,date):
        nextQ = date + timedelta(days=93)
        nextQ = datetime(nextQ.year,nextQ.month,1)
        return nextQ
        
    def buildQuarterlyStats(self,connection,hashtag ,hashtagId, startDate,endDate):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # We need to find first day of the quarter

        
        beginingDate = startDate = datetime.combine(startDate, datetime.min.time()) 
        endDate = startDate if endDate is None else datetime.combine(endDate, datetime.min.time())

        print('startDate',startDate,'endDate',endDate) 
       
       
        while beginingDate.month != 10 and  beginingDate.month != 7 and  beginingDate.month != 4 and beginingDate.month != 1 :
            beginingDate = beginingDate - timedelta(days=1)

        beginingDate = datetime(beginingDate.year,beginingDate.month,1)
        print('beginingDate',beginingDate)

        
        endDate = self.getNewEndDateQuarter(endDate)
        print('quarter startDate',beginingDate,'quarter endDate',endDate) 
        
        endOfTheQuarter = self.getNextQuarter(beginingDate)
        while endOfTheQuarter <= endDate:
            if(not self.checkIfExists(connection,beginingDate,endOfTheQuarter,hashtagId)):
                [buildingCount,highwayMeters,newAmenity,modifiedAmenity ,newPlaces,modifiedPlaces] = self.getTotalBuildingsHighways(cursor,beginingDate,endOfTheQuarter,hashtag)
                # Do contributors
                contributorsCount = self.getTotalUniqueContributors(cursor,beginingDate,endOfTheQuarter,hashtag)
                insert = f'''
                INSERT INTO public.hashtag_stats
                        (hashtag_id, "type", start_date, end_date, total_new_buildings, total_uq_contributors, total_new_road_m, calc_date, total_new_amenity, total_modified_amenity, total_new_places, total_modified_places)
                        VALUES({hashtagId}, 'q', '{beginingDate}' , '{endOfTheQuarter}',{buildingCount} , {contributorsCount} , {highwayMeters}, now(),{newAmenity},{modifiedAmenity},{newPlaces},{modifiedPlaces})  on conflict do nothing ;
                    ''' 
                cursor.execute(insert)
                connection.commit()
            else:
                print(f'Stats for {hashtag} between {beginingDate} and {endOfTheQuarter} is already calculated')
            beginingDate = endOfTheQuarter
            endOfTheQuarter = self.getNextQuarter(beginingDate)           
        print('Done quarterly stats for', hashtag)
        cursor.close()
    def buildYearlyStats(self,connection,hashtag ,hashtagId, startDate,endDate):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # We need to find first day of the year

        
        beginingDate = startDate = datetime.combine(startDate, datetime.min.time()) 
        endDate = startDate if endDate is None else datetime.combine(endDate, datetime.min.time())

        print('startDate',startDate,'endDate',endDate) 
        beginingDate = datetime(beginingDate.year,1,1)
        
        endDate = datetime(endDate.year,1,1) 
        print('year startDate',beginingDate,'year endDate',endDate) 
        
        endOfTheYear =  datetime(beginingDate.year + 1 ,1,1)
        while endOfTheYear <= endDate:
            if(not self.checkIfExists(connection,beginingDate,endOfTheYear,hashtagId)):
                [buildingCount,highwayMeters,newAmenity,modifiedAmenity ,newPlaces,modifiedPlaces] = self.getTotalBuildingsHighways(cursor,beginingDate,endOfTheYear,hashtag)
                # Do contributors
                contributorsCount = self.getTotalUniqueContributors(cursor,beginingDate,endOfTheYear,hashtag)
                insert = f'''
                INSERT INTO public.hashtag_stats
                        (hashtag_id, "type", start_date, end_date, total_new_buildings, total_uq_contributors, total_new_road_m, calc_date, total_new_amenity, total_modified_amenity, total_new_places, total_modified_places)
                        VALUES({hashtagId}, 'y', '{beginingDate}' , '{endOfTheYear}',{buildingCount} , {contributorsCount} , {highwayMeters}, now(),{newAmenity},{modifiedAmenity},{newPlaces},{modifiedPlaces})  on conflict do nothing ;
                    ''' 
                cursor.execute(insert)
                connection.commit()
            else:
                print(f'Stats for {hashtag} between {beginingDate} and {endOfTheYear} is already calculated')
            beginingDate = endOfTheYear
            endOfTheYear = datetime(beginingDate.year + 1 ,1, 1)        
        print('Done yearly stats for', hashtag)
        cursor.close()

    def calcHashtagStats(self,connection):
        self.createTables(connection)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql = '''
        SELECT id, "name", added_by, created_at, first_used, last_used
        FROM public.hashtag
        order by first_used desc;

        '''
        cursor.execute(sql)
       
        records = cursor.fetchall() 
        print("records", records )
       
        for row in records:
            # select * 
            # from public.tm_project_details(10405)
            [firstUsed,lastUsed] = self.getFirstLastUsed(connection,row['name'].strip(),row['first_used'],row['last_used'])
            if firstUsed is not None and lastUsed is not None:
                self.buildWeeklyStats(connection, row['name'].strip(),row['id'],firstUsed,lastUsed)
                self.buildMonthlyStats(connection, row['name'].strip(),row['id'],firstUsed,lastUsed)
                self.buildQuarterlyStats(connection, row['name'].strip(),row['id'],firstUsed,lastUsed)
                self.buildYearlyStats(connection, row['name'].strip(),row['id'],firstUsed,lastUsed)
            else:
                print(f'''hashtag {row['name']} has never been used in an OSM changetst hashtags field or comment''')
         
           
        cursor.close()


initialTime = datetime.now()
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


try:
    md = hashtags()

    md.calcHashtagStats(conn)

    endTime = datetime.now()
    timeCost = endTime - initialTime
    print( 'Processing time cost is ', timeCost)
    print ('All done. Hashtag calculation is done')
except Exception as e:
    print (e.__doc__)
    print (e.message)
