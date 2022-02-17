#!/usr/bin/env python3
'''
changesetStats.py should calculate changeset statistics for each individual changetset ID and store it in the DB

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


num_format = "{:,}".format
class hashtags():
    def __init__(self):
        None
    def createTables(self, connection):
        print ('creating all_changesets_stats table if not exists')
        cursor = connection.cursor()
        cursor.execute(queries.createAllChangesetsStatsTable)
        connection.commit()
        cursor.close()
    def getMaxTime(self, connection):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql = f'''
              select max( c.created_at) latest_time from  public.osm_changeset  c;
                '''
        cursor.execute(sql)

        record = cursor.fetchone()
        latestTime = record['latest_time'] 
        if (latestTime is None):
            print("osm_changeset table is empty so starting from todays date to scan changesets")
            latestTime = datetime.now()

        print("Maximum time to scan from =",latestTime)
        return latestTime
    
    def create(self,connection,maxTime):
        # self.createTables(connection)
        #getMaxChangeset if it is not a parameter 
        maxTime = self.getMaxTime(connection) if maxTime is None else datetime.strptime(maxTime, '%Y-%m-%d') 
        print(f"The script will scan all changesets starting from {maxTime} backward")
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
       
        counter = maxTime

        while counter > datetime(2005,4,9): #2005-04-09 first changeset created date in OSM
            sql = f'''
                insert into all_changesets_stats 
                select osh.changeset , 
                    sum((osh.tags ? 'building' and osh."type" in ('way','relation') and (osh."action" =  'create'))::int ) added_buildings,
                    sum((osh.tags ? 'building' and osh."type" in ('way','relation') and (osh."action" =  'modify'))::int ) modified_buildings , 
                    sum((osh.tags ? 'amenity' and osh."type" in ('way','node') and (osh."action" =  'create'))::int) added_amenity,
                    sum((osh.tags ? 'amenity' and osh."type" in ('way','node') and (osh."action" =  'modify'))::int) modified_amenity , 
                    sum((osh.tags ? 'highway' and (osh."action" =  'create'))::int) added_highway,
                    sum((osh.tags ? 'highway' and (osh."action" =  'modify'))::int) modified_highway,
                    sum ( 
                    case 
                        when (osh.tags ? 'highway' and osh."type" in ('way','relation') and  (osh."action" =  'create')) then ST_Length(public.construct_geometry(osh.id,
                            osh.version,
                            osh."timestamp",
                            osh.nds,
                            osh.changeset)::geography)
                        else 0
                    end
                    ) added_highway_meters,
                    sum ( 
                    case 
                        when (osh.tags ? 'highway' and osh."type" in ('way','relation') and  (osh."action" =  'modify')) then ST_Length(public.construct_geometry(osh.id,
                            osh.version,
                            osh."timestamp",
                            osh.nds,
                            osh.changeset)::geography)
                        else 0
                    end)  modified_highway_meters,
                    sum(((osh.tags -> 'place' in ('isolated_dwelling', 'hamlet','village','neighbourhood','suburb','town','city')) and 
                    	osh."type" in ('way','node') and 
                    	(osh."action" =  'create') )::int) added_places, 
                    sum(((osh.tags -> 'place' in ('isolated_dwelling', 'hamlet','village','neighbourhood','suburb','town','city')) and 
                    	osh."type" in ('way','node') and 
                    	(osh."action" =  'modify') )::int) modified_places
                    from public.osm_element_history osh
                    where "action" != 'delete'
                    and osh."timestamp" between '{counter - timedelta(days=1)}' and '{counter}'
                    group by changeset on conflict (changeset) DO UPDATE 
               SET added_buildings = EXCLUDED.added_buildings ,
              	   	modified_buildings = EXCLUDED.modified_buildings,
              	    added_amenity = EXCLUDED.added_amenity,
              	    modified_amenity=EXCLUDED.modified_amenity,
              	    added_highway=EXCLUDED.added_highway,
              	    modified_highway=EXCLUDED.modified_highway,
                    added_highway_meters=EXCLUDED.added_highway_meters,
                    modified_highway_meters= EXCLUDED.modified_highway_meters,
                    added_places = EXCLUDED.added_places,
              	    modified_places=EXCLUDED.modified_places;

                    '''
            print(f'{datetime.now()} Calculating changesets stats from changesets between {counter - timedelta(days=1)} to {counter}')
            cursor.execute(sql)
            connection.commit()
            counter = counter - timedelta(days=1)

        cursor.close()
    
    def getMaxChangeset(self, connection):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql = f'''
              select max(id) latest_changeset from  public.osm_changeset;
                '''
        cursor.execute(sql)

        record = cursor.fetchone()
        print("Maximum changeset id in osm_changeset table =",record['latest_changeset'])
        return record['latest_changeset']
    def fixAmenityPlaces(self,connection,maxChangeset):
        #getMaxChangeset if it is not a parameter 
        maxChangeset = self.getMaxChangeset(connection) if maxChangeset is None else int(maxChangeset)
        print(f"The script will scan all changesets starting from {num_format(maxChangeset)} backward")
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
       
        counter = maxChangeset

        while counter > 0:
            sql = f'''
              update  all_changesets_stats 
                set added_amenity = sub.added_amenity,
                modified_amenity = sub.modified_amenity,
                added_places = sub.added_places,
                modified_places = sub.modified_places
                from (select osh.changeset, sum((osh.tags ? 'amenity' and osh."type" in ('way','node') and (osh."action" =  'create'))::int) added_amenity,
                                    sum((osh.tags ? 'amenity' and osh."type" in ('way','node') and (osh."action" =  'modify'))::int) modified_amenity , 
                                    sum(((osh.tags -> 'place' in ('isolated_dwelling', 'hamlet','village','neighbourhood','suburb','town','city')) and 
                                        osh."type" in ('way','node') and 
                                        (osh."action" =  'create') )::int) added_places, 
                                    sum(((osh.tags -> 'place' in ('isolated_dwelling', 'hamlet','village','neighbourhood','suburb','town','city')) and 
                                        osh."type" in ('way','node') and 
                                        (osh."action" =  'modify') )::int) modified_places
                                    from public.osm_element_history osh
                                    where "action" != 'delete'
                                    and osh.changeset between {counter - 10000} and {counter}
                                    group by osh.changeset) sub
                where all_changesets_stats.changeset = sub.changeset;
                    '''
            cursor.execute(sql)
            print(f'{datetime.now()} Amenity & Places fixed for changesets IDs {num_format(counter - 10000)} to {num_format(counter)}')
            connection.commit()
            counter = counter - 10001

        cursor.close()

    def update(self,connection):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql = f'''
              select max(changeset) latest_changeset from public.all_changesets_stats
                '''
        cursor.execute(sql)

        record = cursor.fetchone()
        if (record['latest_changeset'] is None):
            print("There are no changesets in all_changesets_stats table, you need to run changesetStats.py script without -U first")    
            exit(1)
        print("Maximum changeset id=",record['latest_changeset'])
        
        sql = f'''
              insert into all_changesets_stats 
                select osh.changeset , 
                    sum((osh.tags ? 'building' and osh."type" in ('way','relation') and (osh."action" =  'create'))::int ) added_buildings,
                    sum((osh.tags ? 'building' and osh."type" in ('way','relation') and (osh."action" =  'modify'))::int ) modified_buildings , 
                    sum((osh.tags ? 'amenity' and osh."type" in ('way','node') and (osh."action" =  'create'))::int) added_amenity,
                    sum((osh.tags ? 'amenity' and osh."type" in ('way','node') and (osh."action" =  'modify'))::int) modified_amenity , 
                    sum((osh.tags ? 'highway' and (osh."action" =  'create'))::int) added_highway,
                    sum((osh.tags ? 'highway' and (osh."action" =  'modify'))::int) modified_highway,
                    sum ( 
                    case 
                        when (osh.tags ? 'highway' and osh."type" in ('way','relation') and  (osh."action" =  'create')) then ST_Length(public.construct_geometry(osh.id,
                            osh.version,
                            osh."timestamp",
                            osh.nds,
                            osh.changeset)::geography)
                        else 0
                    end
                    ) added_highway_meters,
                    sum ( 
                    case 
                        when (osh.tags ? 'highway' and osh."type" in ('way','relation') and  (osh."action" =  'modify')) then ST_Length(public.construct_geometry(osh.id,
                            osh.version,
                            osh."timestamp",
                            osh.nds,
                            osh.changeset)::geography)
                        else 0
                    end)  modified_highway_meters,
                    sum(((osh.tags -> 'place' in ('isolated_dwelling', 'hamlet','village','neighbourhood','suburb','town','city')) and 
                    	osh."type" in ('way','node') and 
                    	(osh."action" =  'create') )::int) added_places, 
                    sum(((osh.tags -> 'place' in ('isolated_dwelling', 'hamlet','village','neighbourhood','suburb','town','city')) and 
                    	osh."type" in ('way','node') and 
                    	(osh."action" =  'modify') )::int) modified_places
                    from public.osm_element_history osh
                    where "action" != 'delete'
                and osh.changeset > {record['latest_changeset']}
                group by changeset  on conflict do nothing;

                '''
        cursor.execute(sql)
        connection.commit()
        print(f"Inserted the new change sets with IDs > {num_format(record['latest_changeset'])}")

        sql = f'''
              select max(changeset) latest_changeset from public.all_changesets_stats
                '''
        cursor.execute(sql)
        record = cursor.fetchone()
        print("The new calculated changetset ID=",record['latest_changeset'])

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
argParser.add_argument('-U', '--update', action='store_true', dest='update', default=False, help='Top update the changetset statistics after the latest calculated changeset')
argParser.add_argument('-F', '--fixAmenityPlaces', action='store_true', dest='fix', default=False, help='Fix amenity calcs and places from changeset')
argParser.add_argument('-c', '--changeset', action='store', dest='maxChangeset', default=None, help='Maximum changeset ID to start from')

args = argParser.parse_args()

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

if (args.update):
    md.update(conn)
else:
    if (args.fix):
        md.fixAmenityPlaces(conn,args.maxChangeset)
    else:
        md.create(conn,args.maxChangeset)

endTime = datetime.now()
timeCost = endTime - beginTime

print( 'Processing time cost is ', timeCost)

print ('All done. Changeset statistics are up to date')