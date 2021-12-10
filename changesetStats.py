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
    def getMaxChangeset(self, connection):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql = f'''
              select max(id) latest_changeset from  public.osm_changeset;
                '''
        cursor.execute(sql)

        record = cursor.fetchone()
        print("Maximum changeset id in osm_changeset table =",record['latest_changeset'])
        return record['latest_changeset']
    
    def create(self,connection):
        self.createTables(connection)
        maxChangeset = self.getMaxChangeset(connection)
        print(f"The script will scan all changesets starting from {num_format(maxChangeset)} backward")
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
       
        counter = maxChangeset

        while counter > 0:
            sql = f'''
                insert into all_changesets_stats select osh.changeset , 
                    sum((osh.tags ? 'building' and (osh."action" =  'create'))::int) added_buildings,
                    sum((osh.tags ? 'building' and (osh."action" =  'modify'))::int) modified_buildings , 
                    sum((osh.tags ? 'amenity' and (osh."action" =  'create'))::int) added_amenity,
                    sum((osh.tags ? 'amenity' and (osh."action" =  'modify'))::int) modified_amenity , 
                    sum((osh.tags ? 'highway' and (osh."action" =  'create'))::int) added_highway,
                    sum((osh.tags ? 'highway' and (osh."action" =  'modify'))::int) modified_highway,
                    sum ( 
                    case 
                        when (osh.tags ? 'highway' and  (osh."action" =  'create') and "type" = 'way' and osh.nds is not null) then ST_Length(public.construct_geometry(osh.nds,osh.id)::geography)
                        else 0
                    end
                    ) added_highway_meters,
                    sum ( 
                    case 
                        when (osh.tags ? 'highway' and  (osh."action" =  'modify')) then ST_Length(public.construct_geometry(osh.nds,osh.id)::geography)
                        else 0
                    end)  modified_highway_meters
                    from public.osm_element_history osh
                    where "type" in ('way','relation')
                    and "action" != 'delete'
                    and osh.changeset between {counter - 10000} and {counter}
                    group by changeset on conflict do nothing;

                    '''
            print(f'{datetime.now()} Calculating changesets stats from changesets IDs {num_format(counter - 10000)} to {num_format(counter)}')
            cursor.execute(sql)
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
                    sum((osh.tags ? 'building' and (osh."action" =  'create'))::int) added_buildings,
                    sum((osh.tags ? 'building' and (osh."action" =  'modify'))::int) modified_buildings , 
                    sum((osh.tags ? 'amenity' and (osh."action" =  'create'))::int) added_amenity,
                    sum((osh.tags ? 'amenity' and (osh."action" =  'modify'))::int) modified_amenity , 
                    sum((osh.tags ? 'highway' and (osh."action" =  'create'))::int) added_highway,
                    sum((osh.tags ? 'highway' and (osh."action" =  'modify'))::int) modified_highway,
                    sum ( 
                    case 
                        when (osh.tags ? 'highway' and  (osh."action" =  'create') and "type" = 'way' and osh.nds is not null) then ST_Length(public.construct_geometry(osh.nds,osh.id)::geography)
                        else 0
                    end
                    ) added_highway_meters,
                    sum ( 
                    case 
                        when (osh.tags ? 'highway' and  (osh."action" =  'modify')) then ST_Length(public.construct_geometry(osh.nds,osh.id)::geography)
                        else 0
                    end)  modified_highway_meters
                    from public.osm_element_history osh
                where "type" in ('way','relation')
                and "action" != 'delete'
                and osh.changeset > {record['latest_changeset']}
                group by changeset  ;

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
    md.create(conn)

endTime = datetime.now()
timeCost = endTime - beginTime

print( 'Processing time cost is ', timeCost)

print ('All done. Changeset statistics are up to date')