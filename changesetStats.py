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

class hashtags():
    def __init__(self):
        None
    def create(self,connection):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql = f'''
               create table if not exists all_changesets_stats as select osh.changeset , 
                sum((osh.tags ? 'building' and (osh."action" =  'create'))::int) added_buildings,
                sum((osh.tags ? 'building' and (osh."action" =  'modify'))::int) modified_buildings , 
                sum((osh.tags ? 'aminity' and (osh."action" =  'create'))::int) added_aminity,
                sum((osh.tags ? 'aminity' and (osh."action" =  'modify'))::int) modified_aminity , 
                sum((osh.tags ? 'highway' and (osh."action" =  'create'))::int) added_highway,
                sum((osh.tags ? 'highway' and (osh."action" =  'modify'))::int) modified_highway 
                from public.osm_element_history osh
                where "type" in ('way','relation')
                group by changeset;
                '''
        print('creating all_changesets_stats and populating all existing countries changeset stats')
        print('Might take around 4 hours to finish ... and will do nothing is the table is already exists')
        cursor.execute(sql)
        connection.commit()
        cursor.close()

    def update(self,connection):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        sql = f'''
              select max(changeset) latest_changeset from public.all_changesets_stats
                '''
        cursor.execute(sql)

        record = cursor.fetchone()
        print("Maximum changeset id=",record['latest_changeset'])

        sql = f'''
              insert into all_changesets_stats 
                select osh.changeset , 
                sum((osh.tags ? 'building' and (osh."action" =  'create'))::int) added_buildings,
                sum((osh.tags ? 'building' and (osh."action" =  'modify'))::int) modified_buildings , 
                sum((osh.tags ? 'aminity' and (osh."action" =  'create'))::int) added_aminity,
                sum((osh.tags ? 'aminity' and (osh."action" =  'modify'))::int) modified_aminity , 
                sum((osh.tags ? 'highway' and (osh."action" =  'create'))::int) added_highway,
                sum((osh.tags ? 'highway' and (osh."action" =  'modify'))::int) modified_highway 
                from public.osm_element_history osh
                where "type" in ('way','relation')
                and osh.changeset > {record['latest_changeset']}
                group by changeset  ;

                '''
        cursor.execute(sql)
        connection.commit()
        print("Inserted the new change sets")

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