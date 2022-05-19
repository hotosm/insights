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
    
    def create(self,connection,maxChangeset):
        # self.createTables(connection)
        #getMaxChangeset if it is not a parameter 
        maxChangeset = self.getMaxChangeset(connection) if maxChangeset is None else int(maxChangeset)
        print(f"The script will scan all changesets starting from {maxChangeset} backward")
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
       
        counter = maxChangeset

        while counter > 0: #2005-04-09 first changeset created date in OSM
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
                    and osh.changeset between '{counter - 50000}' and '{counter}'
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
            print(f'{datetime.now()} Calculating changesets stats from changesets between {counter - 50000} to {counter}')
            cursor.execute(sql)
            connection.commit()
            counter = counter - 50000

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
        
        listOfMissedChangesetsSql =  f'''
                   select t1.changeset
                        from (
                        select distinct osh.changeset
                                from public.osm_element_history osh        
                                where "timestamp" between NOW() - INTERVAL '1 DAY'  and now()
                                 and action != 'delete'
                                ) t1
                        left outer join (

                                select c.id
                                    from public.all_changesets_stats s
                                    join public.osm_changeset c on c.id = s.changeset where c.created_at between NOW() - INTERVAL '1 DAY'  and now()
                              ) t2
                        on t1.changeset = t2.id
                        where t2.id is null 
                        order by 1 desc              
        '''
               
        cursor.execute(listOfMissedChangesetsSql)
        
        records = cursor.fetchall() 
        print(f'''List of missed changesets {len(records)} in the last 24 hours \n{records}''',  )

        
        for row in records:
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
                and osh.changeset = {str(row[0])}
               group by changeset on conflict do nothing;

                '''
            # Apotential enhancement is to use and osh.changeset in ( ... ) for the where condition
            cursor.execute(sql)
            connection.commit()
            print(f"Inserted missed changeset # {str(row[0])}")  


        sql = f'''
              select max(changeset) latest_changeset from public.all_changesets_stats
                '''
        cursor.execute(sql)
        record = cursor.fetchone()
        print("The new calculated changetset ID=",record['latest_changeset'])

        cursor.close()    
    def updateFix(self,connection):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        start = int(args.startChangedet)
        while start > 0 :
            listOfMissedChangesetsSql =  f'''
             select t1.changeset
                            from (
                            select distinct osh.changeset
                                    from public.osm_element_history osh        
                                    where osh.changeset between  {start - 50000} and {start}
                                    and action != 'delete'
                                    ) t1
                            left outer join (
                                    select s.changeset
                                        from public.all_changesets_stats s                                      
                                        where s.changeset between  {start - 50000} and {start}
                                ) t2
                            on t1.changeset = t2.changeset
                            where t2.changeset is null 
                    
            '''
                
            cursor.execute(listOfMissedChangesetsSql)
            
            records = cursor.fetchall() 
            print(f'''{datetime.now()}: List of missed changesets {len(records)} between {start - 50000} and {start} ''' )
            
            for row in records:
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
                    and osh.changeset = {str(row[0])}
                group by changeset on conflict do nothing;

                    '''
                # Apotential enhancement is to use and osh.changeset in ( ... ) for the where condition
                cursor.execute(sql)
                connection.commit()
            
            if (len(records) > 0):
                print(f"Inserted missed {len(records)} changesets ")  
            start = start - 50000

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
argParser.add_argument('-F', '--fix', action='store_true', dest='fix', default=False, help='Fix amenity calcs and places from changeset while creating and fixing missing changesets when used with -U to update replications')
argParser.add_argument('-c', '--changeset', action='store', dest='maxChangeset', default=None, help='Maximum changeset ID to start from')
argParser.add_argument('-s', '--startdate', action='store', dest='startChangedet', default=None, help='Starting changeset ID to fix missing changesets')

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
    if (args.fix):
        md.updateFix(conn)
    else:
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