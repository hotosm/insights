#!/usr/bin/python
'''
osmh.py should parse the osm full historical file .osm.bz2 of a specific county 

@author: Omran NAJJAR
'''

import os
import sys
import argparse
import psycopg2
import psycopg2.extras
import queries
import gzip
import urllib.request as urllib2
import yaml
from lxml import etree
from datetime import datetime
from datetime import timedelta
from io import StringIO
import math
try:
    from bz2file import BZ2File
    bz2Support = True
except ImportError:
    bz2Support = False

class osmh():
    def __init__(self, createGeometry):
        self.createGeometry = createGeometry

    def truncateTables(self, connection):
        print('truncating tables')
        cursor = connection.cursor()
        cursor.execute("TRUNCATE TABLE osm_changeset_comment CASCADE;")
        cursor.execute("TRUNCATE TABLE osm_changeset CASCADE;")
        cursor.execute(queries.dropIndexes)
        cursor.execute("UPDATE osm_changeset_state set last_sequence = -1, last_timestamp = null, update_in_progress = 0")
        connection.commit()

    def createTables(self, connection):
        print ('creating tables')
        cursor = connection.cursor()
        cursor.execute(queries.createChangesetTable)
        cursor.execute(queries.initStateTable)
        if self.createGeometry:
            cursor.execute(queries.createGeometryColumn)
        connection.commit()

    def insertNewBatch(self, connection, data_arr):
        cursor = connection.cursor()
        
        sql = '''INSERT INTO public.osm_element_history
                (id, "type", tags, lat, lon, nds, members, changeset, "timestamp", uid, "version", "action",country)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING'''
        
        psycopg2.extras.execute_batch(cursor, sql, data_arr)
        
        cursor.close()

    def insertBoundary(self, connection, boundary):


        polyFile = urllib2.urlopen(boundary)
        data = polyFile.read()
        polyFile.close()

        poly = data.decode('utf-8')
        # print('data',poly.split('\n'))
        
        points = {}
        currentPoly = 0
        i = 1
        for l in poly.split('\n'):
            # print(l)
            arr= l.split(' ')
            if len(arr) == 7:
                # print (float(arr[3]),float(arr[6]))
                points[str(currentPoly)] = points[str(currentPoly)] + str(float(arr[3])) + ' ' + str(float(arr[6])) + ','
            
            else:
                if (len(arr) == 1) and arr[0] == str(i):
                    i = i + 1
                    currentPoly = int(arr[0]) - 1
                    points[str(currentPoly)] =''
        # print('points',points)
        postgisPolygon = ''
        if (len(points) == 1):
            postgisPolygon = 'POLYGON(('+points["0"][0:-1]+'))'
        else:
            postgisPolygon = 'MULTIPOLYGON(('
            for i in range(0,len(points)) :
                postgisPolygon = postgisPolygon + '('+points[str(i)][0:-1] + '),'
                None
            postgisPolygon = postgisPolygon[0:-1] + '))'
        # print ('postgisPolygon',postgisPolygon)
        countryName = boundary.split('/')[len(boundary.split('/'))-1][0:-5].capitalize()

        # print (countryName)
        cursor = connection.cursor()

        cursor.execute(queries.createBoundaries)
        connection.commit()

        sql = "INSERT INTO boundaries (name_en,boundary,priority) values ('"+countryName+"','"+postgisPolygon+"'::geometry,false) ON CONFLICT (name_en) DO update set boundary = '"+postgisPolygon+"'::geometry"
        # print (sql)
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        print(countryName, 'inserted into boundaries table')


    def insertNewBatchReplication(self, connection, data_arr):
        cursor = connection.cursor()
        
        sql = '''INSERT INTO public.osm_element_history
                (id, "type", tags, lat, lon, nds, members, changeset, "timestamp", uid, "version", "action",country)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,(select g.name_en from public.boundaries g where ST_CONTAINS(st_setsrid(g.boundary,4326),st_setsrid ('POINT(%s %s)'::geometry,4326)) limit 1)) ON CONFLICT DO NOTHING'''
        
        psycopg2.extras.execute_batch(cursor, sql, data_arr)
        
        cursor.close()

    def insertNewBatchComment(self, connection, comment_arr):
        cursor=connection.cursor()
        sql = '''INSERT into osm_changeset_comment
                    (comment_changeset_id, comment_user_id, comment_user_name, comment_date, comment_text)
                    values (%s,%s,%s,%s,%s)'''
        psycopg2.extras.execute_batch(cursor, sql, comment_arr)
        cursor.close()

    def deleteExisting(self, connection, id):
        cursor = connection.cursor()
        # cursor.execute('''DELETE FROM osm_changeset
        #                   WHERE id = %s''', (id,))
    def getWayRelationLonLat(self, type, id):
        
        if (type == 'node'):
            try:
                url =   'https://www.openstreetmap.org/api/0.6/'+type+'/' + str(id) + '/history'
                # url = 'https://www.openstreetmap.org/api/0.6/node/60337559/history'
                # print ('url',url)
                replicationFile = urllib2.urlopen(url)
                data = replicationFile.read()
                # data = data.
                replicationFile.close()

                data = etree.fromstring(data)
                # context = etree.iterparse(data)        
                
                # print ('context',data)
                lat = 0
                lon = 0
                for elem in data.iterchildren():
                    if (elem.tag == 'node' and elem.attrib.get('lat',0) != 0):
                        # print ('elem.attrib',elem.attrib)
                        lat = elem.attrib.get('lat',0)
                        lon = elem.attrib.get('lon',0)
                return (lon,lat)
            except Exception as e:
                print ('element ',type,'lat,lon not found',e)
                return (0,0)
                
        if (type == 'way'):
            try:
                url = 'https://www.openstreetmap.org/api/0.6/'+type+'/' + str(id) + '/history'
                # url = 'https://www.openstreetmap.org/api/0.6/node/60337559/history'
                # print ('url',url)
                replicationFile = urllib2.urlopen(url)
                data = replicationFile.read()
                # data = data.
                replicationFile.close()

                data = etree.fromstring(data)
                # context = etree.iterparse(data)        
                
                # print ('context',len(data))
                
                lastVersionWay = data[len(data)-1]
                ndId = -1
                i = len(data)-1
                while i >=0 :
                    # print ('i',i)
                    lastVersionWay = data[i]
                    for elemWay in lastVersionWay.iterchildren(tag='nd'):
                        ndId = elemWay.attrib.get('ref',0)
                        # print('ndId',ndId)
                        if (int(ndId) > 0):
                            break
                    if (int(ndId) > 0):
                            break
                    i = i-1
                return self.getWayRelationLonLat('node',ndId)
            except Exception as e:
                print ('element ',type,'lat,lon not found',e)
                return (0,0)

        if (type == 'relation'):
            try:
                url =   'https://www.openstreetmap.org/api/0.6/'+type+'/' + str(id) + '/history'
                # url = 'https://www.openstreetmap.org/api/0.6/node/60337559/history'
                # print ('url',url)
                replicationFile = urllib2.urlopen(url)
                data = replicationFile.read()
                # data = data.
                replicationFile.close()

                data = etree.fromstring(data)
                # context = etree.iterparse(data)        
                ndId = -1
                i = len(data)-1
                memberType = ''
                while i >=0 :
                    # print ('i',i)
                    lastVersionWay = data[i]
                    for elemWay in lastVersionWay.iterchildren(tag='member'):
                        ndId = elemWay.attrib.get('ref',0)
                        memberType = elemWay.attrib.get('type',0)
                        if (int(ndId) > 0):
                            break
                    if (int(ndId) > 0):
                            break
                    i = i-1
                
                return self.getWayRelationLonLat(memberType,ndId)
            except Exception as e:
                print ('element ',type,'lat,lon not found',e)
                return (0,0)
        
        # cursor.execute('''DELETE FROM osm_changeset
        #                   WHERE id = %s''', (id,))

    def parseFile(self, connection, changesetFile, doReplication):
        parsedCount = 0
        startTime = datetime.now()
        cursor = connection.cursor()
        context = etree.iterparse(changesetFile)        
        # action, root = context.next()
        tags = {}
        nds = []
        members = []
        nodes = []
        ways = []
        relations = []
        osm_element_history = []
        try :
            for a,elem in context:
                # print('elem.tag',elem.tag)
                parsedCount += 1

                if elem.tag == 'bounds':
                    continue

                if elem.tag == 'tag':
                    tags[elem.attrib.get('k')] = elem.attrib.get('v')
                    continue
                
                if elem.tag == 'nd':
                    nds.append(int(elem.attrib['ref'])) 
                    continue

                if elem.tag == 'member':
                    members.append([elem.attrib['ref'],elem.attrib['type'],elem.attrib.get('role','unknown')]) 
                    continue    
                
                if elem.tag == 'node':
                   
                    tags1 = None if len(tags) == 0 else  {key: value[:] for key, value in tags.items()}
                    nodes.append((elem.attrib.get('id', None), 
                                                elem.tag,  # elemnt type node, way, relation
                                                tags1, # tags
                                                elem.attrib.get('lat', 0), # lat for node only
                                                elem.attrib.get('lon', 0), # lon for node only
                                                None, # nds for way only
                                                None,# members for relation only
                                                elem.attrib.get('changeset', None), 
                                                elem.attrib.get('timestamp', None), # timestamp for all
                                                elem.attrib.get('uid', None), # uid for all
                                                elem.attrib.get('version', None), # version for all
                                                # action is still missing
                                                # country is still missing 
                                                ))
                    tags.clear()
                if elem.tag == 'way':
                    tags1 = None if len(tags) == 0 else {key: value[:] for key, value in tags.items()}
                    nds1 = None if len(nds) == 0 else  nds[:]        
                    (lon,lat) = (0,0) # TODO: separate getting geo info for way and relation to another process using  md.getWayRelationLonLat('way',elem.attrib.get('id', 0))        
                    ways.append((elem.attrib.get('id', None), 
                                                elem.tag,  # elemnt type node, way, relation
                                                tags1, # tags
                                                lat, # lat 
                                                lon, # lon 
                                                nds1, # nds for way only
                                                None,# members for relation only
                                                elem.attrib.get('changeset', None), 
                                                elem.attrib.get('timestamp', None), # timestamp for all
                                                elem.attrib.get('uid', None), # uid for all
                                                elem.attrib.get('version', None), # version for all
                                                # action is still missing
                                                # country is still missing 
                                                ))
                    tags.clear()
                    nds.clear()
                if elem.tag == 'relation':
                    tags1 = None if len(tags) == 0 else {key: value[:] for key, value in tags.items()}
                    members1 = None if len(members) == 0 else members[:]         
                    (lon,lat) = (0,0) # TODO: separate getting geo info for way and relation to another process using  md.getWayRelationLonLat('way',elem.attrib.get('id', 0))        
                    relations.append((elem.attrib.get('id', None), 
                                                elem.tag,  # elemnt type node, way, relation
                                                tags1, # tags
                                                lat,
                                                lon, # 
                                                None, # nds for way only
                                                members1,# members for relation only
                                                elem.attrib.get('changeset', None), 
                                                elem.attrib.get('timestamp', None), # timestamp for all
                                                elem.attrib.get('uid', None), # uid for all
                                                elem.attrib.get('version', None), # version for all
                                                # action is still missing
                                                # country is still missing 
                                                ))
                    tags.clear()
                    members.clear()
                if elem.tag == 'create' or elem.tag == 'delete' or elem.tag == 'modify':
                    for (id,ntag,ntags,nlat,nlon,nnds,nmembers,nchangeset,ntimestamp,nuid,nversion) in nodes:
                        osm_element_history.append((id, 
                                                ntag,  # elemnt type node, way, relation
                                                ntags, # tags
                                                nlat, # lat for node only
                                                nlon, # lon for node only
                                                nnds, # nds for way only
                                                nmembers,# members for relation only
                                                nchangeset, 
                                                ntimestamp, # timestamp for all
                                                nuid, # uid for all
                                                nversion, # version for all
                                                elem.tag, # action= create, modify, delete, base (for base line items)
                                                float(nlon),
                                                float(nlat)))

                    for (id,ntag,ntags,nlat,nlon,wnds,nmembers,nchangeset,ntimestamp,nuid,nversion) in ways:
                        osm_element_history.append((id, 
                                                ntag,  # elemnt type node, way, relation
                                                ntags, # tags
                                                nlat, # lat for node only
                                                nlon, # lon for node only
                                                wnds, # nds for way only
                                                nmembers,# members for relation only
                                                nchangeset, 
                                                ntimestamp, # timestamp for all
                                                nuid, # uid for all
                                                nversion, # version for all
                                                elem.tag, # action= create, modify, delete, base (for base line items)
                                                float(nlon),
                                                float(nlat)))
                    for (id,ntag,ntags,nlat,nlon,nnds,nmembers,nchangeset,ntimestamp,nuid,nversion) in relations:
                        osm_element_history.append((id, 
                                                ntag,  # elemnt type node, way, relation
                                                ntags, # tags
                                                nlat, # lat for node only
                                                nlon, # lon for node only
                                                nnds, # nds for way only
                                                nmembers,# members for relation only
                                                nchangeset, 
                                                ntimestamp, # timestamp for all
                                                nuid, # uid for all
                                                nversion, # version for all
                                                elem.tag, # action= create, modify, delete, base (for base line items)
                                                float(nlon),
                                                float(nlat)))
                    None
                    nodes.clear()
                    ways.clear()
                    relations.clear()
                # if self.createGeometry:
                #     changesets.append((elem.attrib['id'], elem.attrib.get('uid', None),   elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                #                     elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),  elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                #                          elem.attrib.get('open', None), elem.attrib.get('num_changes', None), elem.attrib.get('user', None), tags,elem.attrib.get('min_lon', None), elem.attrib.get('min_lat', None),
                #                         elem.attrib.get('max_lon', None), elem.attrib.get('max_lat', None)))
                # # else:
                # #     changesets.append((elem.attrib['id'], elem.attrib.get('uid', None),   elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                # #                     elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),  elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                # #                          elem.attrib.get('open', None), elem.attrib.get('num_changes', None), elem.attrib.get('user', None), tags))
        except Exception as e:
            print ("error parsing .osc.gz file")
            print (e)
            return sys.exit(1)  
        # Update whatever is left, then commit

        # #TODO: get lat lon for way and relation
        # for elm in osm_element_history:
            
        #     (id,ntag,ntags,nlat,nlon,nnds,nmembers,nchangeset,ntimestamp,nuid,nversion,action,lot,lat) = elm
        #     if ntag =='way':
        #         print('nnds[0]',nnds[0])

        self.insertNewBatchReplication(connection, osm_element_history)
        # self.insertNewBatchComment(connection, comments)
        connection.commit()
        osm_element_history.clear()
        print ("parsing complete")
        print ("parsed {:,}".format(parsedCount))
        
    def parseHistoryFile(self, connection, changesetFile):
        parsedCount = 0
        parsedElements = 0
        startTime = datetime.now()
        #cursor = connection.cursor()
        context = etree.iterparse(changesetFile)        
        # action, root = context.next()
        osm_element_history = []
        tags = {}
        nds = []
        members = [] 
        beginTime  = datetime.now()
        listSize = 0
        for action, elem in context:
            # print('tags ',tags,'osm_element_history',osm_element_history )  
            parsedCount += 1
            if elem.tag == 'bounds':
                continue

            if elem.tag == 'tag':
                tags[elem.attrib.get('k')] = elem.attrib.get('v')
                continue
            
            if elem.tag == 'nd':
                nds.append(int(elem.attrib['ref'])) 
                continue

            if elem.tag == 'member':
                members.append([elem.attrib['ref'],elem.attrib['type'],elem.attrib.get('role','unknown')]) 
                # <member type="way" ref="148653924" role="forward"/>
                continue

            if elem.tag == 'node' or elem.tag == 'way' or elem.tag == 'relation':
                # print ('node tags',tags)
                parsedElements = parsedElements + 1
                
                if (elem.attrib.get('version', None) == '1'):
                    action = 'create'
                else:
                    action = 'modify'

                if (elem.tag == 'node' and  elem.attrib.get('lat', 0) == 0):
                    action = 'delete'

                if (elem.tag == 'way' and  len(nds) == 0 and len(tags) == 0):
                    action = 'delete'

                if (elem.tag == 'relation' and  len(members) == 0 and len(tags) == 0):
                    action = 'delete'
                    # TODO way is deleted when its visibility is false and no properties inside it

                # print ('elem.attrib',elem.attrib)
                # tags = {}
                # for tagElem in elem.iterchildren(tag='tag'):
                #         print ('hash tag key',tagElem.attrib,'elem.tag',elem.tag)
                #         tags[tagElem.attrib.get('k')] = tagElem.attrib.get('v')
                # print('tags',tags)
                # for ways: get all its nodes
            
                # print('node 1 tags',elem.attrib)
                
                
                # for nd in elem.iterchildren(tag='nd'):
                #     #print ('nd.attrib ref ',nd.attrib['ref'])
                #     if nd.attrib.get('ref', None) != None:
                #         nds.append(int(nd.attrib['ref'])) 
                # # print('nds',nds)
                    
                
                # for member in elem.iterchildren(tag='member'):
                #     if member.attrib.get('ref', None) != None:
                #         members.append(int(member.attrib['ref'])) 
                 
                tags1 = None if len(tags) == 0 else {key: value[:] for key, value in tags.items()}
                nds1 =  None if len(nds) == 0 else nds[:]
                members1 =  None if len(members) == 0 else members[:]
                osm_element_history.append((elem.attrib.get('id', None), 
                                            elem.tag,  # elemnt type node, way, relation
                                            tags1, # tags
                                            elem.attrib.get('lat', 0), # lat for node only
                                            elem.attrib.get('lon', 0), # lon for node only
                                            nds1, # nds for way only
                                            members1,# members for relation only
                                            elem.attrib.get('changeset', None), 
                                            elem.attrib.get('timestamp', None), # timestamp for all
                                            elem.attrib.get('uid', None), # uid for all
                                            elem.attrib.get('version', None), # version for all
                                            action, # action= create, modify, delete, base (for base line items)
                                            args.region))
                tags.clear()
                nds.clear()
                members.clear()
                listSize += sys.getsizeof(osm_element_history[len(osm_element_history)-1])

            # if (parsedCount == 22):
            #     sys.exit(0)
            # To avoid extra memory usage 
            if (elem.tag == 'node' and len(osm_element_history) >= 1000000) or (
                elem.tag == 'way' and len(osm_element_history) >= 500000)  or (
                elem.tag == 'relation' and len(osm_element_history) >= 10000) or (
                listSize >= 200000000): # if the size of the list reaches ~ 200 MB
                print ('Parsed',parsedElements,'elements')
                print('with osm_element_history siz=',listSize, 'bytes')
                self.insertNewBatch(connection, osm_element_history)
                connection.commit()
                endTime = datetime.now()
                timeCost = endTime - beginTime
                print ('Committed elements',elem.tag ,len(osm_element_history), 'in', timeCost)
                osm_element_history.clear() # important to avoid memory usage
                beginTime  = datetime.now()
                listSize = 0
            elem.clear() # important to avoid memory usage
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        print ('Committing last batch',len(osm_element_history),' elements')
        self.insertNewBatch(connection, osm_element_history)
        connection.commit()
        osm_element_history.clear()  
        print ("parsing complete, it was started:",startTime)
        print ("parsed {:,}".format(parsedElements),'Ended on: ',datetime.now())

    def fetchReplicationFile(self, sequenceNumber):
        topdir = '{:>03}'.format(str(math.floor(sequenceNumber / 1000000))) #format(sequenceNumber / 1000000, '000')
        subdir = '{:>03}'.format(str(math.floor((sequenceNumber / 1000) % 1000))) # format((sequenceNumber / 1000) % 1000, '000')
        fileNumber = '{:>03}'.format(str(sequenceNumber % 1000)) # format(sequenceNumber % 1000, '000')
        fileUrl = BASE_REPL_URL + topdir + '/' + subdir + '/' + fileNumber + '.osc.gz'
        print ("opening replication file at " + fileUrl)
        
        try:
            replicationFile = urllib2.urlopen(fileUrl)
            #print('replicationFile', replicationFile)
            replicationData = gzip.open(replicationFile)
            #print('replicationData', replicationData)            
        except Exception as e:
            print ("error during replicationFile urllib2.urlopen(fileUrl)")
            print (e)
            return ''
        
        return replicationData

    def doReplication(self, connection):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cursor.execute('LOCK TABLE osm_element_history_state IN ACCESS EXCLUSIVE MODE NOWAIT')
        except psycopg2.OperationalError as e:
            print ("error getting lock on state table. Another process might be running")
            return 1
        cursor.execute('select * from osm_element_history_state')
        dbStatus = cursor.fetchone()
        lastDbSequence = dbStatus['last_sequence']
        timestamp = None
        lastServerTimestamp = None
        newTimestamp = None
        if(dbStatus['last_timestamp'] is not None):
            timestamp = dbStatus['last_timestamp']
        print( "latest timestamp in database: " + str(timestamp))
        if(dbStatus['update_in_progress'] == 1):
            print( "concurrent update in progress. Bailing out!")
            return 1
        if(lastDbSequence == -1):
            print ("replication state not initialized. You must set the sequence number first.")
            return 1
        cursor.execute('update osm_element_history_state set update_in_progress = 1')
        connection.commit()
        print("latest sequence from the database: " + str(lastDbSequence))

        #No matter what happens after this point, execution needs to reach the update statement
        #at the end of this method to unlock the database or an error will forever leave it locked
        returnStatus = 0
        try:
            serverState = yaml.load(urllib2.urlopen(BASE_REPL_URL + "state.txt"))
            print ("serverState",serverState.split(' ')[0].split('=')[1])
            lastServerSequence = int(serverState.split(' ')[0].split('=')[1])
            print ("got sequence")
            lastServerTimestamp = serverState.split(' ')[1].split('=')[1].replace("\\", "")
            print( "last timestamp on server: " + str(lastServerTimestamp))
        except Exception as e:
            print ("error retrieving server state file. Bailing on replication")
            print (e)
            returnStatus = 2
        else:
            try:
                print("latest sequence on OSM server: " + str(lastServerSequence))
                if(lastServerSequence > lastDbSequence):
                    print("server has new sequence. commencing replication")
                    currentSequence = lastDbSequence + 1
                    while(currentSequence <= lastServerSequence):
                        self.parseFile(connection, self.fetchReplicationFile(currentSequence), True)
                        cursor.execute('update osm_element_history_state set last_sequence = %s', (currentSequence,))
                        connection.commit()
                        currentSequence += 1
                    timestamp = lastServerTimestamp
                print("finished with replication. Clearing status record")
            except Exception as e:
                print ("error during replication")
                print (e)
                returnStatus = 2
        cursor.execute('update osm_element_history_state set update_in_progress = 0, last_timestamp = %s', (timestamp,))
        connection.commit()
        return returnStatus




beginTime = datetime.now()
endTime = None
timeCost = None

argParser = argparse.ArgumentParser(description="Parse OSM elements history into a PG database")
argParser.add_argument('-t', '--trunc', action='store_true', default=False, dest='truncateTables', help='Truncate existing tables (also drops indexes)')
argParser.add_argument('-c', '--create', action='store_true', default=False, dest='createTables', help='Create tables')
argParser.add_argument('-H', '--host', action='store', dest='dbHost', help='Database hostname')
argParser.add_argument('-P', '--port', action='store', dest='dbPort', default=None, help='Database port')
argParser.add_argument('-u', '--user', action='store', dest='dbUser', default=None, help='Database username')
argParser.add_argument('-p', '--password', action='store', dest='dbPass', default=None, help='Database password')
argParser.add_argument('-d', '--database', action='store', dest='dbName', help='Target database', required=True)
argParser.add_argument('-f', '--file', action='store', dest='fileName', help='OSM baseline or history file, baseline file should contain base keyword in its name')
argParser.add_argument('-b', '--boundary', action='store', dest='boundary', help='GeoJSON file for the boundaries. it support .gz zipped files')
argParser.add_argument('-r', '--replicate', action='store_true', dest='doReplication', default=False, help='Apply a replication file to an existing database')
argParser.add_argument('-g', '--geometry', action='store_true', dest='createGeometry', default=False, help='Build geometry of changesets (requires postgis)')
argParser.add_argument('-re', '--region', action='store', dest='region', help='Region of the parsed file')
argParser.add_argument('-freq', '--frequancy', action='store', dest='frequancy',default='hour', help='Replication frequancy, (default = hour), minute, day are the other values and should consider the sequance in osm_element_history_state table')

args = argParser.parse_args()

conn = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass, host=args.dbHost, port=args.dbPort)

BASE_REPL_URL = "https://planet.openstreetmap.org/replication/"+args.frequancy+"/"
print("BASE_REPL_URL",BASE_REPL_URL)
md = osmh(args.createGeometry)
# if args.truncateTables:
#     md.truncateTables(conn)

# if args.createTables:
#     md.createTables(conn)

psycopg2.extras.register_hstore(conn)
# relation/60337
# print ('node log lat',md.getWayRelationLonLat('relation',60337))

# sys.exit(1)
if(args.doReplication):
    returnStatus = md.doReplication(conn)
    sys.exit(returnStatus)

if not (args.boundary is None):
    try:
        if(args.boundary[-5:] == '.poly'):
            None
            print( 'Parsing boundary .poly file', args.boundary)
            md.insertBoundary(conn, args.boundary)    
            sys.exit(0)      
    except Exception as e:
        print ("error during loading boundary")
        print (e)
        sys.exit(1)
    None

if not (args.fileName is None):
    print( 'Parsing history file', args.fileName)
    historyFile = None
    if(args.fileName[-4:] == '.bz2'):
        if(bz2Support):
            historyFile = BZ2File(args.fileName)
        else:
            print ('ERROR: bzip2 support not available. Unzip file first or install bz2file')
            sys.exit(1)
    else:
        historyFile = open(args.fileName, 'rb')        

    if(historyFile != None):
        print ('Checking Osm Element History Table and State Table')
        cursor = conn.cursor()
        cursor.execute(queries.createOsmHistoryTable)
        conn.commit()
        md.parseHistoryFile(conn, historyFile)
    else:
        print ('ERROR: no file opened. Something went wrong in processing args')
        sys.exist(1)

    if(not args.doReplication):
        None 
        ## TODO: create constraint based on a parameter 
        #cursor = conn.cursor()
        # print ('creating constraints')
        #cursor.execute(queries.createConstraints)
        # print ('creating indexes')
        #cursor.execute(queries.createIndexes)
        #if args.createGeometry:
            #cursor.execute(queries.createGeomIndex)
        #conn.commit()

    conn.close()

endTime = datetime.now()
timeCost = endTime - beginTime

print( 'Processing time cost is ', timeCost)

print ('All done. Enjoy your Historical OSM Elements!')
