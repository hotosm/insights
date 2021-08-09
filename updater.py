#!/usr/bin/python
'''
osmh.py should parse the osm full historical changes

@author: Omran NAJJAR
'''

import os
import sys
import argparse
import lxml
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

BASE_REPL_URL = "http://planet.openstreetmap.org/replication/changesets/"

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
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,(select g.name
                                                            from public.geoboundaries g
                                                            where ST_CONTAINS(g.boundary,st_setsrid ('POINT(%s %s)'::geometry,4326))
limit 1)) ON CONFLICT DO NOTHING'''
        
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
        cursor.execute('''DELETE FROM osm_changeset_comment
                          WHERE comment_changeset_id = %s''', (id,))
        cursor.execute('''DELETE FROM osm_changeset
                          WHERE id = %s''', (id,))

    def parseFile(self, connection, changesetFile, doReplication):
        parsedCount = 0
        startTime = datetime.now()
        cursor = connection.cursor()
        context = etree.iterparse(changesetFile)        
        # action, root = context.next()
        changesets = []
        comments = []
        for action, elem in context:
            if(elem.tag != 'changeset'):
                continue

            parsedCount += 1

            tags = {}
            for tag in elem.iterchildren(tag='tag'):
                tags[tag.attrib['k']] = tag.attrib['v']

            for discussion in elem.iterchildren(tag='discussion'):
                for commentElement in discussion.iterchildren(tag='comment'):
                    for text in commentElement.iterchildren(tag='text'):
                       text = text.text
                    comment = (elem.attrib['id'], commentElement.attrib.get('uid'),  commentElement.attrib.get('user'), commentElement.attrib.get('date'), text)
                    comments.append(comment)

            if(doReplication):
                self.deleteExisting(connection, elem.attrib['id'])

            if self.createGeometry:
                changesets.append((elem.attrib['id'], elem.attrib.get('uid', None),   elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                                elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),  elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                                     elem.attrib.get('open', None), elem.attrib.get('num_changes', None), elem.attrib.get('user', None), tags,elem.attrib.get('min_lon', None), elem.attrib.get('min_lat', None),
                                    elem.attrib.get('max_lon', None), elem.attrib.get('max_lat', None)))
            else:
                changesets.append((elem.attrib['id'], elem.attrib.get('uid', None),   elem.attrib['created_at'], elem.attrib.get('min_lat', None),
                                elem.attrib.get('max_lat', None), elem.attrib.get('min_lon', None),  elem.attrib.get('max_lon', None), elem.attrib.get('closed_at', None),
                                     elem.attrib.get('open', None), elem.attrib.get('num_changes', None), elem.attrib.get('user', None), tags))

            if((parsedCount % 100000) == 0):
                self.insertNewBatch(connection, changesets)
                self.insertNewBatchComment(connection, comments )
                connection.commit()
                changesets = []
                comments = []
                print ("parsed %s" % ('{:,}'.format(parsedCount)))
                print ("cumulative rate: %s/sec" % '{:,.0f}'.format(parsedCount/timedelta.total_seconds(datetime.now() - startTime)))

            #clear everything we don't need from memory to avoid leaking
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        # Update whatever is left, then commit
        self.insertNewBatch(connection, changesets)
        self.insertNewBatchComment(connection, comments)
        connection.commit()
        print ("parsing complete")
        print ("parsed {:,}".format(parsedCount))

    def parseHistoryFile(self, connection, changesetFile,changesetId, doReplication):
        if (changesetFile == ''):
            return []
        parsedCount = 0
        startTime = datetime.now()
        cursor = connection.cursor()
        context = changesetFile    
        # action, root = context.next()
        osm_element_history = []
        # print ('context',context)
            # if(elem.tag != 'changeset'):
        for elem in context.iterchildren():
            # print (elem.tag)
            # print ('elem.tag',elem.tag)
            try:
                for elemItem in elem.iterchildren():
                    # print ('elemItem.tag',elemItem.tag,
                    # 'uid:',elemItem.attrib.get('uid', None),
                    # '''Time:''',elemItem.attrib.get('timestamp', None),
                    # '''id:''',elemItem.attrib.get('id', None),
                    # '''changeset:''',elemItem.attrib.get('changeset', None),
                    # '''version:''',elemItem.attrib.get('version', None))                    
                    # handle here node, way and relation 
                    tags = {}
                    for tag in elemItem.iterchildren(tag='tag'):
                            #print ('hash tag key',tag.get('k', None),'value:',tag.get('v', None))
                            tags[tag.attrib['k']] = tag.attrib['v']
                    # print('tags',tags)
                    # for ways: get all its nodes     
                    nds = []
                    for nd in elemItem.iterchildren(tag='nd'):
                        #print ('nd.attrib ref ',nd.attrib['ref'])
                        if nd.attrib.get('ref', None) != None:
                            nds.append(int(nd.attrib['ref'])) 
                    # print('nds',nds)
                        
                    members = []
                    for member in elemItem.iterchildren(tag='member'):
                        if member.attrib.get('ref', None) != None:
                            members.append(int(member.attrib['ref'])) 
                    # print('members',members)
                    
                    osm_element_history.append((elemItem.attrib.get('id', None), 
                                                elemItem.tag,  # elemnt type node, way, relation
                                                tags, # tags
                                                elemItem.attrib.get('lat', 0), # lat for node only
                                                elemItem.attrib.get('lon', 0), # lon for node only
                                                nds, # nds for way only
                                                members,# members for relation only
                                                changesetId, 
                                                elemItem.attrib.get('timestamp', None), # timestamp for all
                                                elemItem.attrib.get('uid', None), # uid for all
                                                elemItem.attrib.get('version', None), # version for all
                                                elem.tag, # action= create, modify, delete
                                                elemItem.attrib.get('lon', 0), # lon for the country from boundaries
                                                elemItem.attrib.get('lat', 0) # lat for the country from boundaries
                                                ))
                                                                               
            except Exception as e:
                print ("error parsing changeset id", changesetId)
                print (e)
                return []
        print("finished changeset id ", changesetId, ' successfully')
        return osm_element_history
        
    def fetchReplicationFile(self, sequenceNumber):
        # topdir = '{:>03}'.format(str(math.floor(sequenceNumber / 1000000))) #format(sequenceNumber / 1000000, '000')
        # subdir = '{:>03}'.format(str(math.floor((sequenceNumber / 1000) % 1000))) # format((sequenceNumber / 1000) % 1000, '000')
        # fileNumber = '{:>03}'.format(str(sequenceNumber % 1000)) # format(sequenceNumber % 1000, '000')
        fileUrl = 'https://www.openstreetmap.org/api/0.6/changeset/'+ str(sequenceNumber) +'/download'
        # print ("opening replication file at " + fileUrl)
        
        try:
            replicationFile = urllib2.urlopen(fileUrl)
            data = replicationFile.read()
            # data = data.
            replicationFile.close()

            data = etree.fromstring(data)

            # print('data', data)
            # replicationData = gzip.open(replicationFile)
            #print('replicationData', replicationData)            
        except Exception as e:
            print ("error during replicationFile urllib2.urlopen(fileUrl)")
            print (e,'chengeset id not found: ',sequenceNumber )
            return ''
        
        return data

    def doReplication(self, connection):
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            # cursor.execute('LOCK TABLE osm_changeset_state IN ACCESS EXCLUSIVE MODE NOWAIT')
            None
        except psycopg2.OperationalError as e:
            print ("error getting lock on state table. Another process might be running")
            return 1
        # cursor.execute('select * from osm_changeset_state')
        # dbStatus = cursor.fetchone()
        # lastDbSequence = dbStatus['last_sequence']
        # timestamp = None
        # lastServerTimestamp = None
        # newTimestamp = None
        # if(dbStatus['last_timestamp'] is not None):
        #     timestamp = dbStatus['last_timestamp']
        # print( "latest timestamp in database: " + str(timestamp))
        # if(dbStatus['update_in_progress'] == 1):
        #     print( "concurrent update in progress. Bailing out!")
        #     return 1
        # if(lastDbSequence == -1):
        #     print ("replication state not initialized. You must set the sequence number first.")
        #     return 1
        # cursor.execute('update osm_changeset_state set update_in_progress = 1')
        # connection.commit()
        # print("latest sequence from the database: " + str(lastDbSequence))

        #No matter what happens after this point, execution needs to reach the update statement
        #at the end of this method to unlock the database or an error will forever leave it locked
        returnStatus = 0
        try:
            # serverState = yaml.load(urllib2.urlopen(BASE_REPL_URL + "state.yaml"))
            # lastServerSequence = serverState['sequence']
            # print ("got sequence")
            # lastServerTimestamp = serverState['last_run']
            # print( "last timestamp on server: " + str(lastServerTimestamp))
            None
            lastServerSequence = 100000000 -1 # should be the change set ID
        except Exception as e:
            print ("error retrieving server state file. Bailing on replication")
            print (e)
            returnStatus = 2
        else:
            try:
                rng = range(int(args.fromId), int(args.toId)+1)
                accumlativeChangesets = []
                scanedItems = 0
                for changesetId in rng:                    
                    # print("parsing change set changeset: " + str(changesetId))
                    # while(currentSequence <= lastServerSequence):
                    osm_element_history = self.parseHistoryFile(connection, self.fetchReplicationFile(changesetId),changesetId, True)
                    accumlativeChangesets = accumlativeChangesets + osm_element_history
                    scanedItems = scanedItems + len(osm_element_history)
                    if (len(accumlativeChangesets) > 50000):
                        print('Commited osm elements:', scanedItems)
                        self.insertNewBatch(connection, accumlativeChangesets)
                        connection.commit()
                        accumlativeChangesets.clear()
                    # print ("parsed change set id {:,}".format(changesetId))
                    # cursor.execute('update osm_changeset_state set last_sequence = %s', (currentSequence,))
                    # connection.commit()
                    # currentSequence += 1
                    # timestamp = lastServerTimestamp
                # insert the remaining 
                print('Commited osm elements:', scanedItems)
                self.insertNewBatch(connection, accumlativeChangesets)
                connection.commit()
                accumlativeChangesets.clear()
            except Exception as e:
                print ("error during replication",e)
                returnStatus = 2
        # cursor.execute('update osm_changeset_state set update_in_progress = 0, last_timestamp = %s', (timestamp,))
        # connection.commit()
        return returnStatus




beginTime = datetime.now()
endTime = None
timeCost = None

argParser = argparse.ArgumentParser(description="Parse OSM Changeset data into a database")
argParser.add_argument('-t', '--trunc', action='store_true', default=False, dest='truncateTables', help='Truncate existing tables (also drops indexes)')
argParser.add_argument('-c', '--create', action='store_true', default=False, dest='createTables', help='Create tables')
argParser.add_argument('-H', '--host', action='store', dest='dbHost', help='Database hostname')
argParser.add_argument('-P', '--port', action='store', dest='dbPort', default=None, help='Database port')
argParser.add_argument('-u', '--user', action='store', dest='dbUser', default=None, help='Database username')
argParser.add_argument('-p', '--password', action='store', dest='dbPass', default=None, help='Database password')
argParser.add_argument('-d', '--database', action='store', dest='dbName', help='Target database', required=True)
argParser.add_argument('-f', '--file', action='store', dest='fileName', help='OSM changeset file to parse')
argParser.add_argument('-r', '--replicate', action='store_true', dest='doReplication', default=False, help='Apply a replication file to an existing database')
argParser.add_argument('-from', '--from', action='store', dest='fromId', help='Starting changeset id ')
argParser.add_argument('-to', '--to', action='store', dest='toId',help='Stopping changeset id')
argParser.add_argument('-g', '--geometry', action='store_true', dest='createGeometry', default=False, help='Build geometry of changesets (requires postgis)')

args = argParser.parse_args()

conn = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass, host=args.dbHost, port=args.dbPort)


md = osmh(args.createGeometry)
# if args.truncateTables:
#     md.truncateTables(conn)

# if args.createTables:
#     md.createTables(conn)

psycopg2.extras.register_hstore(conn)

if (args.fromId == None or args.toId == None):
    print('Error: -from and -to args are required')
    sys.exit(0)
if(args.doReplication):
    returnStatus = md.doReplication(conn)
    sys.exit(returnStatus)

endTime = datetime.now()
timeCost = endTime - beginTime

print( 'Processing time cost is ', timeCost)

print ('All done. Enjoy your (meta)data!')
