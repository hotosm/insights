#!/usr/bin/python
'''
osmh.py should parse the osm full historical file fullhistory.osm.bz2

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
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING'''
        
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

    def parseHistoryFile(self, connection, changesetFile, doReplication):
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
                members.append(int(elem.attrib['ref'])) 
                continue

            if elem.tag == 'node' or elem.tag == 'way' or elem.tag == 'relation':
                # print ('node tags',tags)
                parsedElements = parsedElements + 1
                if ("base" in args.fileName):
                    action = 'base'
                else:
                    if (elem.attrib.get('version', None) == '1'):
                        action = 'create'
                    else:
                        action = 'modify'

                    if (elem.tag == 'node' and  elem.attrib.get('lat', 0) == 0):
                        action = 'delete'
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
                 
                tags1 = {key: value[:] for key, value in tags.items()}
                nds1 = nds[:]
                members1 = members[:]
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
            # if (parsedCount == 22):
            #     sys.exit(0)

            if (elem.tag == 'node' and len(osm_element_history) >= 1000000) or (
                elem.tag == 'way' and len(osm_element_history) >= 500000)  or (
                elem.tag == 'relation' and len(osm_element_history) >= 300000):
                print ('Parsed',parsedElements,'item')
                self.insertNewBatch(connection, osm_element_history)
                connection.commit()
                endTime = datetime.now()
                timeCost = endTime - beginTime
                print ('Committed elements',elem.tag ,len(osm_element_history), 'in', timeCost)
                osm_element_history.clear() # important to avoid memory usage
                beginTime  = datetime.now()
            elem.clear() # important to avoid memory usage
            while elem.getprevious() is not None:
                del elem.getparent()[0]
        
        print ('Committing last batch',parsedCount)
        self.insertNewBatch(connection, osm_element_history)
        connection.commit()
        osm_element_history.clear()  
        print ("parsing complete, it was started:",startTime)
        print ("parsed {:,}".format(parsedCount),'Ended on: ',datetime.now())

    def fetchReplicationFile(self, sequenceNumber):
        topdir = '{:>03}'.format(str(math.floor(sequenceNumber / 1000000))) #format(sequenceNumber / 1000000, '000')
        subdir = '{:>03}'.format(str(math.floor((sequenceNumber / 1000) % 1000))) # format((sequenceNumber / 1000) % 1000, '000')
        fileNumber = '{:>03}'.format(str(sequenceNumber % 1000)) # format(sequenceNumber % 1000, '000')
        fileUrl = BASE_REPL_URL + topdir + '/' + subdir + '/' + fileNumber + '.osm.gz'
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
            cursor.execute('LOCK TABLE osm_changeset_state IN ACCESS EXCLUSIVE MODE NOWAIT')
        except psycopg2.OperationalError as e:
            print ("error getting lock on state table. Another process might be running")
            return 1
        cursor.execute('select * from osm_changeset_state')
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
        cursor.execute('update osm_changeset_state set update_in_progress = 1')
        connection.commit()
        print("latest sequence from the database: " + str(lastDbSequence))

        #No matter what happens after this point, execution needs to reach the update statement
        #at the end of this method to unlock the database or an error will forever leave it locked
        returnStatus = 0
        try:
            serverState = yaml.load(urllib2.urlopen(BASE_REPL_URL + "state.yaml"))
            lastServerSequence = serverState['sequence']
            print ("got sequence")
            lastServerTimestamp = serverState['last_run']
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
                        cursor.execute('update osm_changeset_state set last_sequence = %s', (currentSequence,))
                        connection.commit()
                        currentSequence += 1
                    timestamp = lastServerTimestamp
                print("finished with replication. Clearing status record")
            except Exception as e:
                print ("error during replication")
                print (e)
                returnStatus = 2
        cursor.execute('update osm_changeset_state set update_in_progress = 0, last_timestamp = %s', (timestamp,))
        connection.commit()
        return returnStatus




beginTime = datetime.now()
endTime = None
timeCost = None

argParser = argparse.ArgumentParser(description="Parse OSM Changeset metadata into a database")
argParser.add_argument('-t', '--trunc', action='store_true', default=False, dest='truncateTables', help='Truncate existing tables (also drops indexes)')
argParser.add_argument('-c', '--create', action='store_true', default=False, dest='createTables', help='Create tables')
argParser.add_argument('-H', '--host', action='store', dest='dbHost', help='Database hostname')
argParser.add_argument('-P', '--port', action='store', dest='dbPort', default=None, help='Database port')
argParser.add_argument('-u', '--user', action='store', dest='dbUser', default=None, help='Database username')
argParser.add_argument('-p', '--password', action='store', dest='dbPass', default=None, help='Database password')
argParser.add_argument('-d', '--database', action='store', dest='dbName', help='Target database', required=True)
argParser.add_argument('-f', '--file', action='store', dest='fileName', help='OSM baseline or history file, baseline file should contain base keyword in its name')
argParser.add_argument('-r', '--replicate', action='store_true', dest='doReplication', default=False, help='Apply a replication file to an existing database')
argParser.add_argument('-g', '--geometry', action='store_true', dest='createGeometry', default=False, help='Build geometry of changesets (requires postgis)')
argParser.add_argument('-re', '--region', action='store', dest='region', help='Region of the parsed file')

args = argParser.parse_args()

conn = psycopg2.connect(database=args.dbName, user=args.dbUser, password=args.dbPass, host=args.dbHost, port=args.dbPort)


md = osmh(args.createGeometry)
# if args.truncateTables:
#     md.truncateTables(conn)

# if args.createTables:
#     md.createTables(conn)

psycopg2.extras.register_hstore(conn)

# if(args.doReplication):
#     returnStatus = md.doReplication(conn)
#     sys.exit(returnStatus)

if not (args.fileName is None):
    if args.createGeometry:
        print ('parsing baseline file with geometries')
    else:
        print( 'parsing baseline file')
    changesetFile = None
    if(args.doReplication):
        changesetFile = gzip.open(args.fileName, 'rb')
    else:
        if(args.fileName[-4:] == '.bz2'):
            if(bz2Support):
                changesetFile = BZ2File(args.fileName)
            else:
                print ('ERROR: bzip2 support not available. Unzip file first or install bz2file')
                sys.exit(1)
        else:
            changesetFile = open(args.fileName, 'rb')

    if(changesetFile != None):
        md.parseHistoryFile(conn, changesetFile, args.doReplication)
    else:
        print ('ERROR: no baseline file opened. Something went wrong in processing args')
        sys.exist(1)

    if(not args.doReplication):
        None
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

print ('All done. Enjoy your (meta)data!')
