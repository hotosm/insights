ChangesetMD
=========

ChangesetMD is a simple XML parser written in python that takes the weekly changeset metadata dump file from http://planet.openstreetmap.org/ and shoves the data into a simple postgres database so it can be queried.

It can also keep a database created with a weekly dump file up to date using minutely changeset diff files available at [http://planet.osm.org/replication/changesets/](http://planet.osm.org/replication/changesets/)

Setup
------------

ChangesetMD works with python 2.7.

Aside from postgresql, ChangesetMD depends on the python libraries psycopg2 and lxml.
On Debian-based systems this means installing the python-psycopg2 and python-lxml packages.

If you want to parse the changeset file without first unzipping it, you will also need to install the [bz2file library](http://pypi.python.org/pypi/bz2file) since the built in bz2 library can not handle multi-stream bzip files.

For building geometries, ```postgis``` extension needs to be [installed](http://postgis.net/install).

ChangesetMD expects a postgres database to be set up for it. It can likely co-exist within another database if desired. Otherwise, As the postgres user execute:

    createdb changesets

It is easiest if your OS user has access to this database. I just created a user and made myself a superuser. Probably not best practices.

    createuser <username>


Execution
------------
The first time you run it, you will need to include the -c | --create option to create the table:

    python changesetmd.py -d <database> -c

The create function can be combined with the file option to immediately parse a file.

To parse a dump file, use the -f | --file option.

    python changesetmd.py -d <database> -f /tmp/changeset-latest.osm

If no other arguments are given, it will access postgres using the default settings of the postgres client, typically connecting on the unix socket as the current OS user. Use the ```--help``` argument to see optional arguments for connecting to postgres.

You can add the `-g` | `--geometry` option to build polygon geometries (the database also needs to be created with this option).

Replication
------------
After you have parsed a weekly dump file into the database, the database can be kept up to date using changeset diff files that are generated on the OpenStreetMap planet server every minute. To initiate the replication system you will need to find out which minutely sequence number you need to start with and update the ```osm_changeset_state``` table so that ChangesetMD knows where to start. Unfortunately there isn't an easy way to get the needed sequence number from the dump file. Here is the process to find it:

First, determine the timestamp present in the first line of XML in the dump file. Assuming you are starting from the .bzip2 file, use this command:

    bunzip2 -c discussions-latest.osm.bz2 | head

Look for this line:

    <osm license="http://opendatacommons.org/licenses/odbl/1-0/" copyright="OpenStreetMap and contributors" version="0.6" generator="planet-dump-ng 1.1.2" attribution="http://www.openstreetmap.org/copyright" timestamp="2015-11-16T01:59:54Z">

Note the timestamp at the end of it. In this case, just before 02:00 on November 16th, 2015. Now browse to [http://planet.osm.org/replication/changesets/](http://planet.osm.org/replication/changesets/) and navigate the directories until you find files with a similar timestamp as the one from the dump file. Each second level directory contains 1,000 diffs so there is generally one directory per day with one day occasionally crossing two directories.

Unfortunately there is no metadata file that goes along with the changeset diff files (like there is with the map data diff files) so there isn't a way to narrow it down to one specific file. However it is safe to apply older diffs to the database since it will just update the data to its current state again. So just go back 2 or 3 hours from the timestamp in the dump file and start there. This will ensure that any time zone setting or daylight savings time will be accounted for. So in the example from above, look for the file with a timestamp around November 15th at 23:00 since that is 3 hours before the given timestamp in the dump file of 02:00 on November 16th.

This gives the file 048.osm.gz in the directory [http://planet.osm.org/replication/changesets/001/582/](http://planet.osm.org/replication/changesets/001/582/). Now take the numbers of all the directories and the file and remove the slashes. So 001/582/048.osm.gz becomes: 1582048. This is the sequence to start replication at. To set this, run the following SQL query in postgres:

    update osm_changeset_state set last_sequence = 1582048;

Now you are ready to start consuming the replication diffs with the following command:

    python changesetmd.py -d <database> -r

Run this command as often as you wish to keep your database up to date with OSM. You can put it in a cron job that runs every minute if you like. The first run may take a few minutes to catch up but each subsequent run should only take a few seconds to finish.

Notes
------------
- Prints a status message every 10,000 records.
- Takes 2-3 hours to import the current dump on a decent home computer.
- Might be faster to process the XML into a flat file and then use the postgres COPY command to do a bulk load but this would make incremental updates a little harder
- I have commonly queried fields indexed. Depending on what you want to do, you may need more indexes.
- Changesets can be huge in extent, so you may wish to filter them by area before any visualization. 225 square km seems to be a fairly decent threshold to get the actual spatial footprint of edits. `WHERE ST_Area(ST_Transform(geom, 3410)) < 225000000` will do the trick.
- Some changesets have bounding latitudes outside the range of [-90;90] range. Make sure you handle them right before projecting (e.g. for area checks).

Table Structure
------------
ChangesetMD populates two tables with the following structure:

osm\_changeset:  
Primary table of all changesets with the following columns:
- `id`: changeset ID
- `created_at/closed_at`: create/closed time 
- `num_changes`: number of objects changed
- `min_lat/max_lat/min_lon/max_lon`: description of the changeset bbox in decimal degrees
- `user_name`: OSM username
- `user_id`: numeric OSM user ID
- `tags`: an hstore column holding all the tags of the changeset
- `geom`: [optional] a postgis geometry column of `Polygon` type (SRID: 4326)

Note that all fields except for id and created\_at can be null.

osm\_changeset\_comment:
All comments made on changesets via the new commenting system
- `comment_changeset_id`: Foreign key to the changeset ID
- `comment_user_id`: numeric OSM user ID
- `comment_user_name`: OSM username
- `comment_date`: timestamp of when the comment was created

If you are unfamiliar with hstore and how to query it, see the [postgres documentation](http://www.postgresql.org/docs/9.2/static/hstore.html)

Example queries
------------
Count how many changesets have a comment tag:

    SELECT COUNT(*)
    FROM osm_changeset
    WHERE tags ? 'comment';

Find all changesets that were created by JOSM:

    SELECT COUNT(*)
    FROM osm_changeset
    WHERE tags -> 'created_by' LIKE 'JOSM%';

Find all changesets that were created in Liberty Island:

    SELECT count(id)
    FROM osm_changeset c, (SELECT ST_SetSRID(ST_MakeEnvelope(-74.0474545,40.6884971,-74.0433990,40.6911817),4326) AS geom) s
    WHERE ST_CoveredBy(c.geom, s.geom);

License
------------
Copyright (C) 2012  Toby Murray

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  

See the GNU Affero General Public License for more details: http://www.gnu.org/licenses/agpl.txt


OSMH
=========

OSMH is an extension for the below repository ChangesetMD to load and scan replications for the historical versions of OSM elements. It uses the same technique and python libraries used by ChangesetMD. OSMH will build the historical OSM elements for specific countires of interests and will maintain the new changes using cron job based on the minute, hour or day changes from the OSM Planet replications.

OSMH requires the same initial setup as ChangesetMD.

Additional dependencies setup:

* `pip install pyyaml`
* `pip install lxml`
* `create extension hstore` against the Postgres database
* `pip install bz2file`

4 GB RAM and 2vCPU cloud instance (AWS c5a.large) is used in practical implementation of running OSMH
## OSMH Setup

OSMH can parse specific country .osm(.bz2) files and inserts OSM elements into a single table called `osm_element_history`. This table would contain all nodes, ways and relations in the .osm file

### Pre-processing the historical file (pbf)

From geofabrik internal server the historical files (.osh.pbf) are available for download. Internal geofabrik server would require log in using OSM authentication. Using `curl` geofabrik (.osh.pbf) can be downloaded by passing the cookie as below example, it would download Kenya historical file 

    curl -b 'gf_download_oauth=REPLACE_WITH_YOUR_COOKIE_VALUE' -O https://osm-internal.download.geofabrik.de/africa/kenya-internal.osh.pbf

Then the country osh.pbf file is passed to [osmimum tool time filter](https://docs.osmcode.org/osmium/latest/osmium-time-filter.html), the time filter can have any start and end date of your interests. In the following example, we using the start date as the date of [first OSM changeset](https://www.openstreetmap.org/api/0.6/changeset/1) and end date as begining of 5 Aug 2021, make sure the the osh.pbf file has the end date when you check it in [geofabrik](https://osm-internal.download.geofabrik.de/africa/kenya.html) where the last modification of the file is declared

It took ~1 hour to run osmium time filter for Kenya as it is .osh.pdf file was around 260 MB at the time of implementation.

For bigger files such as Tanzania .osh.pbf (~ 1 GB), it took ~3 hours.


    osmium time-filter -o kenya-history.osm.bz2 kenya-internal.osh.pbf 2005-04-09T00:00:00Z 2021-08-05T00:00:00Z

osmium output file is an .osm.bz2 [OSM XML standard file](https://wiki.openstreetmap.org/wiki/OSM_XML) that includes historical OSM elements with their references, meaning the nodes are listed first, then ways and each way has a list of refereced nodes then relations with list of referenced memeber. 
When running osmium notice the WARNING message as osmium time filter supports OSM historical files.

![osmium time filter](/resources/osmium-time-filter-example.PNG)

### OSMH Loading Run

When running OSMH to load osm.bz2, OSMH craetes the `osm_element_history` table if not already exists, and creates `osm_element_history_state` which is needed for the replication run. The primary key for `osm_element_history` wil be the OSM element ID combined with the type and version as same OSM element ID will have multiple versions and we noticed that same ID might be for a node/way/relations.
Here is an example for a [node](https://www.openstreetmap.org/api/0.6/node/279096140/history) and a [way](https://www.openstreetmap.org/api/0.6/way/279096140/history) with the same ID # 279096140

OSMH can parse osmium output file using the following command and insert OSM elements into `osm_element_history` table.

    python3 osmh.py -d DB_NAME -u DB_USER -p DB_PASSWORD -H DB_HOST -re Kenya -f ../kenya-history.osm.bz2

Since the table doesn't have any indexes at this stage, parsing is faster than situation where the table has indexes as PG insert command would insert the date and update the index for each batch of insert.

Practically, it took 42 minutes to load Somalia osm.bz2 history file (~ 297MB), 3.5 hours to load Philippines history file (~1.5 GB) - pic below. 

![Philippines history file parsing time](/resources/Philippines-parsing.PNG)

However, it tool ~9.5 hours to load Indonesia history file .osm.bz2 (~4 GB) as shown below 

![Indonesia history file parsing time](/resources/Indonesia-parsing.PNG)
### OSMH Replication Run

The replication run in OSMH is a process of readingthe OSM changes (day,hour or minute) frequancy and continue inserting the new OSM elements history in to the same `osm_element_history`. Same technique as the one used in ChangesetMD, OSMH has a `osm_element_history_state` table which has the latest sequance of the OSM Planet replication.

`osm_element_history_state` will be created when do the OSMH loading run if it is not already exists in the DB.

#### Get Countries Boundaries Before OSMH Replication Run

Before OSMH Replication Run, you need to setup the countries boundaries table. OSMH has an option to load the geofabrik .poly files and constructs the boundaries into (MULTI)POLYGON and insert them into the `boundaries` table. OSMH would craete the `boundaries` table if it is not already exists. It is required to load your country of interests before you run the OSMH replication run as the replication run gets the OSM element country based on the `boundaries` table.

For example, here is the command to load Afghanistan boundaries.

Under /resources/boundaries.txt, you can find a list of all commands for all countries defined in geofabrik.

    python3 osmh.py  -d DB_NAME -u DB_USER -p DB_PASSWORD -P DB_PORT -H DB_HOST -b http://download.geofabrik.de/asia/afghanistan.poly

#### Setup Last Sequence Manually
The `last_sequance` needs to match the `-freq` and updated in the database before running the OSMH replication. The `last_sequance` value can be learned from the OSM Planet replication for the start date/time where you need the replication to start. For example, in our practical run we used _5 Aug 2021_ as the end date in the osmium time filter so as showb below, the best minutely last sequance would be 004/657/384 so the value of 4657384 can be updated in the `osm_element_history_state` table.

![OSMH flow chart](/resources/last-sequance-example.PNG)

    python3 osmh.py -d DB_NAME -u DB_USER -p DB_PASSWORD -H DB_HOST -r -freq minute

OSM replication run would grab all OSM change files starting from `osm_element_history_state` table `last_sequance` till the last sequance in OSM Planet replication. Here is where the last OSM Plant minute replication can be found (https://planet.openstreetmap.org/replication/minute/state.txt)


After the OSMH replication run finishes, it can be scheduled as cron job to update the OSM element history up to the minutly frequancy.

It is recommended to run the replocations on the hour frequancy to catch up from  the loading date (_5 Aug 2021_ in our practical exmaples) to the last hour. Then run the replications on the minute frequancy to catch up to last minute. Additionally, the hour frequancy replication file use acceptable amound of memory. Practically, it used ~30% of the 8 GB RAM device.

OSM replication run would get all countries OSM eleemnts history as of the start sequance so you wil end up with OSM elements from countries that you didn't load their history into the `osm_element_history` table yet. You can maintain them in the table and when OSMH loading run again for a new country of interest you would need to filter the osh.pbf using osmium time filter till your specific date. Like in our practical implementation, it is _5 Aug 2021_

It took 3 minutes and 11 seconds to load the replications for 1 hour of OSM Planet minutes repliation. Log file shows the start and end sequances example in the resources folder.
### OSMH Data Flow

Loading multiple countries historical OSM elements would need going through Pre-processing the historical file for the new country and OSHM run steps again.

![OSMH flow chart](/resources/flowchart.PNG)


### TODO: find country for later added OSM country history

TDC...
### Indexes Creation

After loading you country of interest, you can build DB indexes on the `osm_element_history` table to support the types of queries you are interested to run. Postgres supports building indexes concurrently to avoid locking the table so OSMG replication run can continue while indexes are being created.

    CREATE INDEX CONCURRENTLY osm_element_history_timestamp_idx ON public.osm_element_history ("timestamp");
    CREATE INDEX CONCURRENTLY osm_element_history_country_idx ON public.osm_element_history (country);
    CREATE INDEX CONCURRENTLY osm_element_history_changeset_idx ON public.osm_element_history (changeset);
    CREATE INDEX CONCURRENTLY osm_element_history_action_idx ON public.osm_element_history ("action");
    CREATE INDEX CONCURRENTLY osm_element_history_tags_idx ON public.osm_element_history USING GIST (tags);
    CREATE INDEX CONCURRENTLY osm_element_history_geom_gist__idx ON public.osm_element_history using GIST (geom);


GIST index creation might take long time based on the  `osm_element_history` size. Practically, it took around +36 hours when the `osm_element_history` table size was 200 GB.
Bear in mind that after creating the indexes, you can run OSMH loading but it would take longer time as postgres would need to updae the indexes after inserting new OSM Elemnts. Practically, the time needed to load an osm.bz2 file has increased 5 times

## TODO: Sample Queries on the OSM Element History

TBC...
