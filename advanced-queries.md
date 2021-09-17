# Advanced Queries

In this section of the documentation, the focus is to present osm element's wrangling and queries to produce multiple insights:


## Live Events

Live events are one of the activities OSM community are contributing to, they include mapathons, map and chat events and any other event that gathers mappers to contribute through Tasking Manager. Some time a specific hash tag is used in these events and it is added in the changeset comments or in the hashtags field of a changeset.

Live events insights can be queries from the `osm_element_history` table directly as they are focused on short period of time so the postgres index on the timestamp column would make the query fast. HOT TM projects normally use hashtags as "hotosm-project-PROJECT_ID" to be added in the changeset comments or hashtags field.

### Total Number of Created or Modified Features

The following query would return the total number of created and modified features during the live event period for specific HOT TM project(s) such as 11224 and 10042 or specific hashtag such as mapandchathour2021

    select t.key,t.action, count(distinct id)
    from (select (each(osh.tags)).key, (each(osh.tags)).value,osh.*
    from public.osm_element_history osh
    where osh.changeset in (select c.id 
                            from public.osm_changeset c
                            where c.created_at  between '2021-08-27 09:00:00' and '2021-08-27 11:00:00'
                            and (
                            (c.tags -> 'comment') ~~    '%hotosm-project-11224%' or (c.tags -> 'hashtags') ~~ '%hotosm-project-11224%' 
                            or (c.tags -> 'comment') ~~ '%hotosm-project-10042%' or (c.tags -> 'hashtags') ~~ '%hotosm-project-10042%'
                            or (c.tags -> 'comment') ~~ '%mapandchathour2021%' or (c.tags -> 'hashtags') ~~ '%mapandchathour2021%'
                            )												
                            )
                            ) as t
    group by t.key,t.action
    order by 3 desc
				
### Live Events Contributors
The following query would return the total number of contributors who have submited at least 1 changeset during the event for specific projects or hastags

    select count(distinct uid) Total_contributers
    from (select (each(osh.tags)).key, (each(osh.tags)).value,osh.*
    from public.osm_element_history osh
    where osh.changeset in (select c.id 
                            from public.osm_changeset c
                            where c.created_at between '2021-08-27 09:00:00' and '2021-08-27 11:00:00'
                            and (
                            (c.tags -> 'comment') ~~ '%hotosm-project-11224%' or (c.tags -> 'hashtags') ~~ '%hotosm-project-11224%' 
                            or (c.tags -> 'comment') ~~ '%hotosm-project-10042%' or (c.tags -> 'hashtags') ~~ '%hotosm-project-10042%'
                            or (c.tags -> 'comment') ~~ '%mapandchathour2021%' or (c.tags -> 'hashtags') ~~ '%mapandchathour2021%'
                            )												
                            )
                            ) as t


## Country Insights

TBC..

### OSM Building and HOT TM Mapped Building Counts


### Country Validated Building 

TBC..
#### Updating OSM element history 

In the replication run mentioned above, the nodes inserted by relication run process will have their country and geom (point) inserted directly as country will be matched with bounday table and geom is a point geometry based on lat and lon for each node.

For ways and relations, there will be some that are out of the priority countries as relication run grabs all new relication on the frequancy passed for the script. Additionally, ways' and relations' lat,lon and geom are not accurate. Therefore, the following sql update statements can be used to udpate the lat,lon and geom for ways and relations that is laoded as of the replication start date (5 Aug 2021), deleted ways and relations are excluded in the update command:
For ways, the lat, lon and geom (point) of the first node that is contained in the nds field isused.

    update public.osm_element_history osh
    set country = (select n.country
					from public.osm_element_history n
					where n."type" = 'node' 
			        and n.id = osh.nds[1]
			        and "version" = (select max(i."version") from  public.osm_element_history i where i."type"  = osh."type"  and i.id = osh.id)
			        ),
	lat =  (select n.lat 
					from public.osm_element_history n
					where n."type" = 'node' 
			        and n.id = osh.nds[1] 
			        and "version" = (select max(i."version") from  public.osm_element_history i where i."type"  = osh."type"  and i.id = osh.id)
			        ),
	lon  =  (select n.lon 
					from public.osm_element_history n
					where n."type" = 'node' 
			        and n.id = osh.nds[1]
			        and "version" = (select max(i."version") from  public.osm_element_history i where i."type"  = osh."type"  and i.id = osh.id)
			        ),
	geom = ST_GeomFromText('POINT('||  (select n.lon 
					from public.osm_element_history n
					where n."type" = 'node' 
			        and n.id = osh.nds[1]
			        and "version" = (select max(i."version") from  public.osm_element_history i where i."type"  = osh."type"  and i.id = osh.id)
			        ) ||' '||  (select n.lat 
					from public.osm_element_history n
					where n."type" = 'node' 
			        and n.id = osh.nds[1] 
			        and "version" = (select max(i."version") from  public.osm_element_history i where i."type"  = osh."type"  and i.id = osh.id)
			        ) || ')',4326) 	        
    where "type" = 'way'
    and "action" != 'delete'
    and "timestamp"  >= '2021-08-05';

For relations, the lat, lon and geom (point) of the first member (way or node) that is contained in the members field isused.

    update public.osm_element_history osh
    set country = (select n.country
					from public.osm_element_history n
					where n."type" = osh.members[1][2]
			        and n.id = osh.members[1][1]::int8
			        and "version" = (select max(i."version") from  public.osm_element_history i where i."type"  = osh."type"  and i.id = osh.id)
			        ),
	lat =  (select n.lat 
					from public.osm_element_history n
					where n."type" = osh.members[1][2]
			        and n.id = osh.members[1][1]::int8
			        and "version" = (select max(i."version") from  public.osm_element_history i where i."type"  = osh."type"  and i.id = osh.id)
			        ),
	lon  =  (select n.lon 
					from public.osm_element_history n
					where n."type" = osh.members[1][2]
			        and n.id = osh.members[1][1]::int8
			        and "version" = (select max(i."version") from  public.osm_element_history i where i."type"  = osh."type"  and i.id = osh.id)
			        ),
	geom = (select n.geom 
					from public.osm_element_history n
					where n."type" = osh.members[1][2]
			        and n.id = osh.members[1][1]::int8
			        and "version" = (select max(i."version") from  public.osm_element_history i where i."type"  = osh."type"  and i.id = osh.id)
			        )   
    where "type" = 'relation'
    and "action" != 'delete'
    and "timestamp"  >= '2021-08-05';
