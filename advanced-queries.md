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

The country level insight have multiple sub-levels:
- First one on the monthy basis calulations and those and be calculated periodacily and stored as they never change such as # of buildings on specific time stamp and # of buidlings coming to OSM through Tasking Manager.

Those insights can be stored in `country_insights` table as the following:

    CREATE TABLE public.country_insights (
        country varchar NOT NULL,
        by_month timestamp NOT NULL,
        building_count int4 NULL,
        tm_building_count int4 NULL,
        CONSTRAINT country_insights_pk PRIMARY KEY (country, by_month)
    );

Here is an example of the country insight table data. Below are more details about how calculations can be done
![Example of country insight per month](/resources/country-insights-monthly-sample.PNG)

- Second one is on the country level regardless of time. Such as total number of validated buildings. Below are more details about how calculations can be done. Although those would need to be re-calculated as more TM Tasks are being validated.

Those insights can be stored in `country_insights2` table as the following:
    CREATE TABLE public.country_insights2 (
        country varchar NOT NULL,
        validated_buildings int4 NULL
    );


### OSM Building and HOT TM Mapped Building Counts
Calculating the total number of buildings in specific country on a specific time stamp can be calculated and stored using the following update command. Make sure you have the country of interest and months (practically, we are using the end of each month) are already inserted into the `country_insights` table.

For `building_count`: It is the total count of:
- ways and relations (no nodes) (`"type" in ('way', 'relation')`)
- and has the 'building' tag (`tags ? 'building'`)
- in its latest version where the latest version is before the specific date (`"version" = (select max(i."version") from  public.osm_element_history i where i."type"  = internal."type"  and i.id = internal.id and i."timestamp"  <  osh.by_month)`)
- and in specific country (`country = osh.country `)

For `tm_building_count`: it is similar to `building_count` with one more condition:
- the osm element changeset id is coming from hot_changeset (`changeset in (select id from public.hot_changeset)`)

Where `hot_changeset` is a materialized view for all changesets that has "hotosm" in its hashtags field or in the comment section. The create command is listed below

    update public.country_insights osh
    set building_count = (select count(distinct internal.id ) 
            from public.osm_element_history internal 
            where internal.country = osh.country 
            and internal.tags ? 'building'
            and internal."type" in ('way', 'relation')
            and internal."version" = (select max(i."version") from  public.osm_element_history i where i."type"  = internal."type"  and i.id = internal.id and i."timestamp"  <  osh.by_month)
            ),
    tm_building_count = (select count(distinct internal.id ) 
            from public.osm_element_history internal 
            where internal.country = osh.country 
            and internal.tags ? 'building'
            and internal."type" in ('way', 'relation')
            and internal."version" = (select max(i."version") from  public.osm_element_history i where i."type"  = internal."type"  and i.id = internal.id and i."timestamp"  <  osh.by_month)
            and internal.changeset in (select id from public.hot_changeset)
            )
    where by_month < now() 
    and country = 'Tanzania'
    and EXTRACT('year' from by_month) in (2021);

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

## `hot_changeset` Materialized View Command
Including the indexes created on the matterialized view

    create materialized view public.hot_changeset as
    select  id, 
            user_id, 
            created_at, 
            min_lat, 
            max_lat, 
            min_lon, 
            max_lon, 
            closed_at, 
            "open", 
            num_changes, 
            user_name, 
            tags, 
            c.geom ,
            (select g.name_en 
            from public.boundaries g 
            where ST_INTERSECTS( st_setsrid( g.boundary,4326) , ST_Centroid(geom))  
            limit 1) country
    from public.osm_changeset c
    where (c.tags ? 'comment' and (c.tags -> 'comment') like '%hotosm%')
    or ((c.tags -> 'hashtags')  like '%hotosm%');

    CREATE INDEX hot_changeset_country_idx ON public.hot_changeset USING btree (country);
    CREATE INDEX hot_changeset_geom_idx ON public.hot_changeset USING gist (geom);
    CREATE INDEX hot_changeset_id_idx ON public.hot_changeset USING btree (id);
    CREATE UNIQUE INDEX hot_changeset_id_uq_idx ON public.hot_changeset USING btree (id);
    CREATE INDEX hot_changeset_tags_idx ON public.hot_changeset USING btree (tags);