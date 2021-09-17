### Updating OSM element history 

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
