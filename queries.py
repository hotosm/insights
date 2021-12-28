'''
Just a utility file to store some SQL queries for easy reference

@author: Toby Murray
'''
createChangesetTable = '''CREATE EXTENSION IF NOT EXISTS hstore;
  CREATE TABLE osm_changeset (
  id bigint,
  user_id bigint,
  created_at timestamp without time zone,
  min_lat numeric(10,7),
  max_lat numeric(10,7),
  min_lon numeric(10,7),
  max_lon numeric(10,7),
  closed_at timestamp without time zone,
  open boolean,
  num_changes integer,
  user_name varchar(255),
  tags hstore
);
CREATE TABLE osm_changeset_comment (
  comment_changeset_id bigint not null,
  comment_user_id bigint not null,
  comment_user_name varchar(255) not null,
  comment_date timestamp without time zone not null,
  comment_text text not null
);
CREATE TABLE osm_changeset_state (
  last_sequence bigint,
  last_timestamp timestamp without time zone,
  update_in_progress smallint
);
'''
createOsmHistoryTable = '''
CREATE TABLE if not EXISTS public.osm_element_history (
	id int8 NULL,
	"type" varchar NULL,
	tags hstore NULL,
	lat numeric(9, 7) NULL,
	lon numeric(10, 7) NULL,
	nds _int8 NULL,
	members text[][] NULL,
	changeset int8 NULL,
	"timestamp" timestamp NULL,
	uid int8 NULL,
	"version" int8 NULL,
	"action" varchar NULL,
	country varchar NULL,
	geom geometry NULL,
	CONSTRAINT osm_element_history_un UNIQUE (id, version,"type")
);

CREATE TABLE if not EXISTS public.osm_element_history_state (
	last_sequence int8 NULL,
	last_timestamp timestamp NULL,
	update_in_progress int2 NULL
);

'''

initStateTable = '''INSERT INTO osm_changeset_state VALUES (-1, null, 0)''';

dropIndexes = '''ALTER TABLE osm_changeset DROP CONSTRAINT IF EXISTS osm_changeset_pkey CASCADE;
DROP INDEX IF EXISTS user_name_idx, user_id_idx, created_idx, tags_idx, changeset_geom_gist ;
'''

createConstraints = '''ALTER TABLE osm_changeset ADD CONSTRAINT osm_changeset_pkey PRIMARY KEY(id);'''

createIndexes = '''CREATE INDEX user_name_idx ON osm_changeset(user_name);
CREATE INDEX user_id_idx ON osm_changeset(user_id);
CREATE INDEX created_idx ON osm_changeset(created_at);
CREATE INDEX tags_idx ON osm_changeset USING GIN(tags);
'''

createGeometryColumn = '''
CREATE EXTENSION IF NOT EXISTS postgis;
SELECT AddGeometryColumn('osm_changeset','geom', 4326, 'POLYGON', 2);
'''

createGeomIndex = '''
CREATE INDEX changeset_geom_gist ON osm_changeset USING GIST(geom);
'''

createBoundaries = '''
CREATE TABLE if not exists public.boundaries (
	name_en varchar NULL,
	admin_level int4 NULL,
	tags hstore NULL,
	boundary geometry NULL,
	priority bool NULL,
	loaded bool NULL
);
CREATE UNIQUE INDEX if not exists boundaries_nameen_idx ON public.boundaries USING btree (name_en);
'''

createHashtagsTables = '''
    
CREATE TABLE  if not exists  public.hashtag (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	"name" varchar NOT NULL,
	added_by int4 NULL,
	created_at timestamp NOT NULL DEFAULT now(),
	is_tm_project bool NULL,
	first_used date NULL,
	last_used date NULL,
	CONSTRAINT hashtag_pk PRIMARY KEY (id)
);
CREATE UNIQUE INDEX if not exists  hashtag_name_idx ON public.hashtag USING btree (name);

CREATE TABLE  if not exists public.hashtag_stats (
	hashtag_id int4 NOT NULL,
	"type" varchar(1) NOT NULL,
	start_date timestamp NOT NULL,
	end_date timestamp NOT NULL,
	total_new_buildings int4 NOT NULL,
	total_uq_contributors int4 NOT NULL,
	total_new_road_km int4 NULL,
	calc_date timestamp NOT NULL DEFAULT now(),
	CONSTRAINT hashtag_stats_fk FOREIGN KEY (hashtag_id) REFERENCES public.hashtag(id)
);
CREATE UNIQUE INDEX if not exists hashtag_stats_hashtag_id_idx ON public.hashtag_stats USING btree (hashtag_id, type, start_date, end_date);

'''
createAllChangesetsStatsTable = '''
    
CREATE TABLE if not exists public.all_changesets_stats (
	changeset int8 NULL,
	added_buildings int8 NULL,
	modified_buildings int8 NULL,
	added_amenity int8 NULL,
	modified_amenity int8 NULL,
	added_highway int8 NULL,
	modified_highway int8 NULL,
	added_highway_meters float8 NULL,
	modified_highway_meters float8 NULL
);
CREATE UNIQUE INDEX if not exists all_changesets_stats_changeset_idx ON public.all_changesets_stats USING btree (changeset);

'''
