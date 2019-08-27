class SqlQueries:
songplays_table_insert = ("""
INSERT INTO songplays 
(
playid,
start_time,
userid,
"level",
songid,
artistid,
sessionid,
location,
user_agent,
)    
SELECT
md5(events.sessionid || events.start_time) songplay_id,
events.start_time, 
events.userid, 
events.level, 
songs.song_id, 
songs.artist_id, 
events.sessionid, 
events.location, 
events.useragent
FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
FROM staging_events
WHERE page='NextSong') events
LEFT JOIN staging_songs songs
ON events.song = songs.title
AND events.artist = songs.artist_name
AND events.length = songs.duration
"""
)

users_table_insert = (
"""
INSERT INTO users 
(
user_id
,first_name
,last_name
,gender
,level
) 
SELECT distinct 
user_id
,first_name
,last_name
,gender
,level
FROM staging_events
WHERE page='NextSong'
"""
)

songs_table_insert = (
"""
INSERT INTO songs 
(
song_id
,title
,artist_id
,"year"
,duration
) 
SELECT distinct 
song_id
,title
,artist_id
,"year"
,duration
FROM staging_songs
"""
)

artists_table_insert = (
"""
INSERT INTO artists 
(
artist_id
,artist_name
,artist_location
,artist_lattitude
,artist_longitude
) 
SELECT distinct 
artist_id
,artist_name
,artist_location
,artist_latitude
,artist_longitude
FROM staging_songs
"""
)

times_table_insert = (
"""
INSERT INTO time 
(
start_time, 
hour, 
day, 
week, 
month, 
year, 
weekday
) 
SELECT 
start_time
,extract(hour from start_time)
,extract(day from start_time)
,extract(week from start_time)
,extract(month from start_time)
,extract(year from start_time)
,extract(dayofweek from start_time)
FROM songplays
"""
)

##################################
## Create Table Helpers
#################################

songplays_table_create = ("""
DROP TABLE IF EXISTS songplays

CREATE TABLE IF NOT EXISTS songplays
(
song_id varchar(256) NOT NULL,
title varchar(256),
artist_id varchar(256),
"year" int4,
duration numeric(18,0),
CONSTRAINT songs_pkey PRIMARY KEY (song_id)
)
"""
)

users_table_create = (
"""
DROP TABLE IF EXISTS users

CREATE TABLE IF NOT EXISTS users 
(
user_id int4 NOT NULL,
first_name varchar(256),
last_name varchar(256),
gender varchar(256),
"level" varchar(256),
CONSTRAINT users_pkey PRIMARY KEY (user_id)
)
"""
)

songs_table_create = (
"""
DROP TABLE IF EXISTS songs

CREATE TABLE IF NOT EXISTS songs (
	song_id varchar(256) NOT NULL,
	title varchar(256),
	artist_id varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (song_id)
)
"""
)

artists_table_create = (
"""
DROP TABLE IF EXISTS artists

CREATE TABLE IF NOT EXISTS artists (
	artist_id varchar(256) NOT NULL,
	artist_name varchar(256),
	artist_location varchar(256),
	artist_lattitude numeric(18,0),
	artist_longitude numeric(18,0)
)
)
"""
)

times_table_create = (
"""
DROP TABLE IF EXISTS times

CREATE TABLE IF NOT EXISTS times (
start_time varchar NOT NULL distkey, 
hour int, 
day int, 
week int, 
month int,
year int, 
weekday int
)
"""
)

staging_events_table_create = (
"""    
DROP TABLE IF EXISTS staging_events

CREATE TABLE IF NOT EXISTS staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);
"""
)

staging_songs_table_create = (
"""    
DROP TABLE IF EXISTS staging_songs

CREATE TABLE IF NOT EXISTS staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);
"""
)