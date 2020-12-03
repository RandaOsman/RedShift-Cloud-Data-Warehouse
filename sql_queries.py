import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


"""
drop queries to drop the DWH tables if exists before (after checking the stability for the model and no changes in the data types we can dim this section)
"""

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists  users"
song_table_drop = "drop table  if exists songs"
artist_table_drop = "drop table if exists  artists"
time_table_drop = "drop table if exists  time"

"""
call the create query statments for staging stable that will hold the data that will be copied from S3 into it 
"""



staging_events_table_create= ("""
create table "staging_events"
(
event_id    int IDENTITY(0,1)  ,
artist text,
auth text,
firstName text,
gender text,
itemInSession text,
lastName text,
length text,
level text,
location text,
method text,
page text,
registration text,
sessionId int,
song  text,
status text,
ts text,
useragent varchar(max),
userId text
)
""")

staging_songs_table_create = ("""
create table "staging_songs"

(
num_songs numeric,
artist_id character varying(50),
artist_latitude float, 
artist_longitude float,
artist_location varchar(max),
artist_name varchar(max), 
song_id character varying(50),
title varchar(max),
duration float,
year int
)
""")


"""
call the create query statments for fact and dim tables
"""


songplay_table_create = ("""
create table "songplays"(
songplay_id int IDENTITY(0,1) primary key sortkey distkey,
start_time text NOT NULL,
user_id varchar NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar
)
""")

user_table_create = ("""
create table "users"(
user_id varchar primary key sortkey,
first_name varchar,
last_name varchar,
gender char(1),
level varchar
)
diststyle auto;
""")

song_table_create = ("""
create table "songs" (
song_id varchar primary key sortkey,
title varchar,
artist_id varchar, 
year int,
duration numeric
)
diststyle auto;
""")

artist_table_create = ("""
create table "artists" (
artist_id varchar primary key sortkey,
artist_name varchar,
artist_location varchar,
artist_latitude numeric,
artist_longitude numeric
)
diststyle auto;
""")

time_table_create = ("""
create table "time" (
start_time timestamp  primary key sortkey,
hour int,
day int,
week int,
month int,
year int,
weekday int
)
diststyle auto;
""")


"""
copy the data from S3 repository into staging tables that will feed the DWH tables
"""


staging_events_copy = (
    """
    COPY staging_events
    FROM {} 
    iam_role '{}'
    JSON {}
    REGION 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))




staging_songs_copy = (
    """
    COPY staging_songs
    FROM {} 
    iam_role '{}'
    JSON 'auto'
    REGION 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))





"""
insert query statments to populate the fact and dim tables from the staging tables 

"""


user_table_insert = ("""
insert into users ( user_id ,first_name , last_name , gender , level ) 
select distinct userId, firstName , lastName , gender,level 

FROM staging_events m

WHERE userId IS NOT null AND ts = (select max(ts) FROM staging_events s WHERE s.userId = m.userId) and m.page='NextSong'

ORDER BY userId DESC

""")

song_table_insert = ("""insert into songs
(song_id,title,artist_id,year,duration )
select distinct song_id , title , artist_id , year , duration

from staging_songs
""")


artist_table_insert = ("""insert into artists 
(artist_id,artist_name, artist_location, artist_latitude, artist_longitude )

select distinct artist_id , artist_name ,artist_location , artist_latitude , artist_longitude

from staging_songs
""")

songplay_table_insert = (""" insert into songplays 
(start_time , user_id , level , song_id , artist_id , session_id , location , user_agent ) 

select distinct e.ts, e.userId , e.level,s.song_id  ,s.artist_id ,e.sessionId,e.location ,e.useragent

from staging_events e join staging_songs s

on e.song = s.title
and e.artist =s.artist_name
and e.length=s.duration

where e.page='NextSong'

""")

time_table_insert = ("""insert into time 
(start_time, hour, day, week, month, year, weekday)
SELECT start_time, 
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(dayofweek from start_time)
FROM  
        (SELECT distinct TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM songplays)
    WHERE start_time is not NULL;

""")




"""
create list for copy intoo staging tables , create , drop and insert into DWH tables to be used in the ETL.py
"""
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
