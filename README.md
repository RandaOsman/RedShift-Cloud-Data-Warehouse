 music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3 so we need to allocate these data to staging tables to be source table for their dimensional model then reposrts and dashboards can be created easily and improve their services by analyzing users behaviour and intersets also they can predict the upcoming trends 
 
 
Schema:
1. staging tables: 

log data and song info that belongs to sparkfy are stored on S3 so we need to copy them into intermediary layer 

staging_events and stagin_songs ; then they will feed the fact and dimensions tables in DWH model

2. DWH model :
star schema has been created it includes the following tables.

Fact Table

    songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)

Dimension Tables

    users (user_id, first_name, last_name, gender, level)

    songs (song_id, title, artist_id, year, duration)

    artists (artist_id, name, location, latitude, longitude)

    time - timestamps of records in songplays broken down into specific units

    (start_time, hour, day, week, month, year, weekday)

steps:

1. runt the create_tables.py 

to create fact and dimension tables for the star schema in Redshift.


2. prepare the sql_queries.py 

to include the
> drop tables if exists before (after checking the stability for the model and no changes in the data types and so on , no need to drop)
> create statment for staging tables , fact and dimension also 
> copy staments from S3 repository on cloud for JSON files into staging tables then use this these tables to be used in 
> insert staments that will feed fact table by joins the necessary tables to get the fact data and dimension tables with appropriate casing for some    columns like the start_time in TIME table , trying to apply logic on user table to gurantee the record uniquness by catch the max time stamo which    means the latest transaction by user 


4. runt ETL.py to call the COPY & INSERT functions that responsible about load into staging and insert into DWH tables 


5. select sample of data from each tables to check the major constrains like data integrity and nullablity also recogize the response time and data retrival 

