import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
users_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# Creating staging table for data from events log files

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    event_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    artist_name VARCHAR(MAX),
    auth VARCHAR(MAX),
    user_first_name VARCHAR(MAX),
    user_gender  VARCHAR(MAX),
    item_in_session	INTEGER,
    user_last_name VARCHAR(255),
    song_length	DOUBLE PRECISION, 
    user_level VARCHAR(255),
    location VARCHAR(255),	
    method VARCHAR(255),
    page VARCHAR(255),	
    registration VARCHAR(50),	
    session_id	BIGINT,
    song_title VARCHAR(MAX),
    status INTEGER,
    ts VARCHAR(50),
    user_agent TEXT,	
    user_id VARCHAR(50)
    );
""")
# Creating staging table for data from song plays log files

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR(MAX) PRIMARY KEY,
    num_songs INTEGER,
    artist_id VARCHAR(MAX),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(MAX),
    artist_name VARCHAR(MAX),
    title VARCHAR(MAX),
    duration DOUBLE PRECISION,
    year INTEGER
);
""")

#------------------------------------------------------------------------
# Creating facts table for our star schema

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id VARCHAR(50),
    level VARCHAR(MAX),
    song_id VARCHAR(MAX),
    artist_id VARCHAR(MAX),
    session_id BIGINT,
    location VARCHAR(MAX),
    user_agent TEXT
);
""")
# Creating dim tables for our star schema (users, song, artist, time)

users_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id VARCHAR(100) PRIMARY KEY, 
    first_name VARCHAR(MAX), 
    last_name VARCHAR(MAX), 
    gender VARCHAR(MAX), 
    level VARCHAR 
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song( 
    song_id VARCHAR(MAX) PRIMARY KEY, 
    title VARCHAR(MAX), 
    artist_id VARCHAR(MAX), 
    year INT, 
    duration DOUBLE PRECISION
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
    artist_id VARCHAR(MAX) PRIMARY KEY, 
    name VARCHAR(255), 
    location VARCHAR(255), 
    lattitude FLOAT, 
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER, 
    weekday INTEGER
);
""")

# STAGING TABLES

# Copying data from JSON to Redshift DB (read creadentials from config file dwh.cfg)

staging_events_copy = ("""
    copy staging_events from '{}' 
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON '{}';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

# Copying data from JSON to Redshift DB (read creadentials from config file dwh.cfg)
staging_songs_copy = ("""
    copy staging_songs from '{}' 
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON 'auto'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

# Populating data from staging to FACT and DIM tables 

songplay_table_insert = ("""
INSERT INTO songplay(
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    )
SELECT 
    TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second',
    e.user_id, 
    e.user_level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
FROM 
    staging_events AS e, 
    staging_songs AS s
WHERE 
    e.page = "NextSong"
AND 
    e.song_title = s.title
AND
    e.artist_name = s.artist_name
""")

users_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
    userId,
    user_first_name, 
    user_last_name, 
    gender, 
    level
FROM   
    staging_events
WHERE   
    page = 'NextSong' 
AND
    user_id IS NOT NULL

""")

song_table_insert = ("""
INSERT INTO song(song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artist(artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time,       
        DATE_PART(hour, start_time) AS hour,
        EXTRACT(day, start_time) AS day,
        EXTRACT(week, start_time) AS week,
        EXTRACT(month, start_time) AS month,
        EXTRACT(year, start_time) AS year, 
        EXTRACT(weekday, start_time) AS weekday 
    FROM songlpay
""")

# QUERY LISTS
# To be called from create_tables.py 
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, users_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, users_table_drop, song_table_drop, artist_table_drop, time_table_drop]

# To be called from etl.py 
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, users_table_insert, song_table_insert, artist_table_insert, time_table_insert]
