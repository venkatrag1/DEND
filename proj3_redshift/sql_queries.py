import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    event_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR(256) DISTKEY,
    auth VARCHAR(64),
    firstName VARCHAR(64),
    gender VARCHAR(2),
    itemInSession INTEGER,
    lastName VARCHAR(64),
    length DECIMAL(10),
    level VARCHAR(64),
    location VARCHAR(256),
    method VARCHAR(16),
    page VARCHAR(32),
    registration REAL,
    sessionId INTEGER NOT NULL,
    song VARCHAR(256) SORTKEY,
    status INTEGER,
    ts BIGINT NOT NULL,
    userAgent VARCHAR(512),
    userId INTEGER  
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR(64) NOT NULL,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(512),
    artist_name VARCHAR(512) DISTKEY,
    song_id VARCHAR(64) NOT NULL PRIMARY KEY,
    title VARCHAR(512) SORTKEY,
    duration DECIMAL(10),
    year SMALLINT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL SORTKEY, 
    user_id INT NOT NULL, 
    level VARCHAR(64), 
    song_id VARCHAR(64) NOT NULL DISTKEY, 
    artist_id VARCHAR(64) NOT NULL, 
    session_id INT NOT NULL, 
    location VARCHAR(256), 
    user_agent VARCHAR(512)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY, 
    first_name VARCHAR(64), 
    last_name VARCHAR(64),
    gender VARCHAR(2),
    level VARCHAR(64)
    )
    DISTSTYLE ALL;  
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(64) PRIMARY KEY DISTKEY,
    title VARCHAR(512),
    artist_id VARCHAR(64) NOT NULL,
    year SMALLINT, 
    duration FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(64) PRIMARY KEY,
    name  VARCHAR(512) SORTKEY,
    location VARCHAR(256),
    lattitude FLOAT,
    longitude FLOAT
    )
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
    hour SMALLINT, 
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday SMALLINT
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {table} 
    CREDENTIALS 'aws_iam_role={dwh_role_arn}'
    FORMAT as JSON {events_json_format}
    REGION 'us-west-2';
""").format(
    table=config.get('S3','LOG_DATA'),
    dwh_role_arn=config.get('IAM_ROLE', 'ARN'),
    events_json_format=config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    COPY staging_songs from {table} 
    CREDENTIALS 'aws_iam_role={dwh_role_arn}'
    FORMAT as JSON 'auto' 
    REGION 'us-west-2';
""").format(
    table=config.get('S3', 'SONG_DATA'),
    dwh_role_arn=config.get('IAM_ROLE', 'ARN'),
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
    TIMESTAMP 'epoch' + s_evts.ts /1000 * INTERVAL '1 second' AS start_time,
    s_evts.userId AS user_id,
    s_evts.level AS level,
    s_songs.song_id AS song_id,
    s_songs.artist_id AS artist_id,
    s_evts.sessionId AS session_id,
    s_evts.location AS location,
    s_evts.userAgent as user_agent
    FROM staging_events AS s_evts
    JOIN staging_songs AS s_songs
    ON (s_evts.artist = s_songs.artist_name)
    WHERE s_evts.page = 'NextSong'
    AND s_evts.song = s_songs.title
    AND s_evts.length = s_songs.duration;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
    s_evts.userId AS user_id,
    s_evts.firstName AS first_name,
    s_evts.lastName AS last_name,
    s_evts.gender AS gender,
    s_evts.level AS level
    FROM staging_events AS s_evts
    WHERE s_evts.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
    s_songs.song_id AS song_id,
    s_songs.title AS title,
    s_songs.artist_id AS artist_id,
    s_songs.year AS year,
    s_songs.duration AS duration
    FROM staging_songs AS s_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT 
    s_songs.artist_id AS artist_id,
    s_songs.artist_name AS name,
    s_songs.artist_location AS location,
    s_songs.artist_latitude AS latitude,
    s_songs.artist_longitude AS longitude
    FROM staging_songs as s_songs;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
    TIMESTAMP 'epoch' + s_evts.ts /1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour from start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(dow FROM start_time) AS weekday
    FROM staging_events AS s_evts
    WHERE s_evts.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
