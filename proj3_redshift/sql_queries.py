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
    event_id IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR(256),
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
    song VARCHAR(256),
    status INTEGER,
    ts BIGINT NOT NULL,
    userAgent VARCHAR(512),
    userId INTEGER NOT NULL  
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR(64) NOT NULL,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(512),
    artist_name VARCHAR(512),
    song_id VARCHAR(64) NOT NULL PRIMARY KEY,
    title VARCHAR(512),
    duration DECIMAL(10),
    year SMALLINT
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id IDENTITY(0,1) PRIMARY KEY, 
    start_time BIGINT, 
    user_id INT NOT NULL, 
    level VARCHAR(64), 
    song_id VARCHAR(64) NOT NULL, 
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
    );  
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(64) PRIMARY KEY,
    title VARCHAR(512),
    artist_id VARCHAR(64) NOT NULL,
    year SMALLINT, 
    duration FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(64) PRIMARY KEY,
    name  VARCHAR(512),
    location VARCHAR(256),
    lattitude FLOAT,
    longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time BIGINT PRIMARY KEY,
    hour SMALLINT, 
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year SMALLINT,
    weekday VARCHAR(16)
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

""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
           date(payment_date)                                           AS date,
           EXTRACT(year FROM payment_date)                              AS year,
           EXTRACT(quarter FROM payment_date)                           AS quarter,
           EXTRACT(month FROM payment_date)                             AS month,
           EXTRACT(day FROM payment_date)                               AS day,
           EXTRACT(week FROM payment_date)                              AS week,
           CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END AS is_weekend
    FROM staging_events AS se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
