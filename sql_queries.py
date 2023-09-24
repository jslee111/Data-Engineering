import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_song_play"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
    user_id INTEGER,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR,
    artist VARCHAR,
    song VARCHAR,
    length FLOAT,
    auth VARCHAR,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    session_id INTEGER,
    item_in_session INTEGER,
    status INTEGER,
    ts TIMESTAMP,
    user_agent VARCHAR
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
    song_id VARCHAR,
    title VARCHAR,
    artist_id VARCHAR,
    artist_name VARCHAR,
    year INTEGER,
    duration FLOAT,
    artist_location VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    num_songs INTEGER
);

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_song_play (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY sortkey, 
    user_id INTEGER, 
    session_id INTEGER, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    level VARCHAR, 
    location VARCHAR, 
    user_agent VARCHAR,
    start_time TIMESTAMP
);

""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user(
    user_id INTEGER PRIMARY KEY distkey, 
    first_name VARCHAR, 
    gender CHAR, 
    level VARCHAR, 
    last_name VARCHAR
);


""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dim_song(
    song_id VARCHAR PRIMARY KEY, 
    artist_id VARCHAR distkey, 
    duration FLOAT,
    title VARCHAR, 
    year INTEGER sortkey
);


""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dim_artist(
    artist_id VARCHAR PRIMARY KEY distkey, 
    location VARCHAR sortkey, 
    name VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT
);

""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dim_time(
    start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
    day INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER,
    hour INTEGER, 
    week INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
    FROM {} 
    JSON {}
    iam_role {}
    REGION 'us-west-2'
    TIMEFORMAT as 'epochmillisecs';

""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs
    FROM {} 
    REGION 'us-west-2'
    iam_role {}
    FORMAT AS JSON 'auto';

""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO fact_song_play (start_time, user_id, song_id, artist_id, level, session_id, location, user_agent)
SELECT DISTINCT 
    to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'), 'YYYY-MM-DD HH24:MI:SS') AS start_time,  
    se.user_id, 
    ss.song_id, 
    ss.artist_id, 
    se.level, 
    se.session_id, 
    ss.artist_location AS location, 
    se.user_agent
FROM staging_events se
JOIN staging_songs ss
ON se.artist = ss.artist_name AND se.song = ss.title AND se.length = ss.duration;

""")

user_table_insert = ("""INSERT INTO dim_user(user_id, level, first_name, last_name, gender)
SELECT DISTINCT 
    user_id, 
    level,
    first_name, 
    last_name, 
    gender
FROM staging_events
WHERE user_id IS NOT NULL;

""")

song_table_insert = ("""
INSERT INTO dim_song (title, artist_id, duration, year, song_id)
SELECT DISTINCT 
    title, 
    artist_id, 
    duration, 
    year, 
    song_id
FROM staging_songs
WHERE song_id IS NOT NULL;

""")

artist_table_insert = ("""INSERT INTO dim_artist(name, latitude, longitude, artist_id, location)
SELECT DISTINCT 
    artist_name AS name, 
    artist_latitude AS latitude,
    artist_longitude AS longitude,
    artist_id, 
    artist_location AS location
FROM staging_songs
WHERE artist_id IS NOT NULL;

""")

time_table_insert = ("""INSERT INTO dim_time(weekday, year, month, week, day, hour, start_time)
SELECT DISTINCT 
    EXTRACT(WEEKDAY FROM ts) AS weekday,
    EXTRACT(YEAR FROM ts) AS year,
    EXTRACT(MONTH FROM ts) AS month,
    EXTRACT(WEEK FROM ts) AS week,
    EXTRACT(DAY FROM ts) AS day,
    EXTRACT(HOUR FROM ts) AS hour,
    ts AS start_time
FROM staging_events
WHERE ts IS NOT NULL;

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
