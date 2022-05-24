import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE','ARN')
roleARN = config.get('IAM_ROLE', 'roleARN')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS events_staging (
  artist VARCHAR,
  auth VARCHAR,
  firstName VARCHAR,
  gender VARCHAR,
  itemInSession INT,
  lastName VARCHAR,
  length DOUBLE PRECISION,
  level VARCHAR,
  location VARCHAR,
  method VARCHAR,
  page VARCHAR,
  registration BIGINT,
  sessionId INT,
  song VARCHAR,
  status INT,
  ts BIGINT,
  userAgent VARCHAR,
  userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_staging (
  num_songs INT,
  artist_id VARCHAR,
  artist_latitude DOUBLE PRECISION,
  artist_longitude DOUBLE PRECISION,
  artist_location VARCHAR,
  artist_name VARCHAR,
  song_id VARCHAR sortkey distkey,
  title VARCHAR,
  duration NUMERIC,
  year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
  (
     songplay_id INT IDENTITY(0,1) sortkey,
     start_time  TIMESTAMP NOT NULL,
     user_id     INT,
     level       VARCHAR,
     song_id     VARCHAR distkey,
     artist_id   VARCHAR,
     session_id  INT,
     location    VARCHAR,
     user_agent  VARCHAR
  ) 
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
  (
     user_id    INT sortkey,
     first_name VARCHAR,
     last_name  VARCHAR,
     gender     VARCHAR(1),
     level      VARCHAR 
  ) 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
  (
     song_id   VARCHAR sortkey distkey,
     title     VARCHAR NOT NULL,
     artist_id VARCHAR,
     year      INT,
     duration  NUMERIC NOT NULL
  ) 
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
  (
     artist_id VARCHAR sortkey,
     name      VARCHAR(500) NOT NULL,
     location  VARCHAR,
     latitude  DOUBLE PRECISION,
     longitude DOUBLE PRECISION
  ) 
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
  (
     start_time TIMESTAMP sortkey,
     hour       INT,
     day        INT,
     week       INT,
     month      INT,
     year       INT,
     weekday    INT
  ) 
""")

# STAGING TABLES

staging_events_copy = ("""
    copy events_staging from {} 
    credentials 'aws_iam_role={}'
    region 'us-west-2' json {};
""").format(LOG_DATA, roleARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy songs_staging from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, roleARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
  start_time, user_id, level, song_id, 
  artist_id, session_id, location, 
  user_agent
) 
SELECT DISTINCT 
  TIMESTAMP 'epoch' + e.ts / 1000 * INTERVAL '1 second' AS start_time, 
  e.userid, 
  e.level, 
  s.song_id, 
  s.artist_id, 
  e.sessionId, 
  e.location, 
  e.userAgent 
FROM 
  events_staging e 
  JOIN songs_staging s ON e.artist = s.artist_name AND e.song = s.title
WHERE 
  e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (
  user_id, first_name, last_name, gender, level
) 
SELECT DISTINCT 
  userId, 
  firstName, 
  lastName, 
  gender, 
  level 
FROM 
  events_staging
WHERE userId is NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (
  song_id, title, artist_id, year, duration
) 
SELECT DISTINCT 
  song_id, 
  title, 
  artist_id, 
  year, 
  duration 
FROM 
  songs_staging
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (
  artist_id, name, location, latitude, longitude
) 
SELECT DISTINCT 
  artist_id, 
  artist_name, 
  artist_location, 
  artist_latitude, 
  artist_longitude 
FROM 
  songs_staging
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO TIME(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
  start_time, 
  EXTRACT(hour FROM start_time),
  EXTRACT(day FROM start_time),
  EXTRACT(week FROM start_time),
  EXTRACT(month FROM start_time),
  EXTRACT(year FROM start_time),
  EXTRACT(dow FROM start_time)
FROM 
  songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
