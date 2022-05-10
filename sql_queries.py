import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')

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
CREATE TABLE IF NOT EXISTS events_staging
  num_songs INT,
  artist_id VARCHAR NOT NULL sortkey distkey,
  artist_latitude DOUBLE PRECISION,
  artist_longitude DOUBLE PRECISION,
  artist_location VARCHAR,
  artist_name VARCHAR NOT NULL,
  song_id VARCHAR,
  title VARCHAR NOT NULL,
  duration NUMERIC NOT NULL,
  year INT
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs_staging
  artist VARCHAR,
  auth VARCHAR,
  firstName VARCHAR,
  gender VARCHAR(1),
  itemInSession INT,
  lastName VARCHAR,
  length DOUBLE PRECISION,
  level VARCHAR,
  location VARCHAR,
  method VARCHAR,
  page VARCHAR,
  registration INT,
  sessionId INT,
  song VARCHAR,
  status INT,
  ts INT NOT NULL,
  userAgent VARCHAR,
  userId INT NOT NULL sortkey distkey
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
  (
     songplay_id IDENTITY(0,1) sortkey,
     start_time  TIMESTAMP NOT NULL REFERENCES time(start_time),
     user_id     INT NOT NULL REFERENCES users(user_id),
     level       VARCHAR,
     song_id     VARCHAR REFERENCES songs(song_id) distkey,
     artist_id   VARCHAR REFERENCES artists(artist_id),
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
     name      VARCHAR NOT NULL,
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
    copy events_staging from '{}' 
    credentials 'aws_iam_role={}'
    gzip region 'us-east-1';
""").format(LOG_DATA, ARN)

staging_songs_copy = ("""
    copy songs_staging from '{}' 
    credentials 'aws_iam_role={}'
    gzip region 'us-east-1';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
            (start_time,
             user_id,
             level,
             song_id,
             artist_id,
             session_id,
             location,
             user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO NOTHING""")

user_table_insert = ("""
INSERT INTO users
            (user_id,
             first_name,
             last_name,
             gender,
             level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level""")

song_table_insert = ("""
INSERT INTO songs
            (song_id,
             title,
             artist_id,
             YEAR,
             duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO NOTHING""")

artist_table_insert = ("""
INSERT INTO artists
            (artist_id,
             name,
             location,
             latitude,
             longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING""")

time_table_insert = ("""
INSERT INTO time
            (start_time,
             hour,
             day,
             week,
             month,
             year,
             weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]