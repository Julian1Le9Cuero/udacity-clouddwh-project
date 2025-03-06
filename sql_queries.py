import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                              CREATE TABLE staging_events(
         artist VARCHAR,
         auth VARCHAR,
         firstName VARCHAR,
         gender VARCHAR,
         itemInSession INT,
         lastName VARCHAR,
         length double precision,
         level VARCHAR,
         location VARCHAR,
         method VARCHAR,
         page VARCHAR,
         registration double precision,
         sessionId INT,
         song VARCHAR,
         status INT,
         ts TIMESTAMP,
         userAgent VARCHAR,
         userId INT)
""")

staging_songs_table_create = ("""
   CREATE TABLE staging_songs (
               num_songs INT,
               artist_id TEXT,
               artist_latitude double precision,
               artist_longitude double precision,
               artist_location VARCHAR(MAX),
               artist_name VARCHAR(MAX),
               song_id TEXT, 
               title VARCHAR(MAX), 
               duration double precision,
               year INT
                      )
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
                        songplay_id int IDENTITY(0,1), 
                         start_time time NOT NULL, 
                         user_id INT NOT NULL, 
                         level character varying(20) NOT NULL, 
                         song_id character varying(30) NOT NULL, 
                         artist_id character varying(30) NOT NULL, 
                         session_id double precision NOT NULL, 
                         location character varying(MAX),  
                         user_agent character varying(MAX) NOT NULL,
                         PRIMARY KEY(songplay_id)
                         )        
""")

user_table_create = ("""
CREATE TABLE users (
                     user_id INT NOT NULL, 
                     first_name character varying(70) NOT NULL, 
                     last_name character varying(70) NOT NULL, 
                     gender character varying(50) NOT NULL, 
                     level character varying(20) NOT NULL
                     )
""")

song_table_create = ("""
                     CREATE TABLE songs (
                        song_id character varying(30) NOT NULL, 
                        title character varying(MAX) NOT NULL, 
                        artist_id character varying(30) NOT NULL, 
                        year INT NOT NULL, 
                        duration double precision NOT NULL
                     )
""")

artist_table_create = ("""
CREATE TABLE artists (
                       artist_id character varying(30) NOT NULL, 
                       name character varying(MAX) NOT NULL, 
                       location character varying(MAX), 
                       latitude double precision, 
                       longitude double precision
                     )
""")

time_table_create = ("""
CREATE TABLE time (
                     start_time time NOT NULL, 
                     hour INT NOT NULL, 
                     day INT NOT NULL, 
                     week INT NOT NULL, 
                     month INT NOT NULL, 
                     year INT NOT NULL, 
                     weekday VARCHAR(10) NOT NULL
                     )
""")

# STAGING TABLES

staging_events_copy = ("""
   COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {}
    TIMEFORMAT 'epochmillisecs'
   REGION 'us-west-2'
""").format(
    config.get("S3", "LOG_DATA"),
    config.get("IAM_ROLE", "ARN"),
    config.get("S3", "LOG_JSONPATH")
)

staging_songs_copy = ("""
   COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2'
""").format(
    config.get("S3", "SONG_DATA"),
    config.get("IAM_ROLE", "ARN")
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT events.ts,
         events.userID,
         events.level,
         songs.song_id,
         songs.artist_id,
         events.sessionId,
         events.location,
         events.userAgent
FROM staging_events AS events
JOIN staging_songs AS songs
     ON (events.artist = songs.artist_name)
     AND (events.song = songs.title)
     AND (events.length = songs.duration)
     WHERE events.page = 'NextSong'
""")

user_table_insert = ("""
 INSERT INTO users (
                     user_id, 
                     first_name, 
                     last_name, 
                     gender, 
                     level
                     )
                     SELECT DISTINCT
                        userId,
                        firstName,
                        lastName,
                        gender,
                        level
                     FROM staging_events
                     WHERE userId IS NOT NULL
""")

song_table_insert = ("""
 INSERT INTO songs (
                     song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration
                         ) 
                        SELECT 
                           song_id, 
                           title, 
                           artist_id, 
                           year, 
                           duration
                        FROM staging_songs
                     WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (
                       artist_id, 
                       name, 
                       location, 
                       latitude, 
                       longitude
                     )
                       SELECT 
                       DISTINCT
                        artist_id, 
                        artist_name, 
                        artist_location, 
                        artist_latitude, 
                        artist_longitude
                       FROM staging_songs
                       WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
   INSERT INTO time (
                     start_time, 
                     hour, 
                     day, 
                     week, 
                     month, 
                     year, 
                     weekday
                     )
   SELECT DISTINCT 
    ts,
    EXTRACT(HOUR FROM ts),
    EXTRACT(DAY FROM ts),
    EXTRACT(WEEK FROM ts),
    EXTRACT(MONTH FROM ts),
    EXTRACT(YEAR FROM ts),
    EXTRACT(DOW FROM ts)
   FROM staging_events
   WHERE ts IS NOT NULL
""")

# 
select_staging_events = """
   SELECT COUNT(*) FROM staging_events
"""
select_staging_songs = """
   SELECT COUNT(*) FROM staging_songs
"""
select_songplays = """
   SELECT COUNT(*) FROM songplays
"""
select_users = """
   SELECT COUNT(*) FROM users
"""
select_songs = """
   SELECT COUNT(*) FROM songs
"""
select_artists = """
   SELECT COUNT(*) FROM artists
"""
select_time = """
   SELECT COUNT(*) FROM time
"""
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_queries_for_check = [select_staging_events, select_staging_songs, select_songplays, select_users, select_songs, select_artists, select_time]