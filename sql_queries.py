import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS ""users"""
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# purpose of staging tables: increase efficiency of ETL processes, ensure data integrity and support data quality operations
# constraints should not be applied to the staging tables
staging_events_table_create= ("""CREATE TABLE staging_events(
      event_id INT IDENTITY(0,1) PRIMARY KEY
    , artist_name VARCHAR
    , auth VARCHAR(50)
    , user_first_name VARCHAR(255)
    , user_gender VARCHAR(1)
    , item_in_session INTEGER
    , user_last_name VARCHAR(255)
    , song_length NUMERIC
    , user_level VARCHAR(50)
    , location VARCHAR(255)
    , method VARCHAR(25)
    , page VARCHAR(35)
    , registration VARCHAR(50)
    , session_id BIGINT
    , song_title VARCHAR
    , status INTEGER
    , ts BIGINT
    , user_agent TEXT
    , user_id BIGINT
    );
""")

# order of columns and column names must match json file
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
      num_songs INTEGER
    , artist_id VARCHAR
    , artist_latitude NUMERIC
    , artist_longitude NUMERIC
    , artist_location VARCHAR
    , artist_name VARCHAR
    , song_id VARCHAR
    , title VARCHAR
    , duration NUMERIC
    , year INTEGER
    );
""")

# If recent data is queried most frequently, specify the timestamp column as the leading column for the sort key
# fact table can have only one distribution key
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
      songplay_id INT IDENTITY(0,1) 
    , start_time TIMESTAMP NOT NULL SORTKEY
    , user_id BIGINT NOT NULL
    , level VARCHAR(50)
    , song_id VARCHAR(18) NOT NULL
    , artist_id VARCHAR(18) DISTKEY NOT NULL
    , session_id BIGINT NOT NULL
    , location VARCHAR(255)
    , user_agent TEXT
    , PRIMARY KEY (songplay_id)
    , FOREIGN KEY (start_time) REFERENCES time(start_time)
    , FOREIGN KEY (user_id) REFERENCES "users"(user_id)
    , FOREIGN KEY (song_id) REFERENCES songs(song_id)
    , FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
    );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS "users" (
      user_id BIGINT PRIMARY KEY DISTKEY
    , first_name VARCHAR(255)
    , last_name VARCHAR(255)
    , gender VARCHAR(1)
    , level VARCHAR(50)
    );
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
      song_id VARCHAR PRIMARY KEY DISTKEY SORTKEY
    , title VARCHAR
    , artist_id VARCHAR NOT NULL
    , year INTEGER
    , duration NUMERIC
    );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
      artist_id VARCHAR PRIMARY KEY DISTKEY SORTKEY
    , artist_name VARCHAR 
    , location VARCHAR
    , latitude NUMERIC
    , longitude NUMERIC
    );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
      start_time TIMESTAMP PRIMARY KEY DISTKEY SORTKEY
    , hour INTEGER
    , day INTEGER
    , week INTEGER
    , month INTEGER
    , year INTEGER
    , weekday INTEGER
    );
""")

# STAGING TABLES

# If the JSON data objects don't correspond directly to column names, you can use a JSONPaths file to map the JSON elements to columns. The order doesn't matter in the JSON source data, but the order of the JSONPaths file expressions must match the column order
# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json
staging_events_copy = ("""
                          COPY staging_events 
                          FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          REGION 'us-west-2'
                          JSON AS '{}'
                       """).format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

# To load from JSON data using the 'auto' argument, the JSON data must consist of a set of objects. The key names must match the column names
staging_songs_copy = ("""
                          COPY staging_songs 
                          FROM '{}'
                          CREDENTIALS 'aws_iam_role={}'
                          REGION 'us-west-2'
                          JSON AS 'auto'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
# UNIQUE is not enforced on tables by Amazon Redshift, hence ensure unique values with select distinct
songplay_table_insert = ("""
    INSERT INTO songplays( 
                              start_time 
                            , user_id 
                            , level 
                            , song_id 
                            , artist_id 
                            , session_id 
                            , location 
                            , user_agent 
                         )
    SELECT distinct TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time
         , se.user_id AS user_id
         , se.user_level AS level
         , s.song_id AS song_id
         , s.artist_id AS artist_id
         , se.session_id AS session_id
         , se.location AS location
         , se.user_agent AS user_agent
    FROM staging_events se
    LEFT JOIN staging_songs s
        ON s.title = se.song_title
        AND s.artist_name = se.artist_name
    WHERE se.page = 'NextSong'
        AND se.user_id IS NOT NULL 
""")

user_table_insert = ("""
    INSERT INTO "users" (
                          user_id
                        , first_name
                        , last_name
                        , gender
                        , level
                        )
    SELECT distinct se.user_id AS user_id
         , se.user_first_name AS first_name
         , se.user_last_name AS user_last_name
         , se.user_gender AS gender
         , se.user_level AS level
    FROM staging_events se
    WHERE se.user_id IS NOT NULL    
""")

song_table_insert = ("""
    INSERT INTO songs (
                          song_id
                        , title
                        , artist_id
                        , year
                        , duration
                      )
    SELECT distinct s.song_id AS song_id
         , s.title AS title
         , s.artist_id AS artist_id
         , s.year AS year
         , s.duration AS duration
    FROM staging_songs s
    WHERE s.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (
                          artist_id
                        , artist_name
                        , location
                        , latitude
                        , longitude
                        )
    SELECT distinct s.artist_id AS artist_id
         , s.artist_name AS artist_name
         , s.artist_location AS location
         , s.artist_latitude AS latitude
         , s.artist_longitude AS longitude
    FROM staging_songs s
    WHERE s.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (
                      start_time
                    , hour
                    , day
                    , week
                    , month
                    , year
                    , weekday
                    )
     SELECT distinct TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time_insert
          , DATE_PART(hrs, start_time_insert) AS hour
          , DATE_PART(dayofyear, start_time_insert) AS day
          , DATE_PART(w, start_time_insert) AS week
          , DATE_PART(mons ,start_time_insert) AS month
          , DATE_PART(yrs , start_time_insert) AS year
          , DATE_PART(dow, start_time_insert) AS weekday
     FROM staging_events se
     WHERE se.ts IS NOT NULL
""")

# QUERY LISTS
#insert and drop songplay last since it contains references to other tables
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
