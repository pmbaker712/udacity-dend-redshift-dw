import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
SONG_DATA = config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""

    CREATE TABLE staging_events (
        artist varchar,
        auth varchar,
        first_name varchar,
        gender char,
        items_in_session int,
        last_name varchar,
        length decimal,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration decimal,
        session_id int,
        song varchar,
        status int ,
        ts bigint,
        user_agent varchar,
        user_id int
);
""")

staging_songs_table_create = ("""

    CREATE TABLE staging_songs (
        artist_id varchar,
        artist_latitude decimal,
        artist_location varchar,
        artist_longitude decimal,
        artist_name varchar,
        duration decimal,
        num_songs int,
        song_id text,
        title varchar, 
        year int
);
""")

songplay_table_create = ("""
    
    CREATE TABLE songplays (
        songplay_id int identity(0,1) PRIMARY KEY SORTKEY DISTKEY,
        start_time timestamp,
        user_id varchar,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id varchar,
        location varchar,
        user_agent varchar
)
DISTSTYLE KEY;
""")

user_table_create = ("""
    
    CREATE TABLE users (
        user_id varchar PRIMARY KEY SORTKEY,
        first_name varchar,
        last_name varchar,
        gender varchar(2),
        level varchar
)
DISTSTYLE ALL;
""")

song_table_create = ("""

    CREATE TABLE songs (
        song_id varchar PRIMARY KEY SORTKEY,
        title varchar,
        artist_id varchar,
        year int,
        duration decimal
)
DISTSTYLE ALL;
""")

artist_table_create = ("""
    
    CREATE TABLE artists (
        artist_id varchar PRIMARY KEY SORTKEY,
        name varchar,
        location varchar,
        latitude decimal,
        longitude decimal
)
DISTSTYLE ALL;
""")

time_table_create = ("""

    CREATE TABLE time (
        start_time timestamp PRIMARY KEY SORTKEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
)
DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""

    COPY staging_events FROM {}
    IAM_ROLE {}
    FORMAT AS json {};

""").format(LOG_DATA,IAM,LOG_JSONPATH)

staging_songs_copy = ("""

    COPY staging_songs FROM {}
    IAM_ROLE {}
    FORMAT AS json 'auto';

""").format(SONG_DATA,IAM)

# FINAL TABLES

songplay_table_insert = ("""
    
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (E.ts/1000) * INTERVAL '1 second' as start_time,
        E.user_id,
        E.level,
        S.song_id,
        S.artist_id,
        E.session_id,
        E.location,
        E.user_agent
    FROM staging_events E
    INNER JOIN staging_songs S
    ON E.song = S.title AND E.artist = S.artist_name
    WHERE E.page = 'NextSong';

""")

user_table_insert = ("""
    
    INSERT INTO users
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE user_id IS NOT NULL;
    
""")

song_table_insert = ("""

    INSERT INTO songs
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
    
""")

artist_table_insert = ("""
   
    INSERT INTO artists
    SELECT DISTINCT
       artist_id,
       artist_name AS name,
       artist_location AS location,
       artist_latitude AS latitude,
       artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
    
""")

time_table_insert = ("""
   
   INSERT INTO time
   SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEK FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       EXTRACT(WEEKDAY FROM start_time) AS weekday
    FROM staging_events;
    
""")

# QUERY LISTS

create_table_queries = {'STAGING EVENTS': staging_events_table_create, 
                        'STAGING SONGS': staging_songs_table_create,
                        'SONGPLAYS':songplay_table_create,
                        'USERS': user_table_create,
                        'SONGS': song_table_create,
                        'ARTISTS': artist_table_create,
                        'TIME': time_table_create
                       }


drop_table_queries = {'STAGING EVENTS': staging_events_table_drop, 
                      'STAGING SONGS': staging_songs_table_drop,
                      'SONGPLAYS':songplay_table_drop,
                      'USERS': user_table_drop,
                      'SONGS': song_table_drop,
                      'ARTISTS': artist_table_drop,
                      'TIME': time_table_drop
                     }

copy_table_queries = {'STAGING EVENTS': staging_events_copy, 
                      'STAGING SONGS': staging_songs_copy
                     }


insert_table_queries = {'SONGPLAYS':songplay_table_insert,
                        'USERS': user_table_insert,
                        'SONGS': song_table_insert,
                        'ARTISTS': artist_table_insert,
                        'TIME': time_table_insert
                       }
                        
