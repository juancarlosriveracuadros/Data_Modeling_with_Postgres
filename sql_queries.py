# DROP TABLES
songplay_table_drop = "DROP table IF EXISTS songplay;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS song_table;"
artist_table_drop = "DROP table IF EXISTS artist_table;"
time_table_drop = "DROP table IF EXISTS time_table;"

# CREATE TABLES
def create_table(table,columns_transact_SQL):
    """
Description: This function return a postgresSQL query to create a table 

Arguments:
    table: table name 
    columns_transact_SQL: name and type of the columns

Returns:
    SQL query 
"""
    return "CREATE TABLE IF NOT EXISTS " + table +"("+ columns_transact_SQL +");"

songplay_table_create = create_table("songplay","songplay_id SERIAL PRIMARY KEY, \
                                     start_time timestamp NOT NULL, user_id text  \
                                     NOT NULL, level text, song_id text, artist_id text,\
                                     session_id text, location text, user_agent text")

user_table_create = "CREATE TABLE IF NOT EXISTS users (userId text primary key, \
                    firstName text, lastName text, gender text, level text);"

song_table_create = create_table("song_table","song_id text primary key, title text, \
                                 artist_id text, year int, duration float")

artist_table_create = create_table("artist_table","artist_id text primary key, \
                                   artist_name text, artist_location text, \
                                   artist_latitude float, artist_longitude float")

time_table_create = create_table("time_table","start_time timestamp primary key, hour int,\
                                 day int, week int, month int, year int, weekday int")


# INSERT RECORDS
songplay_table_insert = ("INSERT INTO songplay (start_time, user_id, level, song_id,\
artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

user_table_insert = ("INSERT INTO users (userId, firstName, lastName, gender, level) VALUES\
(%s, %s, %s, %s, %s) ON CONFLICT (userId) DO UPDATE SET level=EXCLUDED.level;")

song_table_insert = ("INSERT INTO song_table (song_id, title, artist_id, year, duration)\
VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING")

artist_table_insert = ("INSERT INTO artist_table (artist_id, artist_name, artist_location,\
artist_latitude, artist_longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) \
DO NOTHING")

time_table_insert = ("INSERT INTO time_table (start_time, hour, day, week, month, year,\
weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING")

# FIND SONGS
song_select = "SELECT song_table.song_id, artist_table.artist_id from (song_table INNER \
JOIN artist_table ON artist_table.artist_id=song_table.artist_id) WHERE \
song_table.title = %s and artist_table.artist_name = %s and song_table.duration = %s;"

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, \
                        artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, \
                      artist_table_drop, time_table_drop]