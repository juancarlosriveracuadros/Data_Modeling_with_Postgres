import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    """
Description: This function can be used to read the file in the filepath
(data/song_data) to get the song and artis info and used to populate the 
song_table and artist_table dim tables.

Arguments:
    cur: the cursor object. 
    filepath: log data file path. 

Returns:
    None
"""
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[["artist_id", "artist_name", "artist_location", \
                           "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def filter_songs(df):
    # Filtering data which contains NextSong on it.
    """
Description: filtered a dataFram df. 
The filter is all the columns of 'page' have to be 'NextSong'. 

Arguments:
    df: dataFram

Returns:
    df: filtered dataFram
"""
    is_page = df['page'] == 'NextSong'
    df = df[is_page]
    return df

def process_log_file(cur, filepath):
    """
Description: This function can be used to read the file in the filepath
(data/log_data) to get the user and time info and used to populate the users
and time dim tables. As well as compared the info in the song_table,
artist_table and the info readed from the filepath (data/log_data) and used to
populate the songplay fact table.   

Arguments:
    cur: the cursor object. 
    filepath: log data file path. 

Returns:
    None
"""
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = filter_songs(df)

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    hour = list(t.dt.hour)
    day = list(t.dt.day)
    week = list(t.dt.weekofyear)
    month = list(t.dt.month)
    year = list(t.dt.year)
    weekday = list(t.dt.weekday)
    start_time = t
    

    # insert time data records
    time_data = (start_time ,hour, day, week, month, year, weekday)
    column_labels = ("start_time" ,'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_dict = dict(zip(column_labels,time_data))
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        result = cur.fetchone()
        if result:
#            print(result)
            song_id, artist_id = list(result)
            songplay_data = (row.ts, row.userId, row.level, song_id, artist_id, \
                             row.sessionId, row.location, row.userAgent)
#            print(songplay_data)
            cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
Description: This function can be used to read the json file in the filepath 
and interate the func function over the json files the number of files and the 
number of func interationen will be printed.

Arguments:
    cur: the cursor object. 
    conn: the conection to the postgresSQL.
    filepath: the determinate filepath.
    func: func ober the json files.

Returns:
    None
"""
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres \
                            password=Password")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()