import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Ingest song data from the passed-in file into the song and artist database tables

    INPUT
    cur: psycopg2 cursor object
    filepath: path to file to be ingested

    OUTPUT
    none
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    song_values = df[song_cols].values
    song_data = song_values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_cols = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_values = df[artist_cols].values
    artist_data = artist_values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Ingest log data from the passed-in file into the time, user, and songplays database tables

    INPUT
    cur: psycopg2 cursor object
    filepath: path to file to be ingested

    OUTPUT
    none
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong'].reset_index(drop=True)

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')

    # insert time data records
    h = t.dt.hour
    d = t.dt.day
    w = t.dt.week
    m = t.dt.month
    y = t.dt.year
    wd = t.dt.weekday
    time_data = [t, h, d, w, m, y, wd]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_cols = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_cols]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Extract and process all .json data files from the passed-in file directory, using the provided function, cursor object, and database connection to insert data into a postgres database

    INPUT
    cur: psycopg2 cursor object
    conn: psycopg2 databasec connection
    filepath: path to directory housing .json data files
    func: python function to ingest data into database

    OUTPUT
    none
    '''
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
    '''
    Connect to the Sparkify database and process and ingest song and log data; close the connection to the database when finished
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
