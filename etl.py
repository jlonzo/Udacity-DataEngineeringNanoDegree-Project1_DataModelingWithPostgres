import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Parses .json Songs file and loads its contents into 'songs' and 'artists'
    tables

    Parameters
    ----------
    cur : psycopg2 Connection Cursor
        DBConnection Cursor
    filepath: string
        Path of individual .json file to be processed

    """    
    # open song file
    df = pd.read_json(filepath, lines=True).replace({pd.np.nan: None})

    # insert song record
    song_data = pd.DataFrame(df, columns=['song_id','title','artist_id','year',
        'duration']).values.flatten().tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = pd.DataFrame(df, columns=['artist_id','artist_name',
        'artist_location','artist_latitude','artist_longitude']
        ).values.flatten().tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Parses .json Log file and loads its contents into 'time' and 'songplays' 
    tables

    Parameters
    ----------
    cur : psycopg2 Connection Cursor
        DBConnection Cursor
    filepath: string
        Path of individual .json file to be processed

    """       
    # open log file
    df = pd.read_json(filepath, lines=True).replace({pd.np.nan: None})

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,
        t.dt.weekday)
    column_labels = ('start_time','hour','day','week','month','year','weekday')
    time_df = pd.DataFrame.from_dict(pd.Series((time_data), 
        index=column_labels).to_dict())

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

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
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
            row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterate through each .json file found in Path and calls the function 
    that Parses and loads them into the DB

    Parameters
    ----------
    cur : psycopg2 Connection Cursor
        DBConnection Cursor
    conn : psycopg2 DBConnection
        Database Connection
    filepath: string
        Path of the Directory that contains .json files to be processed
    func: Function
        Call-to function for processing each .json file found in Directory

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
    """
    Main Program:     
    1. Create DB Connnection and DBConnection Cursor
    2. Process song_data files loading 'songs' and 'artists' tables
    3. Process log_data files
    4. Close DB Connection

    """    
    conn = psycopg2.connect("""
        host=127.0.0.1 dbname=sparkifydb user=student password=student""")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()