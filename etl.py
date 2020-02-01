import os
import io
import glob
import psycopg2
import pandas as pd
import datetime as dt
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes the individual song file and inserts a record into SONGS table and ARTISTS table
        Args:
        (cursor) cur   - connection cursor for accessing database
        (str) filepath - absolute path for the file that needs processing
    
    """
    
    # open song file
    df = pd.DataFrame(pd.read_json(filepath,typ='Series')).transpose()

    #Clean the data for song-year and artist-location
    df['year'] = df['year'].where(df['year'] != 0 ,None)
    df['artist_location'] = df['artist_location'].where(df['artist_location'] != '',None)
    
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].iloc[0].values.tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].iloc[0].values.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes the individual log file and inserts records into TIME, USERS and SONGPLAYS table.
    USERS and ARTISTS table must be populated before this process.
        Args:
        (cursor) cur   - connection cursor for accessing database
        (str) filepath - absolute path for the file that needs processing
    """
    
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df['page'] =='NextSong']
    # sort the records by timestamp so that the users.level is updated with the latest value from the source in target users table.
    df.sort_values(['ts'], inplace = True)

    # convert timestamp column to datetime
    t = df['ts'].transform(lambda x: dt.datetime.fromtimestamp(x/1000))
    
    # insert time data records
    time_data = (t,t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ('strt_ts','hour','day','weekofyear','month','year','weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))
    time_df.drop_duplicates(keep='first',inplace=True)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame(df[['userId','firstName','lastName','gender','level']])
    #drop duplicates to reduce redundant insert requests
    user_df.drop_duplicates(keep='first',inplace=True)
    
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
        songplay_data = [dt.datetime.fromtimestamp(row['ts']/1000),row['userId'],row['level'],songid,artistid,row['sessionId'],row['location'],row['userAgent'].strip('"')]
        cur.execute(songplay_table_insert, songplay_data)



def db_insert_summary(cur):
    """
    Prints the number of records inserted in the tables
        Args:
        (cursor) cur   - connection cursor for accessing database
    """
    
    # print record count for SONGPLAYS table
    cur.execute("select count(*) from songplays")
    print("{:10d} - Records present in SONGPLAYS table".format(cur.fetchone()[0]))
    # print record count for TIME table
    cur.execute("select count(*) from time")
    print("{:10d} - Records present in TIME table".format(cur.fetchone()[0]))
    # print record count for SONGS table
    cur.execute("select count(*) from songs")
    print("{:10d} - Records present in SONGS table".format(cur.fetchone()[0]))
    # print record count for ARTISTS table
    cur.execute("select count(*) from artists")
    print("{:10d} - Records present in ARTISTS table".format(cur.fetchone()[0]))
    # print record count for USERS table
    cur.execute("select count(*) from users")
    print("{:10d} - Records present in USERS table".format(cur.fetchone()[0]))
    
    
        
def process_data(cur, conn, filepath, func):
    """
    Traverses through the file locations to get exhaustive list of files 
    and processes them to populate data in database tables.
        Args:
        (cursor) cur   - connection cursor for accessing postgreSQL database
        (db con) conn  - connection object to postgreSQL database
        (str) filepath - file path to traverse for the files
        (obj) func     - function to be called for processing data based on the file type(song vs log file)
    
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    #Remove the 'checkpoint' files which get created by the '.ipynb' notebooks and are not the files for data processing.
    fltr_files = []
    for rec in all_files:
        if 'checkpoint' not in rec:
            fltr_files.append(rec)

    # get total number of files found
    num_files = len(fltr_files)
    print('{} files found in {}'.format(num_files, filepath))
    
    # sort the files to be processed
    fltr_files.sort()
    
    # iterate over files and process
    for i, datafile in enumerate(fltr_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))



def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    db_insert_summary(cur)
    conn.close()


if __name__ == "__main__":
    main()