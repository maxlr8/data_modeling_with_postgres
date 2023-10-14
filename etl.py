import pandas as pd
import warnings
import psycopg2
import glob
import os

from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function processes song files and inserts the records into Postgres database.
    """

    # Open the song details file from the path provided as dataframe.
    df = pd.DataFrame([pd.read_json(filepath, typ="series", convert_dates=False)])

    for value in df.values:
        (
            num_songs,
            artist_id,
            artist_latitude,
            artist_longitude,
            artist_location,
            artist_name,
            song_id,
            title,
            duration,
            year,
        ) = value

        # Insert Artist records.
        artist_data = (
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude,
        )
        cur.execute(artist_table_insert, artist_data)

        # Insert Song records.
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(song_table_insert, song_data)

    print(f"Records inserted for file - {filepath}")


def process_log_file(cur, filepath):
    """
    This function processes event log files and inserts records into the Postgres database.
    """
    # Open log file
    df = df = pd.read_json(filepath, lines=True)

    # Filter by NextSong action
    df = df[df["page"] == "NextSong"].astype({"ts": "datetime64[ms]"})

    # Convert timestamp column to datetime
    t = pd.Series(df["ts"], index=df.index)

    # Insert time data records
    column_labels = [
        "timestamp",
        "hour",
        "day",
        "weelofyear",
        "month",
        "year",
        "weekday",
    ]

    time_data = []
    for data in t:
        time_data.append(
            [
                data,
                data.hour,
                data.day,
                data.weekofyear,
                data.month,
                data.year,
                data.day_name(),
            ]
        )

    time_df = pd.DataFrame.from_records(data=time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # Insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # Insert songplay records
    for index, row in df.iterrows():
        # Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Insert songplay record
        songplay_data = (
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, con, filepath, func):
    """
    This is a driver function to load data from songs and event log files into Postgres database.
    """
    # Get all the files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # Get the total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}\n")

    # Iterate over files and process
    for count, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        con.commit()
        print(f"{count}/{num_files} files processed.")


def main():
    """
    This is a driver function to process and load songs data into Postgres database.
    """
    warnings.filterwarnings("ignore")
    
    con = psycopg2.connect(
        host="127.0.0.1",
        dbname="sparkify",
        user="postgres",
        password=os.environ["PSQL_PASS"],
    )
    cur = con.cursor()

    process_data(cur, con, filepath="data/songs_data", func=process_song_file)
    process_data(cur, con, filepath="data/log_data", func=process_log_file)

    cur.close()
    con.close()
    print("\n\nFinished processing!!!\n\nAll files loaded into Sparkify Database.")


if __name__ == "__main__":
    main()
