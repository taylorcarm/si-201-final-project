import os
import shutil
import sqlite3
import unittest
import requests
import json

def create_database(db_name = 'music.sqlite'):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY,
            track_name TEXT UNIQUE
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS artists (
            id INTEGER PRIMARY KEY,
            artist_name TEXT UNIQUE
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS genres (
            id INTEGER PRIMARY KEY,
            genre_name TEXT UNIQUE
        )
    ''')

    cur.execute('''
        CREATE TABLE IF NOT EXISTS lastfm_tracks (
            id INTEGER PRIMARY KEY,
            track_id INTEGER,
            artist_id INTEGER,
            genre_id INTEGER,
            duration INTEGER
        )
    ''')


    # Drop table if it already exists (so UNIQUE constraint is applied)
    cur.execute('DROP TABLE IF EXISTS deezer_data')

    # Deezer data table (linked to lastfm_tracks via lastfm_id)
    cur.execute('''
        CREATE TABLE deezer_data (
            id INTEGER PRIMARY KEY,
            lastfm_id INTEGER UNIQUE,
            rank INTEGER,
            explicit_lyrics BOOLEAN
        )
    ''')
    
    cur.execute('DROP TABLE IF EXISTS spotify_features')

    
    cur.execute('''
        CREATE TABLE IF NOT EXISTS spotify_features (
        track_id TEXT PRIMARY KEY,
        track_name TEXT,
        danceability REAL,
        energy REAL,
        valence REAL,
        tempo REAL
    )
    ''')


    # Lyrics data table (linked to lastfm_tracks via lastfm_id)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS lyrics_data (
            id INTEGER PRIMARY KEY,
            lastfm_id INTEGER,
            lyrics TEXT
        )
    ''')



    cur.execute(''' 
        CREATE TABLE IF NOT EXISTS musicbrainz_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lastfm_id INTEGER NOT NULL,
            release_date TEXT,
            country TEXT,
            FOREIGN KEY(lastfm_id) REFERENCES lastfm_tracks(id)
        )
    ''')

    conn.commit()
    conn.close()
    print("Database and tables created!")

if __name__ == "__main__":
    create_database()