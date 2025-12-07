# practing!
import os
import shutil
import sqlite3
import unittest
import requests
import json


last_fm_apikey = '46bcaf58397c885570ddf19732a63625' 
max_tracks = 25
genres = ['hip-hop', 'rock', 'rnb', 'pop']

def create_lastfm_database(db_name='music.sqlite'):
    # creates sql database for lastfm tracks
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # cur.execute('''
    # CREATE TABLE IF NOT EXISTS lastfm_tracks (
    #     id INTEGER PRIMARY KEY,
    #     track_name TEXT,
    #     artist TEXT,
    #     genre TEXT,
    #     listeners INTEGER,
    #     playcount INTEGER,
    #     mbid TEXT
    # )
    # ''')
    # ^ scratch this bc too hard to get listeners and playcount data

    # bc i already amde the table several times
    cur.execute('DROP TABLE IF EXISTS lastfm_tracks')


    cur.execute('''
    CREATE TABLE IF NOT EXISTS lastfm_tracks (
        id INTEGER PRIMARY KEY,
        track_name TEXT,
        artist TEXT,
        genre TEXT,
        duration INTEGER,
        mbid TEXT
    )
    ''')

    conn.commit()
    conn.close()
    print("database and table ready!")

def fetch_lastfm_data(genre, max_tracks=max_tracks):
    # fetches top 25 tracks for each genre and returns dict with track info
    base_url = 'http://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'tag.gettoptracks',
        'tag': genre,
        'api_key': last_fm_apikey,
        'format': 'json',
        'limit': max_tracks
    }

    response = requests.get(base_url, params=params)
    if not response.ok:
        print(f"failed fetching {genre}, status code: {response.status_code}")
        return []

    data = response.json()
    tracks = data.get('tracks', {}).get('track', [])[:max_tracks]

    results = []
    for track in tracks:
        results.append({
            'track_name': track.get('name', ''),
            'artist': track.get('artist', {}).get('name', ''),
            'genre': genre,
            'duration': int(track.get('duration', 0)),
            'mbid': track.get('mbid', '')
        })
    
    return results


def insert_tracks_into_db(tracks, cur, conn):
    # adds the list of tracks to database
    count_added = 0
    for track in tracks:
        cur.execute('''
            INSERT OR IGNORE INTO lastfm_tracks
            (track_name, artist, genre, duration, mbid)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            track['track_name'],
            track['artist'],
            track['genre'],
            track['duration'],
            track['mbid']
        ))
        # count should reach 100
        count_added += 1

    conn.commit()
    return count_added

def main():
    create_lastfm_database()

    # connect to database
    conn = sqlite3.connect('music.sqlite')
    cur = conn.cursor()

    cur.execute('DELETE FROM lastfm_tracks')
    conn.commit()

    for genre in genres:
        print(f"\nFetching top {max_tracks} tracks for {genre}...")
        tracks = fetch_lastfm_data(genre)
        added = insert_tracks_into_db(tracks, cur, conn)  
        print(f"{added} tracks added for {genre}.")

    conn.close()

CREATE TABLE IF NOT EXISTS spotify_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    lastfm_id INTEGER,
    tempo REAL,
    energy REAL,
    valence REAL,
    danceability REAL,
    popularity INTEGER,
    FOREIGN KEY (lastfm_id) REFERENCES lastfm_tracks(id)
);

    

if __name__ == "__main__":
    main()



#will add spotify now