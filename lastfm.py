# DB_NAME = 'music.sqlite'

import requests
import sqlite3


DB_NAME = 'music.sqlite'
LAST_FM_APIKEY = '46bcaf58397c885570ddf19732a63625'
MAX_TRACKS_PER_RUN = 25
GENRES = ['hip-hop', 'rock', 'rnb', 'pop', 'jazz', 'country', 'metal', 'electronic',
    'indie', 'classical', 'house', 'punk']


def fetch_lastfm_data(genre):
    # get top 25 tracks for each genre
    base_url = 'http://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'tag.gettoptracks',
        'tag': genre,
        'api_key': LAST_FM_APIKEY,
        'format': 'json',
        'limit': 25  # Last.fm returns top 25 tracks per genre
    }

    response = requests.get(base_url, params=params)
    if not response.ok:
        print(f"Failed fetching {genre}, status code: {response.status_code}")
        return []

    data = response.json()
    tracks = data.get('tracks', {}).get('track', [])

    results = []
    for track in tracks:
        results.append({
            'track_name': track.get('name', ''),
            'artist': track.get('artist', {}).get('name', ''),
            'genre': genre,
            'duration': int(track.get('duration', 0))
        })
    return results


def insert_new_tracks(tracks, cur, conn, remaining_limit):
    count_added = 0
    for track in tracks:
        if count_added >= remaining_limit:
            break

        # insert into tracks/artists/genres if not exist
        cur.execute("INSERT OR IGNORE INTO tracks (track_name) VALUES (?)", (track['track_name'],))
        cur.execute("INSERT OR IGNORE INTO artists (artist_name) VALUES (?)", (track['artist'],))
        cur.execute("INSERT OR IGNORE INTO genres (genre_name) VALUES (?)", (track['genre'],))

        # get IDs
        cur.execute("SELECT id FROM tracks WHERE track_name=?", (track['track_name'],))
        track_id = cur.fetchone()[0]

        cur.execute("SELECT id FROM artists WHERE artist_name=?", (track['artist'],))
        artist_id = cur.fetchone()[0]

        cur.execute("SELECT id FROM genres WHERE genre_name=?", (track['genre'],))
        genre_id = cur.fetchone()[0]

        # check if this combination already exists in lastfm_tracks
        cur.execute('''
            SELECT id FROM lastfm_tracks
            WHERE track_id=? AND artist_id=? AND genre_id=?
        ''', (track_id, artist_id, genre_id))
        if cur.fetchone():
            continue

        # insert new track
        cur.execute('''
            INSERT INTO lastfm_tracks (track_id, artist_id, genre_id, duration)
            VALUES (?, ?, ?, ?)
        ''', (track_id, artist_id, genre_id, track['duration']))

        count_added += 1

    conn.commit()
    return count_added




def main():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    total_added = 0

    # loop genre and add track to get to 25 limit
    for genre in GENRES:
        if total_added >= MAX_TRACKS_PER_RUN:
            break

        tracks = fetch_lastfm_data(genre)
        remaining = MAX_TRACKS_PER_RUN - total_added
        added = insert_new_tracks(tracks, cur, conn, remaining)
        total_added += added

        print(f"{added} tracks added for {genre}. Total added this run: {total_added}")

    conn.close()


if __name__ == "__main__":
    main()

