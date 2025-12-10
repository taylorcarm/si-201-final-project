DB_NAME = 'music.sqlite'
# do fetch data function and insert data function
# instert the info into deezer_data which will be a table in the one database
# the lastfm id will be the foreign key
# use track_name and artist from lastfm table to know which songs
# i think we want the rank and explicit lyrics info?

import requests
import sqlite3

# constants
last_fm_apikey = '46bcaf58397c885570ddf19732a63625'
db_name = 'music.sqlite'
max_tracks = 25
genres = ['hip-hop', 'rock', 'rnb', 'pop']


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
            'duration': int(track.get('duration', 0))
        })

    return results


def insert_tracks_into_db(tracks, cur, conn):
    # adds the list of tracks to database
    count_added = 0
    for track in tracks:
        cur.execute('''
            INSERT OR IGNORE INTO lastfm_tracks
            (track_name, artist, genre, duration)
            VALUES (?, ?, ?, ?)
        ''', (
            track['track_name'],
            track['artist'],
            track['genre'],
            track['duration']
        ))

        count_added += cur.rowcount  # increments only when inserted

    conn.commit()
    return count_added


def main():
    # connect to database (database must already be created)
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # ❗ DELETE LINE REMOVED SO DATA ACCUMULATES
    # cur.execute('DELETE FROM lastfm_tracks')

    for genre in genres:
        print(f"\nFetching top {max_tracks} tracks for {genre}...")
        tracks = fetch_lastfm_data(genre)
        added = insert_tracks_into_db(tracks, cur, conn)
        print(f"{added} tracks added for {genre}.")

    conn.close()


if __name__ == "__main__":
    main()