# DB_NAME = 'music.sqlite'
# # do fetch data function and insert data function
# # instert the info into deezer_data which will be a table in the one database
# # the lastfm id will be the foreign key
# # use track_name and artist from lastfm table to know which songs
# # i think we want the rank and explicit lyrics info?

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
    # insert only new tracks after cycling through genres, keep limti to 25
    count_added = 0
    for track in tracks:
        if count_added >= remaining_limit:
            break

        # seeing if track exists
        cur.execute('''
            SELECT id FROM lastfm_tracks
            WHERE track_name=? AND artist=? AND genre=?
        ''', (track['track_name'], track['artist'], track['genre']))
        if cur.fetchone():  # skip bc alr in database
            continue

        # add new track
        cur.execute('''
            INSERT INTO lastfm_tracks (track_name, artist, genre, duration)
            VALUES (?, ?, ?, ?)
        ''', (track['track_name'], track['artist'], track['genre'], track['duration']))

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

