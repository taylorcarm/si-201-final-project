import sqlite3
import requests

BASE_URL = 'https://api.deezer.com'
DB_NAME = 'music.sqlite'

def fetch_deezer_data():
    print("\nFetching 25 tracks from Deezer")

    url = f"{BASE_URL}/chart/0/tracks"
    params = {'limit': 25}

    response = requests.get(url, params=params)

    if not response.ok:
        print(f"Error fetching data: {response.status_code}")
        return []
    
    tracks = response.json()['data']

    results = []
    for track in tracks:
        results.append({
            'track_name': track.get('title', ''),
            'artist': track['artist']['name'],
            'rank': track.get('rank', 0),
            'explicit_lyrics': track.get('explicity_lyrics', False)
        })

    print(f"Fetching {len(results)} tracks from Deezer")
    return results 

def store_deezer_data(deezer_tracks):    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    added = 0

    for track in deezer_tracks:
        cur.execute('''
                SELECT id from lastfm_tracks
                WHERE track_name = ? AND artist = ?
        ''', (track['track_name'], track['artist']))
        result = cur.fetchone()

        if result:
            lastfam_id = result[0]

            try:
                cur.execute('''
                    INSERT INTO deezer_data (lastfm_id, rank, explicit_lyrics)
                    VALUES(?, ?, ?)
                ''', (lastfm_id, track['rank'], track['explicit_lyrics']))
                added += 1
            except sqlite3.IntegrityError:
                pass

            conn.commit()
            conn.close()

            print(f"Stored {added} Deezer record in database")
            return added 

                
