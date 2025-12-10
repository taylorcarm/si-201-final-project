import sqlite3
import requests

BASE_URL = 'https://api.deezer.com'
DB_NAME = 'music.sqlite'
MAX_INSERTS = 25

def fetch_deezer_data():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute('''
        SELECT id, track_name, artist
        FROM lastfm_tracks
        WHERE id NOT IN (SELECT lastfm_id FROM deezer_data)
    ''')
    rows = cur.fetchall()
    conn.close()

    results = []
    inserted_count = 0

    for lastfm_id, track_name, artist in rows:
        if inserted_count >= MAX_INSERTS:
            break 

        url = f"https://api.deezer.com/search"
        params = {'q': f'{track_name} {artist}', 'limit': 1}
        response = requests.get(url, params=params)
       
        if not response.ok:
            continue 
        
        data = response.json().get('data', [])
        if not data:
            continue 
       
        track = data[0]
        
        results.append({
            'lastfm_id': lastfm_id,
            'track_name': track.get('title', track_name),
            'artist': track['artist']['name'],
            'rank': track.get('rank', 0),
            'explicit_lyrics': int(track.get('explicit_lyrics', False))
        })

        inserted_count += 1
    
    print(f'Fetched {len(results)} tracks from Deezer')

    return results 

def store_deezer_data(deezer_tracks):    
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    added = 0

    for track in deezer_tracks:
        lastfm_id = track['lastfm_id']
            
        cur.execute('''
            INSERT INTO deezer_data (lastfm_id, rank, explicit_lyrics)
            VALUES(?, ?, ?)
            ON CONFLICT(lastfm_id) DO UPDATE SET
                rank = excluded.rank,
                explicit_lyrics = excluded.explicit_lyrics
        ''', (lastfm_id, track['rank'], int(track['explicit_lyrics'])))
        
        added += 1

    conn.commit()
    conn.close()

    print(f"Stored {added} Deezer record in database")
    return added 

def main():
    deezer_tracks = fetch_deezer_data()

    stored_count = store_deezer_data(deezer_tracks)

    print(f'Finished! {stored_count} Deezer records added to database.')


if __name__ == "__main__":
    main()


