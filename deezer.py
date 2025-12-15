import sqlite3
import requests

BASE_URL = 'https://api.deezer.com'
DB_NAME = 'music.sqlite'
MAX_INSERTS = 25

def fetch_deezer_data():
    """
    Retrieves Deezer track information for songs stored in lastfm_tracks
    """
    # Connects to SQLite database to access track data
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Selects tracks from lastfm_tracks that are not already in deezer_data
    # Prevents duplicate data 
    # cur.execute('''
    #     SELECT id, track_name, artist
    #     FROM lastfm_tracks
    #     WHERE id NOT IN (SELECT lastfm_id FROM deezer_data)
    # ''')
    cur.execute('''
        SELECT l.id, t.track_name, a.artist_name
        FROM lastfm_tracks l
        JOIN tracks t ON l.track_id = t.id
        JOIN artists a ON l.artist_id = a.id
        WHERE l.id NOT IN (SELECT lastfm_id FROM deezer_data)
    ''')
    
    # Accesses tracks that still need Deezer data 
    rows = cur.fetchall()
    conn.close()

    # Stores Deezer results
    results = []
    # Counts how many Deezer records are collected 
    inserted_count = 0

    # Loops through Last.fm tracks to fetch corresponding Deezer data
    for lastfm_id, track_name, artist in rows:
        if inserted_count >= MAX_INSERTS:
            # Stops loops early if maximum number of allowed API inserts were reached
            break 

        # Builds API request to search for specific track on Deezer
        url = f"https://api.deezer.com/search"
        params = {'q': f'{track_name} {artist}', 'limit': 1}
        # Sends API request and gets response
        response = requests.get(url, params=params)
       
        # If API request fails, skip track
        if not response.ok:
            continue 
        
        # Accesses 'data' field from API response 
        data = response.json().get('data', [])
        # If no matching tracks were found on Deezer, skip  
        if not data:
            continue 
       
        track = data[0]
        
        # Stores Deezer information in dictionary 
        results.append({
            # Connects Deezer info to lastfm_tracks
            'lastfm_id': lastfm_id,
            # Uses Deezer title if available, otherwise keep original Last.fm track_name
            'track_name': track.get('title', track_name),
            # Finds artists name from Deezer
            'artist': track['artist']['name'],
            # Popularity rank 
            # If missing, default to 0
            'rank': track.get('rank', 0),
            # Finds if song is explicit as integer 
            'explicit_lyrics': int(track.get('explicit_lyrics', False))
        })

        # Adds successful retrieval to count 
        inserted_count += 1
    
    print(f'Fetched {len(results)} tracks from Deezer')

    return results 

def store_deezer_data(deezer_tracks):
    # Connects to database     
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    added = 0

    # Insert Deezer record into deezer_data 
    for track in deezer_tracks:
        lastfm_id = track['lastfm_id']
            
        # Inserts rank and explicit_lyrics into database
        # Updates existing row if lastfm_id already exists in table (prevents duplicate records)
        cur.execute('''
            INSERT INTO deezer_data (lastfm_id, rank, explicit_lyrics)
            VALUES(?, ?, ?)
            ON CONFLICT(lastfm_id) DO UPDATE SET
                rank = excluded.rank,
                explicit_lyrics = excluded.explicit_lyrics
        ''', (lastfm_id, track['rank'], int(track['explicit_lyrics'])))
        
        # Counts how many tracks were stored or updated
        added += 1

    # Saves changes to database 
    conn.commit()
    conn.close()

    print(f"Stored {added} Deezer record in database")
    return added 

def main():
    # Fetches new Deezer data for any un-analyzed tracks
    deezer_tracks = fetch_deezer_data()

    # Stores results in database 
    stored_count = store_deezer_data(deezer_tracks)

    print(f'Finished! {stored_count} Deezer records added to database.')


if __name__ == "__main__":
    main()