import sqlite3
import pandas as pd

CSV_PATH = 'spotify_ids.csv'
DB_NAME = 'music.sqlite'

def update_spotify_features_from_csv(csv_path=CSV_PATH, db_name=DB_NAME):
    """
    Reads Spotify track features from a CSV and inserts them into the sqlite database,
    but only for tracks already in lastfm_tracks.
    Assumes CSV contains: track_id, track_name, artists, danceability, energy, valence, tempo
    """
    # Load CSV
    df = pd.read_csv(csv_path)

    # Rename column if necessary
    if 'artists' in df.columns and 'artist' not in df.columns:
        df = df.rename(columns={'artists': 'artist'})

    required_columns = ['track_id', 'track_name', 'artist', 'danceability', 'energy', 'valence', 'tempo']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"CSV must contain columns: {required_columns}")

    # Connect to DB
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    # Create spotify_features table if it doesn't exist
    cur.execute('''
        CREATE TABLE IF NOT EXISTS spotify_features (
            track_id TEXT PRIMARY KEY,
            track_name TEXT,
            artist TEXT,
            danceability REAL,
            energy REAL,
            valence REAL,
            tempo REAL
        )
    ''')

    # Get tracks already in lastfm_tracks
    cur.execute("SELECT track_name, artist FROM lastfm_tracks")
    lastfm_tracks = set(cur.fetchall())  # set of (track_name, artist)

    # # Filter CSV to only include tracks that exist in lastfm_tracks
    # df_to_insert = df[df.apply(lambda row: (row['track_name'], row['artist']) in lastfm_tracks, axis=1)]
    # Filter CSV to only include tracks that exist in lastfm_tracks
    df_to_insert = df[df.apply(lambda row: (row['track_name'], row['artist']) in lastfm_tracks, axis=1)]

    # Remove duplicate songs (multiple Spotify IDs → keep first)
    df_to_insert = df_to_insert.drop_duplicates(subset=['track_name', 'artist'])

    print(f"Found {len(df_to_insert)} tracks from CSV that are in lastfm_tracks.")

    # Insert or update each track
    for _, row in df_to_insert.iterrows():
        cur.execute('''
            INSERT OR REPLACE INTO spotify_features
            (track_id, track_name, artist, danceability, energy, valence, tempo)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            row['track_id'],
            row['track_name'],
            row['artist'],
            row['danceability'],
            row['energy'],
            row['valence'],
            row['tempo']
        ))

    conn.commit()
    conn.close()
    print(f"{len(df_to_insert)} tracks updated successfully in {db_name}!")

if __name__ == '__main__':
    update_spotify_features_from_csv()