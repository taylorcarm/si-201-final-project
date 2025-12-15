import sqlite3
import pandas as pd

CSV_PATH = 'spotify_ids.csv'
DB_NAME = 'music.sqlite'


def update_spotify_features_from_csv(csv_path=CSV_PATH, db_name=DB_NAME):
    import sqlite3
    import pandas as pd

    df = pd.read_csv(csv_path)

    # Rename column if necessary
    if 'artists' in df.columns and 'artist' not in df.columns:
        df = df.rename(columns={'artists': 'artist'})

    required_columns = ['track_id', 'track_name', 'artist', 'danceability', 'energy', 'valence', 'tempo']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"CSV must contain columns: {required_columns}")

    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS spotify_features (
            lastfm_track_id INTEGER PRIMARY KEY,
            spotify_track_id TEXT,
            danceability REAL,
            energy REAL,
            valence REAL,
            tempo REAL
        )
    ''')

    # Map track_name to lastfm_track_id
    cur.execute('SELECT t.track_name, l.id FROM lastfm_tracks l JOIN tracks t ON l.track_id = t.id')
    track_name_to_id = {row[0]: row[1] for row in cur.fetchall()}

    df['lastfm_track_id'] = df['track_name'].map(track_name_to_id)

    # Keep only tracks that exist in lastfm_tracks
    df_to_insert = df.dropna(subset=['lastfm_track_id'])

    # Remove duplicate songs
    df_to_insert = df_to_insert.drop_duplicates(subset=['lastfm_track_id'])

    print(f"Found {len(df_to_insert)} tracks from CSV that are in lastfm_tracks.")

    for _, row in df_to_insert.iterrows():
        cur.execute('''
            INSERT OR REPLACE INTO spotify_features
            (lastfm_track_id, spotify_track_id, danceability, energy, valence, tempo)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            int(row['lastfm_track_id']),
            row['track_id'],
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