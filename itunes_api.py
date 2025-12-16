import sqlite3
import requests

DB_NAME = "music.sqlite"

def main():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Make sure table exists (safe to keep)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS itunes_tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_name TEXT,
            artist_name TEXT,
            genre TEXT,
            track_price REAL,
            UNIQUE(track_name, artist_name)
        );
    """)

    url = "https://itunes.apple.com/search"
    params = {
        "term": "music",
        "entity": "song",
        "limit": 200
    }

    r = requests.get(url, params=params)
    data = r.json()["results"]

    for item in data:
        track = item.get("trackName")
        artist = item.get("artistName")
        genre = item.get("primaryGenreName")
        price = item.get("trackPrice")

        cur.execute("""
            INSERT OR IGNORE INTO itunes_tracks
            (track_name, artist_name, genre, track_price)
            VALUES (?, ?, ?, ?)
        """, (track, artist, genre, price))

    conn.commit()
    conn.close()
    print("Inserted iTunes tracks")

if __name__ == "__main__":
    main()
