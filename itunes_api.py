
import sqlite3
import requests

DB_NAME = "music.sqlite"

def main():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Make sure itunes_tracks table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS itunes_tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            track_name TEXT,
            artist_id INTEGER,
            genre_id INTEGER,
            track_price REAL,
            FOREIGN KEY (artist_id) REFERENCES artists(id),
            FOREIGN KEY (genre_id) REFERENCES genres(id),
            UNIQUE(track_name, artist_id)
        )
    """)

    url = "https://itunes.apple.com/search"
    params = {"term": "music", "entity": "song", "limit": 200}
    r = requests.get(url, params=params)
    data = r.json().get("results", [])

    for item in data:
        track = item.get("trackName")
        artist = item.get("artistName")
        genre = item.get("primaryGenreName")
        price = item.get("trackPrice")

        # Skip if track, artist, or genre is missing
        if not track or not artist or not genre:
            continue

        # Insert artist and get ID
        cur.execute("INSERT OR IGNORE INTO artists (artist_name) VALUES (?)", (artist,))
        cur.execute("SELECT id FROM artists WHERE artist_name = ?", (artist,))
        artist_id = cur.fetchone()[0]

        # Insert genre and get ID
        cur.execute("INSERT OR IGNORE INTO genres (genre_name) VALUES (?)", (genre,))
        cur.execute("SELECT id FROM genres WHERE genre_name = ?", (genre,))
        genre_id = cur.fetchone()[0]

        # Insert track
        cur.execute("""
            INSERT OR IGNORE INTO itunes_tracks
            (track_name, artist_id, genre_id, track_price)
            VALUES (?, ?, ?, ?)
        """, (track, artist_id, genre_id, price))

    conn.commit()
    conn.close()
    print("Inserted iTunes tracks into main database!")

if __name__ == "__main__":
    main()
