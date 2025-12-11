import requests
import sqlite3

def fetch_itunes_data():
    conn = sqlite3.connect("music.sqlite")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS itunes_data (
            id INTEGER PRIMARY KEY,
            trackId INTEGER UNIQUE,
            trackName TEXT,
            artistName TEXT,
            collectionName TEXT,
            releaseDate TEXT,
            primaryGenreName TEXT
        )
    """)

    url = "https://itunes.apple.com/search?term=music&entity=song&limit=25"
    data = requests.get(url).json()
    results = data.get("results", [])

    print("Fetched", len(results), "iTunes tracks")

    for item in results:
        cur.execute("""
            INSERT OR IGNORE INTO itunes_data
            (trackId, trackName, artistName, collectionName, releaseDate, primaryGenreName)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            item.get("trackId"),
            item.get("trackName"),
            item.get("artistName"),
            item.get("collectionName"),
            item.get("releaseDate"),
            item.get("primaryGenreName")
        ))

    # THESE MUST BE AFTER THE LOOP
    conn.commit()
    conn.close()
    print("Inserted iTunes data. Run again to get more.")


if __name__ == "__main__":
    fetch_itunes_data()
