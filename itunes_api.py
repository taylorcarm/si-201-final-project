import sqlite3
import requests

DB_NAME = "music.sqlite"   # ← PUT IT HERE

def main():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    url = "https://itunes.apple.com/search"
    params = {
        "term": "music",
        "entity": "song",
        "limit": 100
    }

    r = requests.get(url, params=params)
    data = r.json()["results"]

    for item in data:
        cur.execute("""
            INSERT INTO itunes_tracks (track_name, artist_name, genre, track_price)
            VALUES (?, ?, ?, ?)
        """, (
            item.get("trackName"),
            item.get("artistName"),
            item.get("primaryGenreName"),
            item.get("trackPrice")
        ))

    conn.commit()
    conn.close()
    print("Inserted 100 iTunes tracks")

if __name__ == "__main__":
    main()
