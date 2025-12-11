import requests
import sqlite3
import re

API_BASE = "https://theaudiodb.com/api/v1/json/2/search.php?s="

def clean_artist_name(name):
    if not name:
        return None

    # Lowercase
    name = name.lower()

    # Remove ft., feat., featuring
    name = re.sub(r"\s*(ft|feat|featuring)\..*$", "", name)

    # Remove punctuation
    name = re.sub(r"[^\w\s]", "", name)

    # Remove "the "
    if name.startswith("the "):
        name = name[4:]

    return name.strip()


def fetch_audiodb_artists():
    conn = sqlite3.connect("music.sqlite")
    cur = conn.cursor()

    # Make sure table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS audiodb_artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artistName TEXT,
            genre TEXT,
            yearBorn TEXT,
            yearFormed TEXT
        )
    """)

    # Get artists from lastfm_tracks
    cur.execute("SELECT DISTINCT artist FROM lastfm_tracks")
    artists = cur.fetchall()

    print(f"Found {len(artists)} artists in lastfm_tracks.")

    for (artist,) in artists:
        cleaned = clean_artist_name(artist)

        if not cleaned:
            continue

        url = API_BASE + cleaned.replace(" ", "%20")

        try:
            response = requests.get(url)
            data = response.json()

            if data and data["artists"] is not None:
                info = data["artists"][0]
                print("Match found:", artist)

                cur.execute("""
                    INSERT OR IGNORE INTO audiodb_artists
                    (artistName, genre, yearBorn, yearFormed)
                    VALUES (?, ?, ?, ?)
                """, (
                    info.get("strArtist"),
                    info.get("strGenre"),
                    info.get("intBornYear"),
                    info.get("intFormedYear")
                ))
        except:
            continue

    conn.commit()
    conn.close()
    print("Finished fetching AudioDB artist metadata.")


if __name__ == "__main__":
    fetch_audiodb_artists()

import sqlite3
conn = sqlite3.connect("music.sqlite")
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM lastfm_tracks")
print("Rows in lastfm_tracks:", cur.fetchone()[0])

conn.close()

    

if __name__ == "__main__":
    fetch_audiodb_artists()
