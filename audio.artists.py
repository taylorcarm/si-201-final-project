import requests
import sqlite3
import time

#   CREATE TABLE

def create_audiodb_table():
    conn = sqlite3.connect("music.sqlite")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audiodb_artists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artistName TEXT UNIQUE,
            country TEXT,
            genre TEXT,
            mood TEXT,
            style TEXT,
            bornYear INTEGER,
            formedYear INTEGER
        );
    """)

    conn.commit()
    conn.close()


#   GET ARTISTS FROM DATABASE (UP TO 100)

def get_artist_list_from_database():
    conn = sqlite3.connect("music.sqlite")
    cur = conn.cursor()

    # These queries pull artists from EVERY table that contains them.
    possible_queries = [
        "SELECT artist FROM lastfm_tracks",
        "SELECT artist FROM deezer_data",
        "SELECT artist FROM spotify_features",
        "SELECT artist FROM lyrics_data",
        "SELECT artist FROM musicbrainz_data",
        "SELECT artistName FROM itunes_data"
    ]

    all_artists = []

    for q in possible_queries:
        try:
            cur.execute(q)
            rows = cur.fetchall()
            all_artists.extend([row[0] for row in rows if row[0]])
        except sqlite3.OperationalError:
            # Skip table if the artist column doesn't exist
            continue

    conn.close()

    # Remove duplicates
    unique_artists = list(set(all_artists))

    print(f"FOUND DISTINCT ARTISTS IN DB: {len(unique_artists)}")

    # Return first 100 ONLY (as required by AudioDB API, no hardcoding names)
    return unique_artists[:100]
  


#   STORE ONE ARTIST RESULT

def store_artist(info):
    conn = sqlite3.connect("music.sqlite")
    cur = conn.cursor()

    cur.execute("""
        INSERT OR IGNORE INTO audiodb_artists
        (artistName, country, genre, mood, style, bornYear, formedYear)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        info.get("strArtist"),
        info.get("strCountry"),
        info.get("strGenre"),
        info.get("strMood"),
        info.get("strStyle"),
        info.get("intBornYear"),
        info.get("intFormedYear"),
    ))

    conn.commit()
    conn.close()


#   FETCH FROM APIs

def fetch_audiodb_artists():
    create_audiodb_table()   # ensure table exists

    artists = get_artist_list_from_database()
    print(f"\nFetching AudioDB data for {len(artists)} artists...\n")

    base_url = "https://theaudiodb.com/api/v1/json/2/search.php?s="

    for name in artists:
        time.sleep(0.20)  # API courtesy delay
        url = base_url + requests.utils.quote(name)

        response = requests.get(url).json()

        if response.get("artists"):
            info = response["artists"][0]
            store_artist(info)
            print(f"Stored: {name}")
        else:
            print(f"No data found: {name}")

    print("\nFinished fetching AudioDB artists.\n")


#   CALCULATE + SAVE ANALYSIS

def save_audiodb_analysis():
    conn = sqlite3.connect("music.sqlite")
    cur = conn.cursor()

    cur.execute("""
        SELECT country, COUNT(*)
        FROM audiodb_artists
        WHERE country IS NOT NULL
        GROUP BY country
        ORDER BY COUNT(*) DESC;
    """)

    results = cur.fetchall()

    with open("audiodb_analysis.txt", "w") as f:
        f.write("AudioDB Country Distribution\n")
        f.write("===========================\n\n")

        for country, count in results:
            f.write(f"{country}: {count} artists\n")

        f.write("\n(Generated from SQLite AudioDB table)\n")

    conn.close()
    print("Saved AudioDB analysis to audiodb_analysis.txt")


#   RUN EVERYTHING

if __name__ == "__main__":
    fetch_audiodb_artists()      # Fetch → store in DB
    save_audiodb_analysis()      # Analyze DB → text output
