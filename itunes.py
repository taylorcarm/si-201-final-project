import requests
import sqlite3
import pandas as pd


# ============================================================
# 1. FETCH FROM ITUNES API + STORE IN DATABASE
# ============================================================

def fetch_itunes_data():
    conn = sqlite3.connect("music.sqlite")
    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS itunes_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trackId INTEGER UNIQUE,
            trackName TEXT,
            artistName TEXT,
            collectionName TEXT,
            releaseDate TEXT,
            primaryGenreName TEXT
        )
    """)

    # API request
    url = "https://itunes.apple.com/search?term=music&entity=song&limit=100"
    data = requests.get(url).json()
    results = data.get("results", [])

    cur.execute("SELECT COUNT(*) FROM itunes_data")
    total = cur.fetchone()[0]
    print("Total tracks stored in DB:", total)


    print(f"Fetched {len(results)} tracks from iTunes API.")

    # Insert rows
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

    conn.commit()
    conn.close()

    print("iTunes data stored in SQLite successfully.\n")


# ============================================================
# 2. CALCULATE SOMETHING FROM DATABASE (Required by rubric)
# ============================================================

def calculate_genre_frequencies():
    conn = sqlite3.connect("music.sqlite")
    df = pd.read_sql("SELECT primaryGenreName FROM itunes_data", conn)
    conn.close()

    # Count how many tracks per genre
    genre_counts = df["primaryGenreName"].value_counts()

    return genre_counts


# ============================================================
# 3. SAVE CALCULATIONS TO A TEXT FILE (Required by rubric)
# ============================================================

def save_itunes_analysis():
    results = calculate_genre_frequencies()

    with open("itunes_analysis.txt", "w") as f:
        f.write("iTunes Genre Frequency Analysis\n")
        f.write("--------------------------------\n\n")
        f.write(results.to_string())
        f.write("\n\n(Generated from SQLite database using iTunes API data)")

    print("Saved analysis to itunes_analysis.txt\n")


# ============================================================
# RUN EVERYTHING
# ============================================================

if __name__ == "__main__":
    fetch_itunes_data()      # Step 1: Fetch & store API → DB
    save_itunes_analysis()   # Step 2: DB calculation + text output
