import sqlite3
import requests
import base64

# ----------------------------------------
# 1. Spotify API Authentication
# ----------------------------------------

CLIENT_ID = "e11b1760b15241c49d6000d0ce2a1c02"
CLIENT_SECRET = "3f6ae737f8a54efbb63c4b313757685a"

def get_spotify_token():
    auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
    b64_auth = base64.b64encode(auth_string.encode()).decode()

    headers = {
        "Authorization": f"Basic {b64_auth}"
    }

    data = {"grant_type": "client_credentials"}

    res = requests.post("https://accounts.spotify.com/api/token",
                        headers=headers, data=data)
    return res.json().get("access_token")


# ----------------------------------------
# 2. Get 25 songs from lastfm_tracks that do NOT have Spotify data yet
# ----------------------------------------

def get_songs_to_fetch():
    conn = sqlite3.connect("music.sqlite")
    cur = conn.cursor()

    query = """
        SELECT id, track_name, artist
        FROM lastfm_tracks
        WHERE (track_name, artist) NOT IN (
            SELECT track_name, artist FROM spotify_features
        )
        LIMIT 25;
    """

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows


# ----------------------------------------
# 3. Search Spotify for the track ID
# ----------------------------------------

def search_spotify_track(track_name, artist, token):
    url = "https://api.spotify.com/v1/search"
    headers = {"Authorization": f"Bearer {token}"}

    params = {
        "q": f"track:{track_name} artist:{artist}",
        "type": "track",
        "limit": 1
    }

    r = requests.get(url, headers=headers, params=params).json()

    try:
        track = r["tracks"]["items"][0]
        return {
            "track_id": track["id"],
            "track_name": track["name"],
            "artist": track["artists"][0]["name"]
        }
    except:
        return None


# ----------------------------------------
# 4. Get audio features
# ----------------------------------------

def get_audio_features(track_id, token):
    url = f"https://api.spotify.com/v1/audio-features/{track_id}"
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers).json()

    return {
        "tempo": r.get("tempo"),
        "energy": r.get("energy"),
        "valence": r.get("valence"),
        "danceability": r.get("danceability")
    }


# ----------------------------------------
# 5. Insert Spotify features into database
# ----------------------------------------

def insert_features(track, features):
    conn = sqlite3.connect("music.sqlite")
    cur = conn.cursor()

    query = """
        INSERT OR REPLACE INTO spotify_features
        (track_id, track_name, artist, danceability, energy, valence, tempo)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    cur.execute(query, (
        track["track_id"],
        track["track_name"],
        track["artist"],
        features["danceability"],
        features["energy"],
        features["valence"],
        features["tempo"]
    ))

    conn.commit()
    conn.close()


# ----------------------------------------
# 6. MAIN PROGRAM
# ----------------------------------------

def main():
    print("Getting Spotify access token...")
    token = get_spotify_token()

    songs = get_songs_to_fetch()
    print(f"Found {len(songs)} songs to fetch.")

    for lastfm_id, track_name, artist in songs:
        print(f"\n🎵 Fetching: {track_name} — {artist}")

        track_info = search_spotify_track(track_name, artist, token)
        if not track_info:
            print("   ❌ No Spotify match found")
            continue

        features = get_audio_features(track_info["track_id"], token)
        insert_features(track_info, features)

        print("   ✔ Added features to database")

    print("\n✨ Done! Run this script again to fetch the next 25 songs.")


if __name__ == "__main__":
    main()