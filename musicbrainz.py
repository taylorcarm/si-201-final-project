import sqlite3
import requests

DB_NAME = 'music.sqlite'
API_BASE = 'https://musicbrainz.org/ws/2/'

HEADERS = {
    'User-Agent': 'MyMusicApp/1.0 ( myemail@example.com )',
    'Accept': 'application/json'
}

def search_musicbrainz(track_name, artist_name):
    query = 'recording:"{}" AND artist:"{}"'.format(track_name, artist_name)
    url = '{}recording/?query={}&limit=1&fmt=json'.format(API_BASE, query)
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print("MusicBrainz request failed:", response.status_code)
            return None
        data = response.json()
        recordings = data.get('recordings', [])
        if len(recordings) == 0:
            return None
        return recordings[0]
    except:
        #print("Error fetching MusicBrainz data for {} by {}".format(track_name, artist_name))
        return None


def insert_musicbrainz_data(cur, lastfm_id, mb_data):
    if mb_data is None:
        return

    release_date = None
    country = None
    releases = mb_data.get('releases', [])
    if len(releases) > 0:
        release = releases[0]
        release_date = release.get('date')
        country = release.get('country')

    cur.execute("""
        INSERT OR REPLACE INTO musicbrainz_data
        (lastfm_id, release_date, country)
        VALUES (?, ?, ?)
    """, (lastfm_id, release_date, country))

def main():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
        SELECT l.id, t.track_name, a.artist_name
        FROM lastfm_tracks l
        JOIN tracks t ON l.track_id = t.id
        JOIN artists a ON l.artist_id = a.id
        WHERE l.id NOT IN (SELECT lastfm_id FROM musicbrainz_data)
        LIMIT 25
    """)
    tracks = cur.fetchall()
    print("Found {} tracks to fetch from MusicBrainz.".format(len(tracks)))

    for lastfm_id, track_name, artist_name in tracks:
        mb_data = search_musicbrainz(track_name, artist_name)
        insert_musicbrainz_data(cur, lastfm_id, mb_data)
        #print("Processed", track_name, "by", artist_name)

    conn.commit()
    print("Inserted {} tracks into the database.".format(len(tracks)))

    conn.close()
    print("run again for next batch")

if __name__ == "__main__":
    main()

