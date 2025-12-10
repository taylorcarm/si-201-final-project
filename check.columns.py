
import sqlite3

conn = sqlite3.connect("music.sqlite")
cur = conn.cursor()

cur.execute("PRAGMA table_info(spotify_features)")
print(cur.fetchall())

conn.close()
