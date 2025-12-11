import sqlite3

conn = sqlite3.connect("music.sqlite")
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS audiodb_artists")
conn.commit()
conn.close()

print("Dropped old audiodb_artists table.")
