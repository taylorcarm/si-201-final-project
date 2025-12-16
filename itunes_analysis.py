
import sqlite3

DB_NAME = "music.sqlite"

def main():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Calculate average price by genre
    query = """
    SELECT g.genre_name, AVG(t.track_price) AS avg_price
    FROM itunes_tracks t
    JOIN genres g ON t.genre_id = g.id
    WHERE t.track_price IS NOT NULL
    GROUP BY g.genre_name
    ORDER BY avg_price DESC;
    """

    results = cur.execute(query).fetchall()
    conn.close()

    # Write results to file
    with open("itunes_analysis.txt", "w") as f:
        f.write("Average iTunes Track Price by Genre\n")
        f.write("==\n\n")
        for genre, avg_price in results:
            f.write(f"{genre}: ${avg_price:.2f}\n")

    print("iTunes analysis complete. Results written to itunes_analysis.txt")

if __name__ == "__main__":
    main()
