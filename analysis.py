import sqlite3
import pandas as pd

DB_NAME = "music.sqlite"

def load_joined_data():
    """
    Loads joined data from lastfm_tracks, deezer_data, and spotify_features.
    Ensures we only use rows that exist in ALL tables.
    """

    conn = sqlite3.connect(DB_NAME)


    query = """
        SELECT 
            l.id AS lastfm_id,
            l.track_name,
            l.artist,
            l.genre,
            d.rank,
            d.explicit_lyrics,
            s.danceability,
            s.energy,
            s.valence,
            s.tempo
        FROM lastfm_tracks l
        JOIN deezer_data d ON l.id = d.lastfm_id
        JOIN spotify_features s 
            ON l.track_name = s.track_name
            AND l.artist = s.artist;
    """

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def calculate_statistics(df):
    """
    Performs meaningful calculations required by the project.
    """

    stats = {}

    # 1. Average Spotify energy by genre
    stats['avg_energy_by_genre'] = df.groupby('genre')['energy'].mean()

    # 2. Average danceability by genre
    stats['avg_danceability_by_genre'] = df.groupby('genre')['danceability'].mean()

    # 3. Correlation between valence (happiness) and explicit lyrics
    stats['explicit_vs_valence'] = df.groupby('explicit_lyrics')['valence'].mean()

    # 4. Average Deezer rank by genre
    stats['avg_rank_by_genre'] = df.groupby('genre')['rank'].mean()

    return stats


def print_results(stats):
    """
    Prints calculations to the console — screenshot these for the report.
    """

    print("\n=====================")
    print("AVERAGE ENERGY BY GENRE")
    print("=====================")
    print(stats['avg_energy_by_genre'])

    print("\n==========================")
    print("AVERAGE DANCEABILITY BY GENRE")
    print("==========================")
    print(stats['avg_danceability_by_genre'])

    print("\n==============================================")
    print("VALENCE (HAPPINESS) BY EXPLICIT LYRICS FLAG")
    print("(0 = NOT EXPLICIT, 1 = EXPLICIT)")
    print("==============================================")
    print(stats['explicit_vs_valence'])

    print("\n==========================")
    print("AVERAGE DEEZER RANK BY GENRE")
    print("==========================")
    print(stats['avg_rank_by_genre'])


def main():
    print("Loading and joining data...")
    df = load_joined_data()

    print(f"Loaded {len(df)} joined rows.\n")

    print("Calculating statistics...")
    stats = calculate_statistics(df)

    print("\nDONE! Here are your results:")
    print_results(stats)


if __name__ == "__main__":
    main()
