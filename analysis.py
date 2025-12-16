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
        t.track_name,
        g.genre_name AS genre,
        d.rank,
        d.explicit_lyrics,
        s.danceability,
        s.energy,
        s.valence,
        s.tempo,
        m.release_date,
        m.country
    FROM lastfm_tracks l
    JOIN tracks t
        ON l.track_id = t.id
    JOIN genres g
        ON l.genre_id = g.id
    JOIN deezer_data d
        ON l.id = d.lastfm_id
    JOIN spotify_features s
        ON l.id = s.lastfm_track_id
    LEFT JOIN musicbrainz_data m
        ON l.id = m.lastfm_id;
    """

    

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def calculate_statistics(df):
    """
    Performs meaningful calculations required by the project,
    including optional MusicBrainz country data.
    """

    stats = {}

    # 1. Average Spotify energy by genre
    stats['avg_energy_by_genre'] = df.groupby('genre')['energy'].mean()

    # 2. Average danceability by genre
    stats['avg_danceability_by_genre'] = df.groupby('genre')['danceability'].mean()

    # 3. Average tempo by genre
    stats['avg_tempo_by_genre'] = df.groupby('genre')['tempo'].mean()

    # 4. Average valence by genre
    stats['avg_valence_by_genre'] = df.groupby('genre')['valence'].mean() 

    # 5. Correlation between valence (happiness) and explicit lyrics
    stats['explicit_vs_valence'] = df.groupby('explicit_lyrics')['valence'].mean()

    # 6. Average Deezer rank by genre
    stats['avg_rank_by_genre'] = df.groupby('genre')['rank'].mean()

    # 7. Extract year from release_date and calculate yearly tempo
    if 'release_date' in df.columns:
        df_with_year = df.copy()
        df_with_year['year'] = pd.to_datetime(df_with_year['release_date'], errors='coerce').dt.year
        df_with_year = df_with_year.dropna(subset=['year'])
        stats['yearly_tempo'] = df_with_year.groupby('year')['tempo'].mean()
        stats['yearly_valence'] = df_with_year.groupby('year')['valence'].mean()
    else:
        stats['yearly_tempo'] = "No release_date data available"
        stats['yearly_valence'] = "No release_date data available"

    # 8. Genre vs. MusicBrainz release country (only if column exists)
    if 'country' in df.columns:
        stats['genre_vs_country'] = pd.crosstab(df['genre'], df['country'])
    else:
        stats['genre_vs_country'] = "No MusicBrainz country data available (join not loaded)"

    return stats

import sqlite3
import pandas as pd

def save_results_to_files(stats):
    """
    Saves each calculation to a separate text file.
    """
    with open("TEXTavg_energy_by_genre.txt", "w") as f:
        f.write("genre\tenergy\n")
        for genre, value in stats['avg_energy_by_genre'].items():
            f.write(f"{genre}\t{value:.4f}\n")

    with open("TEXTavg_danceability_by_genre.txt", "w") as f:
        f.write("genre\tdanceability\n")
        for genre, value in stats['avg_danceability_by_genre'].items():
            f.write(f"{genre}\t{value:.4f}\n")

    with open("TEXTavg_tempo_by_genre.txt", "w") as f:
        f.write("genre\ttempo\n")
        for genre, value in stats['avg_tempo_by_genre'].items():
            f.write(f"{genre}\t{value:.4f}\n")

    with open("TEXTavg_valence_by_genre.txt", "w") as f:
        f.write("genre\tvalence\n")
        for genre, value in stats['avg_valence_by_genre'].items():
            f.write(f"{genre}\t{value:.4f}\n")

    with open("TEXTexplicit_valence_stats.txt", "w") as f:
        f.write("explicit_lyrics\tvalence\n")
        for explicit, value in stats['explicit_vs_valence'].items():
            f.write(f"{explicit}\t{value:.4f}\n")

    with open("TEXTavg_deezer_rank.txt", "w") as f:
        f.write("genre\trank\n")
        for genre, value in stats['avg_rank_by_genre'].items():
            f.write(f"{genre}\t{value:.4f}\n")

    if isinstance(stats['yearly_tempo'], pd.Series):
        with open("TEXTyearly_tempo.txt", "w") as f:
            f.write("year\ttempo\n")
            for year, value in stats['yearly_tempo'].items():
                f.write(f"{int(year)}\t{value:.4f}\n")

    if isinstance(stats['yearly_valence'], pd.Series):
        with open("TEXTyearly_valence.txt", "w") as f:
            f.write("year\tvalence\n")
            for year, value in stats['yearly_valence'].items():
                f.write(f"{int(year)}\t{value:.4f}\n")

    print("\nAll results saved to text files!")


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

    print("\nGENRE VS MUSICBRAINZ COUNTRY")
    print("============================")
    print(stats['genre_vs_country'])




def main():
    print("Loading and joining data...")
    df = load_joined_data()

    print(f"Loaded {len(df)} joined rows.\n")

    print("Calculating statistics...")
    stats = calculate_statistics(df)

    print("\nDONE! Here are your results:")
    print_results(stats)

    print("\nSaving results to files")
    save_results_to_files(stats)


if __name__ == "__main__":
    main()
