import sqlite3
import pandas as pd
import matplotlib.pyplot as plt



# avergae deezer rank by genre (uses lastfm and deezer)
def plot_avg_deezer_rank():
    conn = sqlite3.connect('music.sqlite')

    sql = """
    SELECT l.genre, d.rank
    FROM lastfm_tracks l
    JOIN deezer_data d
      ON l.id = d.lastfm_id
    WHERE l.genre IS NOT NULL
      AND d.rank IS NOT NULL
    """
    df_rank = pd.read_sql_query(sql, conn)
    conn.close()

    if df_rank.empty:
        print("error - no data")
        return

    avg_rank = df_rank.groupby('genre')['rank'].mean().sort_values().reset_index()

    
    plt.figure(figsize=(10,6))

    # custom pink color :)
    bars = plt.bar(avg_rank['genre'], avg_rank['rank'], color=(0.87,0.05,0.50), edgecolor='blue')

    plt.title("Average Deezer Rank by Genre", fontsize=16)
    plt.xlabel("Genre", fontsize=12)
    plt.ylabel("Average Rank", fontsize=12)
    plt.xticks(rotation=45, ha='right')
    #plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()


def plot_tracks_per_genre_per_country():
    conn = sqlite3.connect('music.sqlite')

    sql = """
    SELECT l.genre, m.country
    FROM lastfm_tracks l
    JOIN musicbrainz_data m
      ON l.id = m.lastfm_id
    WHERE l.genre IS NOT NULL
      AND m.country IS NOT NULL
    """
    df_country = pd.read_sql_query(sql, conn)
    conn.close()

    if df_country.empty:
        print("error - no data")
        return

    # this is the data to plot
    country_data = df_country.pivot_table(index='country', columns='genre', aggfunc='size', fill_value=0)

    countries = country_data.index.tolist()
    genres = country_data.columns.tolist()

    plt.figure(figsize=(12,6))

    # made bars thicker
    bar_width = 0.25

    # plot each genre
    for i, genre in enumerate(genres):
        plt.bar(countries, country_data[genre], width=bar_width, label=genre, alpha=0.8)

    plt.xlabel("Country")
    plt.ylabel("Number of Tracks")
    plt.title("Tracks per Genre per Country")
    plt.xticks(rotation=45)
    plt.legend(title="Genre")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    plot_avg_deezer_rank()
    plot_tracks_per_genre_per_country()


