import sqlite3
import pandas as pd
import matplotlib.pyplot as plt


def load_full_dataset():
    conn = sqlite3.connect("music.sqlite")

    query = '''
        SELECT 
        l.id AS lastfm_id, 
        t.track_name, 
        a.artist_name AS artist, 
        g.genre_name AS genre, 
        l.duration, 
        s.danceability, 
        s.energy, 
        s.valence, 
        s.tempo, 
        m.country, 
        m.release_date
    FROM lastfm_tracks l
    JOIN tracks t ON l.track_id = t.id
    JOIN artists a ON l.artist_id = a.id
    JOIN genres g ON l.genre_id = g.id
    LEFT JOIN spotify_features s ON l.id = s.lastfm_track_id
    LEFT JOIN musicbrainz_data m ON l.id = m.lastfm_id
    '''

    df = pd.read_sql(query, conn)
    conn.close()
    return df



def calculate_statistics(df):
    stats = {}

    # 1. Average Spotify energy by genre
    stats['avg_energy_by_genre'] = df.groupby('genre')['energy'].mean()

    # 2. Average danceability by genre
    stats['avg_danceability_by_genre'] = df.groupby('genre')['danceability'].mean()

    # 3. Average valence (happiness) by genre
    stats['avg_valence_by_genre'] = df.groupby('genre')['valence'].mean()

    return stats


# SCATTERPLOT: ENERGY VS DANCEABILITY

def plot_energy_vs_danceability(df):
    plt.figure(figsize=(8, 6))

    for genre in df['genre'].unique():
        subset = df[df['genre'] == genre]
        plt.scatter(subset['energy'], subset['danceability'], label=genre, alpha=0.6)

    plt.title("Energy vs Danceability by Genre")
    plt.xlabel("Energy")
    plt.ylabel("Danceability")
    plt.legend()
    plt.tight_layout()
    plt.savefig("energy_vs_danceability_scatter.png")
    plt.show()



# HISTOGRAM: TEMPO DISTRIBUTION

def plot_tempo_histogram(df):
    plt.figure(figsize=(8, 6))
    plt.hist(df['tempo'].dropna(), bins=20, color='purple', edgecolor='black', alpha=0.7)

    plt.title("Distribution of Song Tempo")
    plt.xlabel("Tempo (BPM)")
    plt.ylabel("Number of Songs")
    plt.tight_layout()
    plt.savefig("tempo_histogram.png")
    plt.show()


# LINE GRAPH — VALENCE OVER TIME

def plot_valence_over_time(df):
    df['year'] = df['release_date'].astype(str).str[:4]
    df['year'] = pd.to_numeric(df['year'], errors='coerce')

    yearly = df.groupby('year')['valence'].mean().dropna()

    plt.figure(figsize=(8, 5))
    yearly.plot(kind='line', marker='o', color='orange')

    plt.title("Average Valence (Happiness) Over Time")
    plt.xlabel("Year")
    plt.ylabel("Valence")
    plt.tight_layout()
    plt.savefig("valence_over_time.png")
    plt.show()



# RUN EVERYTHING

df = load_full_dataset()
stats = calculate_statistics(df)

# 1️⃣ Save avg energy, danceability, valence
stats['avg_energy_by_genre'].to_csv("TEXTavg_energy_by_genre.txt", sep='\t', header=True)
stats['avg_danceability_by_genre'].to_csv("TEXTavg_danceability_by_genre.txt", sep='\t', header=True)
stats['avg_valence_by_genre'].to_csv("TEXTavg_valence_by_genre.txt", sep='\t', header=True)

# 2️⃣ Save avg tempo by genre
avg_tempo = df.groupby('genre')['tempo'].mean().sort_values()
avg_tempo.to_csv("TEXTavg_tempo_by_genre.txt", sep='\t', header=True)

# 3️⃣ Save avg valence over time
df['year'] = df['release_date'].astype(str).str[:4]
df['year'] = pd.to_numeric(df['year'], errors='coerce')
yearly_valence = df.groupby('year')['valence'].mean().dropna()
yearly_valence.to_csv("TEXTyearly_valence.txt", sep='\t', header=True)

# 4️⃣ Save avg tempo over time
yearly_tempo = df.groupby('year')['tempo'].mean().dropna()
yearly_tempo.to_csv("TEXTyearly_tempo.txt", sep='\t', header=True)

plot_energy_vs_danceability(df)
plot_tempo_histogram(df)
plot_valence_over_time(df)

def plot_avg_tempo_by_genre(df):
    avg_tempo = df.groupby('genre')['tempo'].mean().sort_values()

    plt.figure(figsize=(10, 6))
    avg_tempo.plot(kind='bar', color='teal', edgecolor='black')

    plt.title("Average Tempo by Genre")
    plt.xlabel("Genre")
    plt.ylabel("Average Tempo (BPM)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("avg_tempo_by_genre.png")
    plt.show()


plot_avg_tempo_by_genre(df)


def plot_tempo_over_time(df):
    # Extract year
    df['year'] = df['release_date'].astype(str).str[:4]
    df['year'] = pd.to_numeric(df['year'], errors='coerce')

    yearly_tempo = df.groupby('year')['tempo'].mean().dropna()

    plt.figure(figsize=(10, 5))
    yearly_tempo.plot(kind='line', marker='s', color='blue', linewidth=2)

    plt.title("Average Song Tempo Over Time")
    plt.xlabel("Year")
    plt.ylabel("Tempo (BPM)")
    plt.tight_layout()
    plt.savefig("tempo_over_time.png")
    plt.show()

plot_tempo_over_time(df)

