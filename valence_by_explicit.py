import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

DB_NAME = 'music.sqlite'

def valence_explicit_lyric_calculation():
    """
    Performs JOIN function between lastfm_tracks, spotify_features, and deezer_data
    Analyzes relationship between track valence (happiness levels) and explicit lyrics 
    """
    # Connects to SQLite database 
    conn = sqlite3.connect(DB_NAME)

    # JOIN query 
    # Combines last.fm tracks with Spotify audio features and Deezer explicit lyric data
    # l, s, and d represent short names for each table to make column references cleaner 
    # l = lastfm_tracks (track name, artist, genre)
    # s = spotify_features (valence score)
    # d = deezer_data (explicit lyrics)
    query = '''
        SELECT
            s.valence,
            d.explicit_lyrics,
            l.genre,
            l.track_name,
            l.artist
        FROM lastfm_tracks l
        JOIN spotify_features s
            ON l.track_name = s.track_name AND l.artist = s.artist
        JOIN deezer_data d
            ON l.id = d.lastfm_id
    '''

    # Executes query and loads results into a pandas DataFrame
    df = pd.read_sql_query(query, conn)

    # Closes database connection
    conn.close()

    # Converts explicit_lyrics into integer format 
    # 0 means lyrics are clean, 1 means lyrics are explicit
    df['explicit_lyrics'] = df['explicit_lyrics'].astype(int)

    # Dictionary to store calculated results 
    stats = {}

    # Calculates average valence for explicit vs. non-explicit tracks 
    stats['explicit_vs_valence'] = df.groupby('explicit_lyrics')['valence'].mean()

    # Summary of results
    print('\n-=== Average Valence by Track Type ===')
    print(stats['explicit_vs_valence'])

    # Returns completed dataset and calculated statistics
    return df, stats 

def plot_valence_explicit(stats):
    # Accesses average valence values from statistics dictionary 
    averages = stats['explicit_vs_valence']

    # x-values represent non-explicit (0) and explicit (1) categories
    # y-values represent average valence for each category
    x = [0,1]
    y = [averages.get(0, 0), averages.get(1,0)]

    # Creates bar chart for visualization 
    plt.figure(figsize=(6,4))
   
    # Draws bars
    # Uses green for non-explicit tracks and purple for explicit tracks 
    plt.bar(x, y, color=['#4CAF50', '#800080'], edgecolor = 'black')
   
    # Labels each bar for clarity (x-axis labels)
    plt.xticks([0,1], ['Non-Explicit', 'Explicit'])
   
    # Adds y-axis label
    plt.ylabel('Average Valence')
    
    # Creates descriptive title for bar chart
    plt.title('Average Valence by Explicit Content')
    
    # Creates clean spacing around figure
    plt.tight_layout()
    
    # Saves figure as PNG file for documentation
    plt.savefig('average_valence_by_explicit.png', dpi=300, bbox_inches='tight')
    
    # Displays final figure
    plt.show()

# Runs analysis and creates visualization
df, stats = valence_explicit_lyric_calculation()
stats['explicit_vs_valence'].to_csv("TEXTaverage_valence_by_explicit.txt", sep='\t', header=True)
plot_valence_explicit(stats)