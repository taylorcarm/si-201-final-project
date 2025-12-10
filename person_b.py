
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

DB_NAME = 'music.sqlite'

def valence_explicit_lyric_calculation():
    conn = sqlite3.connect(DB_NAME)

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

    df = pd.read_sql_query(query, conn)

    conn.close()

    df['explicit_lyrics'] = df['explicit_lyrics'].astype(int)

    stats = {}
    stats['explicit_vs_valence'] = df.groupby('explicit_lyrics')['valence'].mean()

    print('\n-=== Average Valence by Track Type ===')
    print(stats['explicit_vs_valence'])

    return df, stats 

def plot_valence_explicit(stats):
    averages = stats['explicit_vs_valence']

    x = [0,1]
    y = [averages.get(0, 0), averages.get(1,0)]

    plt.figure(figsize=(6,4))
    plt.bar(x, y, color=['#4CAF50', '#800080'], edgecolor = 'black')
    plt.xticks([0,1], ['Non-Explicit', 'Explicit'])
    plt.ylabel('Average Valence')
    plt.title('Average Valence by Explicit Content')

    plt.tight_layout()
    plt.savefig('average_valence_by_explicit.png', dpi=300, bbox_inches='tight')
    plt.show()

df, stats = valence_explicit_lyric_calculation()
plot_valence_explicit(stats)