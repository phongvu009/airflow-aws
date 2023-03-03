class SqlQueries:
    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname , gender, level
        FROM staging_events
        WHERE page='NextSong' AND userid IS NOT NULL
                         """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
                         """)