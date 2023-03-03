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
    
    artist_table_insert = ("""
        SELECT  distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
                           """)