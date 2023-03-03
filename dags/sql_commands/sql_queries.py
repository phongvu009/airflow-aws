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
    
    time_table_insert = ("""
        SELECT distinct start_time,
                        extract(hour from start_time),
                        extract(day from start_time),
                        extract(week from start_time),
                        extract(month from start_time),
                        extract(year from start_time),
                        extract(dayofweek from start_time)
        FROM songplays
                         """)