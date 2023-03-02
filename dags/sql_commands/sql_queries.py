class SqlQueries:
    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname , gender, level
        FROM staging_events
        WHERE page='NextSong'
                         """)