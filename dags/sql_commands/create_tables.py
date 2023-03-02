# create staging tables 
CREATE_STAGING_EVENTS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS staging_events(
    artist          varchar(256),
	auth            varchar(256),
	firstname       varchar(256),
	gender          varchar(256),
	iteminsession   int4,
	lastname        varchar(256),
	length          numeric(18,0),
	"level"         varchar(256),
	location        varchar(256),
	"method"        varchar(256),
	page            varchar(256),
	registration    numeric(18,0),
	sessionid       int4,
	song            varchar(256),
	status          int4,
	ts              int8,
	useragent       varchar(256),
	userid          int4
    )
"""

CREATE_STAGING_SONGS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_song            int4,
        artist_id           varchar(256),
        artist_name         varchar(256),
        artist_latitude     numeric(18,0),
        artist_longitude    numeric(18,0),
        artist_location     varchar(256),
        song_id             varchar(256),
        title               varchar(256),
        duration            numeric(18,0),
        "year"              int4
    )
"""

CREATE_DIM_USERS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS dim_users(
        userid          int4 NOT NULL,
        first_name      varchar(256),
        last_name       varchar(256),
        gender          varchar(256),
        "level"         varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
    )
"""