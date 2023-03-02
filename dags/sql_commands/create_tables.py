# create staging tables 
CREATE_EVENTS_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS staging_events(
        artist      VARCHAR(256),
        auth        VARCHAR(256),
        firstname   VARCHAR(256),
        gender      VARCHAR(256),
        iteminsession INT4,
        lastname    VARCHAR(256),
        length      nume(18,0),
        level       VARCHAR(256),
        location    VARCHAR(256),
        method      VARCHAR(256),
        page        VARCHAR(256),
        registration NUMERIC(18,0),
        sessionid   INT4,
        song        VARCHAR(256),
        status      INT4,
        ts          INT8,
        useragent   VARCHAR(256),
        userid      INT4
    )
"""