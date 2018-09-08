
from sqlalchemy import *

metadata = MetaData()

user = Table('user', metadata,
    Column('user_id', Integer, primary_key=True),
    Column('user_name', String(16), nullable=False),
    Column('email_address', String(60)),
    Column('password', String(20), nullable=False)
)



'''
CREATE TABLE station (
    id              INTEGER     PRIMARY KEY,
    name            TEXT        NOT NULL,
    timezone        TEXT        NOT NULL,  -- POSIX format (e.g. PST8PDT)
    location        TEXT        NULL,      -- e.g. "<city>, <state>"
    frequency       TEXT        NULL       -- informational only
);

CREATE TABLE show (
    id              INTEGER     PRIMARY KEY,
    name            TEXT        NOT NULL,
    host_name       TEXT        NULL,
    is_syndicated   BOOLEAN     NULL,
    station_id      INTEGER     NULL REFERENCES station,

    -- canonicality
    is_canonical    BOOLEAN     NULL,                  -- true if syndication master
    cnl_show_id     INTEGER     NULL REFERENCES show,  -- points to self, if canonical
    show_uri        TEXT        NULL
);

CREATE TABLE album (
    id              INTEGER     PRIMARY KEY,
    name            TEXT        NOT NULL,
    label           TEXT        NULL,
    catalog_no      TEXT        NULL,
    release_date    DATE        NULL,
    archiv_uri      TEXT        NULL
);

CREATE TABLE person (
    id              INTEGER     PRIMARY KEY,
    name            TEXT        NOT NULL,   -- TBD: normalized or raw?

    -- parsed (pieces of full_name)
    prefix          TEXT        NULL,
    first_name      TEXT        NULL,
    middle_name     TEXT        NULL,
    last_name       TEXT        NULL,
    suffix          TEXT        NULL,

    -- canonicality
    is_canonical    BOOLEAN     NULL,
    cnl_person_id   INTEGER     NULL REFERENCES person,  -- points to self, if canonical
    archiv_uri      TEXT        NULL
);


CREATE TABLE performer (
    id              INTEGER     PRIMARY KEY,
    role            TEXT        NOT NULL,   -- instrument, voice, role, etc.
    person_id       INTEGER     NOT NULL REFERENCES person,
    cnl_person_id   INTEGER     NULL     REFERENCES person,

    -- constraints
    UNIQUE (role, person_id)
);

CREATE TABLE ensemble (
    id              INTEGER     PRIMARY KEY,
    name            TEXT        NOT NULL,

    -- canonicality
    is_canonical    BOOLEAN     NULL,
    cnl_ensemble_id INTEGER     NULL REFERENCES ensemble,  -- points to self, if canonical
    archiv_uri      TEXT        NULL
);

CREATE TABLE piece (
    id              INTEGER     PRIMARY KEY,
    name            TEXT        NOT NULL,

    -- parsed (and normalized!!!)
    piece_type      TEXT        NULL,
    piece_key       TEXT        NULL,
    catalog_no      TEXT        NULL,  -- i.e. op., K., BWV, etc.

    -- canonicality
    is_canonical    BOOLEAN     NULL,
    cnl_piece_id    INTEGER     NULL REFERENCES piece,  -- points to self, if canonical
    archiv_uri      TEXT        NULL
);

CREATE TABLE play (
    id              INTEGER      PRIMARY KEY,
    station         TEXT         NOT NULL,
    play_info       JSONB        NOT NULL,  -- nornalized information
    play_date       DATE         NOT NULL,  -- listed local date
    play_start      TIME         NOT NULL,  -- listed local time
    play_end        TIME         NULL,      -- if listed
    play_dur        INTERVAL     NULL,      -- if listed

    -- foreign keys
    station_id      INTEGER      NULL REFERENCES station,
    show_id         INTEGER      NULL REFERENCES show,
    composer_id     INTEGER      NULL REFERENCES person,
    piece_id        INTEGER      NULL REFERENCES piece,
    -- note: typically, there will be either artist(s) OR ensemble(s)
    -- (though there can also be both); conductors and soloists are
    -- associated with ensembles
    artist_ids      INTEGER[]    NULL,      -- REFERENCES performer
    ensemble_ids    INTEGER[]    NULL,      -- REFERENCES ensemble
    conductor_id    INTEGER      NULL REFERENCES person,
    soloist_ids     INTEGER[]    NULL,      -- REFERENCES performer
    album_id        INTEGER      NULL REFERENCES album,

    -- miscellaneous
    notes           TEXT[]       NULL,

    -- technical
    start_time      TIMESTAMPTZ  NULL,
    end_time        TIMESTAMPTZ  NULL,
    duration        INTERVAL     NULL
);
'''
